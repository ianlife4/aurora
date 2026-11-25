// Aurora Cron Worker（也兼任「法說會行事曆」的保活／備份／告警）
//
// 2026-11-25 改：CF 帳號 cron 上限 5 條（本 worker 3 條 + eps-cron-trigger 2 條 = 滿），
// 法說會後端保活要每 10 分鐘一次，所以把本 worker 的 3 條併成一條 "*/10 * * * *"，
// 在 JS 裡用 scheduledTime（UTC）分派。騰出 2 條 cron 位子。
//
// ⚠ 順便修了既有 bug：CF cron 的 day-of-week 是 1=週日，舊設定 "1-5" 實際 = 週日~週四，
//   GHA 紀錄實證週五 09:30/13:00/15:30 沒跑、週日反而有跑。這裡改用 getUTCDay()（0=週日），
//   1-5 才是真正的週一~五。（UTC 01:30~10:00 與台北同一天，星期不會錯位。）
//
// 分派表（UTC → 台北 +8）：
//   aurora dispatch : 週一~五 01:30 / 05:00 / 07:30（台北 09:30 / 13:00 / 15:30）、每天 10:00（台北 18:00）
//   法說會保活      : 每 10 分鐘 GET /manifest.webmanifest（642B、免登入；不打 /api/health，那支會拿寫鎖）
//   法說會健檢      : 每小時 :00 GET /api/health → Gemini 掛 / DB 異常 / 打不到 才推 TG（狀態改變才推）
//   法說會備份      : 每天 06:00 / 18:00（台北 14:00 / 02:00）GET /api/events → KV EARNINGS_BACKUP
//
// Secrets（wrangler secret put，絕不寫進程式或指令列）：
//   GITHUB_TOKEN, TRIGGER_KEY（原有）
//   EARNINGS_SITE_KEY（法說會 X-Site-Key）、TG_BOT_TOKEN、TG_CHAT_ID（告警私訊）
//   EC_ADMIN_KEY（/backup* /ec/* 手動端點用；本機存在法說會 repo 的 .ec_admin_key，已 exclude）
// ⚠ 這個 repo（ianlife4/aurora）是公開的：程式碼裡不能有任何金鑰；
//   備份含私人行程，/backup* 端點一律要 EC_ADMIN_KEY。

const OWNER = 'ianlife4';
const REPO = 'aurora';
const WORKFLOW = 'update-auction-data.yml';

const EC_BASE = 'https://earnings-calendar-rvfa.onrender.com';
// Render 前面有 CF，非瀏覽器 UA 會吃 error 1010
const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';
const BACKUP_TTL = 35 * 24 * 3600;   // 日期版保留 35 天（KV 自動過期）
const SHRINK_RATIO = 0.8;            // 比上一份小超過 20% 就當成可疑
const PING_FAIL_ALERT = 2;           // 連續幾次保活失敗才推 TG（避開單次網路抖動）

// ---------- 分派（純函式，方便本機測） ----------
export function plan(date) {
  const h = date.getUTCHours();
  const m = Math.floor(date.getUTCMinutes() / 10) * 10;   // 容忍觸發時間的小誤差
  const dow = date.getUTCDay();                             // 0=週日
  const weekday = dow >= 1 && dow <= 5;
  const at = (hh, mm) => h === hh && m === mm;
  return {
    aurora: (weekday && (at(1, 30) || at(5, 0) || at(7, 30))) || at(10, 0),
    keepAlive: true,
    health: m === 0,
    backup: at(6, 0) || at(18, 0),
  };
}

// ---------- aurora（原有行為） ----------
async function dispatchWorkflow(env) {
  const url = `https://api.github.com/repos/${OWNER}/${REPO}/actions/workflows/${WORKFLOW}/dispatches`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'Accept': 'application/vnd.github+json',
      'Authorization': `Bearer ${env.GITHUB_TOKEN}`,
      'X-GitHub-Api-Version': '2022-11-28',
      'User-Agent': 'aurora-cron-worker',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ ref: 'main' }),
  });
  const ok = resp.ok;
  const status = resp.status;
  const body = ok ? '' : await resp.text();
  const log = `[${new Date().toISOString()}] dispatch ${WORKFLOW} → ${status} ${ok ? 'OK' : body}`;
  console.log(log);
  return { ok, status, log };
}

// ---------- 共用 ----------
async function tg(env, text) {
  if (!env.TG_BOT_TOKEN || !env.TG_CHAT_ID) { console.log('TG not configured: ' + text); return false; }
  try {
    const r = await fetch(`https://api.telegram.org/bot${env.TG_BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: env.TG_CHAT_ID, text: text.slice(0, 3900), disable_web_page_preview: true }),
    });
    if (!r.ok) console.log('TG send failed ' + r.status);
    return r.ok;
  } catch (e) { console.log('TG send error ' + e); return false; }
}

function ecFetch(env, path, { timeout = 90000, siteKey = false } = {}) {
  const headers = { 'User-Agent': BROWSER_UA, 'Accept': 'application/json, */*' };
  if (siteKey) headers['X-Site-Key'] = env.EARNINGS_SITE_KEY || '';
  return fetch(EC_BASE + path, { headers, signal: AbortSignal.timeout(timeout) });
}

// 告警狀態：一次執行只讀一次、最後只寫一次 state/alerts（KV 同一 key 每秒只能寫 1 次，
// 連寫兩次會 429 → 例外 → TG 永遠送不出去；審查抓到的），訊息在寫完後才送。
class Alerts {
  constructor(env) { this.env = env; this.state = null; this.dirty = false; this.msgs = []; }
  async load() { this.state = (await this.env.EARNINGS_BACKUP.get('state/alerts', 'json')) || {}; return this; }
  set(key, val) { if (this.state[key] !== val) { this.state[key] = val; this.dirty = true; } }
  // 狀態「變化」才推（壞掉推一次、恢復推一次）
  flag(key, bad, badMsg, okMsg) {
    const was = !!this.state[key];
    if (was === !!bad) return;
    this.set(key, bad ? new Date().toISOString() : null);
    const m = bad ? badMsg : okMsg;
    if (m) this.msgs.push(m);
  }
  // 連續失敗計數：達 threshold 才算壞
  streak(countKey, flagKey, ok, threshold, badMsgFn, okMsg) {
    const n = ok ? 0 : (this.state[countKey] || 0) + 1;
    this.set(countKey, n);
    this.flag(flagKey, n >= threshold, badMsgFn(n), okMsg);
  }
  async flush() {
    if (this.dirty) {
      try { await this.env.EARNINGS_BACKUP.put('state/alerts', JSON.stringify(this.state)); }
      catch (e) { console.log('state put failed ' + e); }
    }
    for (const m of this.msgs) await tg(this.env, m);
  }
}

// ---------- 法說會：保活 ----------
async function keepAlive(env, al) {
  let ok = false, detail = '';
  try {
    const r = await ecFetch(env, '/manifest.webmanifest', { timeout: 90000 });
    await r.arrayBuffer();
    ok = r.ok;
    detail = `HTTP ${r.status}`;
  } catch (e) { detail = String(e).slice(0, 120); }
  console.log(`[keepalive] ${ok ? 'ok' : 'FAIL'} ${detail}`);
  al.streak('ping_fails', 'render_down', ok, PING_FAIL_ALERT,
    (n) => `⚠ 法說會行事曆 Render 連續 ${n} 次打不到（${detail}）\n可能：Render 停機/免費額度或頻寬用完/部署失敗。LINE 進料會收不到。\n${EC_BASE}/api/health`,
    `✅ 法說會行事曆 Render 恢復回應`);
  return { ok, detail };
}

// ---------- 法說會：健檢（Gemini / DB / health 本身） ----------
async function healthCheck(env, al) {
  let h = null, detail = '';
  try {
    const r = await ecFetch(env, '/api/health', { timeout: 120000 });
    detail = `HTTP ${r.status}`;
    if (r.ok) h = await r.json();
  } catch (e) { detail = String(e).slice(0, 120); }
  // manifest 通但 /api/health 5xx/逾時（DB 鎖死、程式錯）也要告警；每小時一次，連 2 次才推
  al.streak('health_fails', 'health_bad', !!h, 2,
    (n) => `⚠ 法說會行事曆 /api/health 連續 ${n} 小時異常（${detail}）\n靜態檔還打得到，可能是 DB 或程式卡住。`,
    `✅ 法說會行事曆 /api/health 恢復`);
  if (!h) { console.log('[health] FAIL ' + detail); return { ok: false, detail }; }
  const llm = h.llm || {};
  const llmBad = !!llm.down;
  al.flag('llm_down', llmBad,
    `⚠ 法說會行事曆 Gemini 掛了（code ${llm.code ?? '?'}，since ${llm.since ?? '?'}）\n402=預付額度用完、403=金鑰、429=限流。OCR 會失敗，照片要等修好後用 tools/reprocess_backlog.py 補跑。`,
    `✅ 法說會行事曆 Gemini 恢復正常`);
  const dbBad = !String(h.db || '').startsWith('ok');
  al.flag('db_bad', dbBad,
    `⚠ 法說會行事曆 DB 異常：${String(h.db).slice(0, 200)}`,
    `✅ 法說會行事曆 DB 恢復：${String(h.db).slice(0, 100)}`);
  console.log(`[health] ok llm_down=${llmBad} db=${h.db}`);
  return { ok: true, llm, db: h.db, build: h.build };
}

// ---------- 法說會：備份 ----------
function stamp(d) {
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}_${p(d.getUTCHours())}${p(d.getUTCMinutes())}`;
}

// force=true（只有手動 /backup?force=1）：跳過縮水檢查。真的大量刪行程後要用它重設基準，
// 不然之後每一份都會被判 shrunk、latest 永遠停在舊版。
async function backup(env, al, when = new Date(), force = false) {
  const prev = (await env.EARNINGS_BACKUP.get('meta/last', 'json')) || null;
  let err = '', buf = null, status = 0;
  for (let attempt = 0; attempt < 2 && !buf; attempt++) {
    try {
      const r = await ecFetch(env, '/api/events', { timeout: 120000, siteKey: true });
      status = r.status;
      if (!r.ok) { err = `HTTP ${r.status}`; await r.arrayBuffer(); continue; }
      buf = await r.arrayBuffer();
      err = '';
    } catch (e) { err = String(e).slice(0, 120); }
  }
  // 不 JSON.parse（3.5MB 解析會吃掉免費方案的 CPU 額度），只做便宜的結構檢查：
  // 開頭 '['、結尾 ']'、大小沒有比上一份縮水超過 20%。完整筆數驗證在本機還原測試做。
  if (buf) {
    const u8 = new Uint8Array(buf);
    let i = 0; while (i < u8.length && u8[i] <= 32) i++;
    let j = u8.length - 1; while (j > 0 && u8[j] <= 32) j--;
    if (u8[i] !== 0x5b || u8[j] !== 0x5d) err = 'not a JSON array';
    else if (u8.length < 1000) err = `too small (${u8.length} B)`;
    else if (!force && prev && prev.bytes && u8.length < prev.bytes * SHRINK_RATIO)
      err = `shrunk ${prev.bytes} → ${u8.length} B (>${Math.round((1 - SHRINK_RATIO) * 100)}%)`;
  } else if (!err) err = 'no data';

  if (err) {
    console.log('[backup] FAIL ' + err);
    // 縮水的那份也存起來（另一個 key，不蓋 latest），方便事後比對
    if (buf && err.startsWith('shrunk'))
      await env.EARNINGS_BACKUP.put(`suspect/${stamp(when)}.json`, buf, { expirationTtl: BACKUP_TTL });
    al.flag('backup_bad', true,
      `⚠ 法說會行事曆備份失敗：${err}\nlatest 沒有被覆蓋（上一份：${prev ? prev.ts + '，' + prev.bytes + ' B' : '無'}）。` +
      (err.startsWith('shrunk') ? '\n若確定是自己刪的，打 aurora-cron /backup?force=1 重設基準。' : ''), '');
    return { ok: false, err, status };
  }
  const key = `events/${stamp(when)}.json`;
  await env.EARNINGS_BACKUP.put(key, buf, { expirationTtl: BACKUP_TTL });
  await env.EARNINGS_BACKUP.put('events/latest.json', buf);
  const meta = { ts: new Date().toISOString(), key, bytes: buf.byteLength, forced: !!force };
  await env.EARNINGS_BACKUP.put('meta/last', JSON.stringify(meta));
  al.flag('backup_bad', false, '', `✅ 法說會行事曆備份恢復正常（${buf.byteLength} B）`);
  console.log(`[backup] ok ${key} ${buf.byteLength} B`);
  return { ok: true, ...meta };
}

// 包一層：載入告警狀態 → 跑 → 不管成敗都 flush 一次
async function withAlerts(env, fn) {
  const al = await new Alerts(env).load();
  try { return await fn(al); }
  finally { await al.flush(); }
}

// ---------- 入口 ----------
async function runScheduled(env, when) {
  const p = plan(when);
  const jobs = [];
  if (p.aurora) jobs.push(dispatchWorkflow(env).catch((e) => console.log('aurora error ' + e)));
  // 保活先跑（順便叫醒 Render），再健檢、備份
  jobs.push(withAlerts(env, async (al) => {
    await keepAlive(env, al).catch((e) => console.log('keepalive error ' + e));
    if (p.health) await healthCheck(env, al).catch((e) => console.log('health error ' + e));
    if (p.backup) await backup(env, al, when).catch((e) => console.log('backup error ' + e));
  }).catch((e) => console.log('earnings error ' + e)));
  await Promise.all(jobs);
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), {
    status, headers: { 'content-type': 'application/json; charset=utf-8' },
  });
}

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runScheduled(env, new Date(event.scheduledTime)));
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    const k = url.searchParams.get('key');
    // /trigger 用原本的 TRIGGER_KEY；法說會備份含私人行程，另用 EC_ADMIN_KEY（金鑰檔在法說會 repo 的 .ec_admin_key）
    if (url.pathname === '/trigger' && !(env.TRIGGER_KEY && k === env.TRIGGER_KEY)) return new Response('forbidden', { status: 403 });
    const ecPaths = ['/backup', '/backup/latest', '/backup/list', '/ec/status', '/ec/health', '/ec/test-alert'];
    if (ecPaths.includes(url.pathname) && !(env.EC_ADMIN_KEY && k === env.EC_ADMIN_KEY)) return new Response('forbidden', { status: 403 });

    if (url.pathname === '/trigger') {
      const result = await dispatchWorkflow(env);
      return json(result, result.ok ? 200 : 500);
    }
    if (url.pathname === '/backup') {                 // 手動備份一次
      const force = url.searchParams.get('force') === '1';
      const r = await withAlerts(env, (al) => backup(env, al, new Date(), force));
      return json(r, r.ok ? 200 : 500);
    }
    if (url.pathname === '/backup/latest') {          // 下載（可加 ?k=events/20261125_0600.json）
      const k = url.searchParams.get('k') || 'events/latest.json';
      if (!/^(events|suspect)\/[\w.]+$/.test(k)) return new Response('bad key', { status: 400 });
      const v = await env.EARNINGS_BACKUP.get(k, 'stream');
      if (!v) return new Response('not found', { status: 404 });
      return new Response(v, { headers: { 'content-type': 'application/json; charset=utf-8' } });
    }
    if (url.pathname === '/backup/list') {
      const l = await env.EARNINGS_BACKUP.list({ prefix: url.searchParams.get('prefix') || 'events/' });
      return json(l.keys.map((k) => ({ name: k.name, expiration: k.expiration })));
    }
    if (url.pathname === '/ec/status') {
      const al = await new Alerts(env).load();
      return json({ state: al.state, last_backup: await env.EARNINGS_BACKUP.get('meta/last', 'json') });
    }
    if (url.pathname === '/ec/health') return json(await withAlerts(env, (al) => healthCheck(env, al)));
    if (url.pathname === '/ec/test-alert') return json({ sent: await tg(env, '🔔 aurora-cron 告警測試（法說會行事曆保活/備份）') });

    return new Response(
      'aurora-cron worker\n' +
      'Schedules: aurora TW 09:30 / 13:00 / 15:30 Mon-Fri, 18:00 daily\n' +
      '           earnings-calendar keep-alive every 10 min, health hourly, backup TW 02:00 / 14:00\n' +
      'Manual: /trigger?key=<TRIGGER_KEY>; /backup[&force=1] /backup/latest[&k=] /backup/list /ec/status /ec/health /ec/test-alert ?key=<EC_ADMIN_KEY>\n',
      { headers: { 'content-type': 'text/plain; charset=utf-8' } }
    );
  },
};
