// =========================================================
// Aurora Live Stock Proxy Worker (v2 — 2026-06-11)
// Deployed at: https://aurora-live.ian-4k.workers.dev
//
// Endpoints:
//   GET /emerging         → 全部興櫃資料（30s cache）
//   GET /stock?code=XXXX  → 單股（興櫃→上櫃/上市→Yahoo）
//   GET /debug            → 各上游健康檢查（診斷用）
//
// 上游策略 (2026-06-11 v2):
//   1) TPEX www API 直連
//   2) 公共 CORS proxy 跳板競速 — 2026-06-11 實測 corsproxy.io(403)/
//      allorigins(500)/codetabs(400) 全滅，僅留作日後復活的備援
//   3) /stock 另有 Yahoo Finance v8 chart 備援（不擋 CF、僅單股）
//   TPEX 全滅時負快取 60s，/stock 直接跳過興櫃表不重複 hang
// =========================================================

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36";
let CACHE = { data: null, ts: 0 };
const TTL = 30000;
let TPEX_DOWN_TS = 0;            // 負快取：TPEX 全部來源失敗的時間
const TPEX_DOWN_TTL = 60000;

function tfetch(url, opts = {}, ms = 6000) {
  return fetch(url, { ...opts, signal: AbortSignal.timeout(ms) });
}

const TPEX_TARGET = () => `https://www.tpex.org.tw/www/zh-tw/emerging/latest?type=EMG&response=json&_=${Date.now()}`;
const PROXIES = [
  u => "https://corsproxy.io/?" + encodeURIComponent(u),
  u => "https://api.allorigins.win/raw?url=" + encodeURIComponent(u),
  u => "https://api.codetabs.com/v1/proxy?quest=" + encodeURIComponent(u),
];

function parseTpexJson(text) {
  const d = JSON.parse(text);
  if (!d.tables?.[0]?.data) throw new Error("no TPEX data");
  return d;
}

// 1) 直連 → 2) 公共 proxy 競速
// timeout 必須讓整條失敗鏈 < 前端的 10s（redirect:manual 讓被擋時立刻 302 失敗而非跟著轉圈）
async function fetchTPEX() {
  const target = TPEX_TARGET();
  try {
    const r = await tfetch(target, {
      redirect: "manual",
      headers: { "User-Agent": UA, "Accept": "application/json", "Referer": "https://www.tpex.org.tw/zh-tw/" },
    }, 3500);
    if (r.ok) return parseTpexJson(await r.text());
  } catch (_) { /* fall through to proxies */ }

  const attempts = PROXIES.map(async p => {
    const r = await tfetch(p(target), { headers: { "User-Agent": UA, "Accept": "application/json" } }, 4000);
    if (!r.ok) throw new Error("HTTP " + r.status);
    return parseTpexJson(await r.text());
  });
  return await Promise.any(attempts);
}

async function getEmerging() {
  const now = Date.now();
  if (CACHE.data && (now - CACHE.ts) < TTL) return { data: CACHE.data, cached: true };
  if (now - TPEX_DOWN_TS < TPEX_DOWN_TTL) throw new Error("TPEX sources down (cached failure)");
  let d;
  try {
    d = await fetchTPEX();
  } catch (e) {
    TPEX_DOWN_TS = now;
    throw e;
  }
  const map = {};
  for (const row of d.tables[0].data) {
    const c = String(row[0]).trim();
    map[c] = {
      code: c, name: row[1],
      prev_avg: +String(row[2]).replace(/,/g, "") || 0,
      bid_price: +String(row[3]).replace(/,/g, "") || 0,
      ask_price: +String(row[5]).replace(/,/g, "") || 0,
      high: +String(row[7]).replace(/,/g, "") || 0,
      low: +String(row[8]).replace(/,/g, "") || 0,
      avg: +String(row[9]).replace(/,/g, "") || 0,
      price: +String(row[10]).replace(/,/g, "") || 0,
      volume: row[13],
    };
  }
  CACHE = { data: map, ts: Date.now() };
  TPEX_DOWN_TS = 0;
  return { data: map, cached: false };
}

async function getListed(code) {
  for (const prefix of ["otc_", "tse_"]) {
    try {
      const url = `https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=${prefix}${code}.tw&json=1&delay=0&_=${Date.now()}`;
      const r = await tfetch(url, { headers: { "User-Agent": UA, "Referer": "https://mis.twse.com.tw/", "Accept": "application/json" } });
      if (!r.ok) continue;
      const d = await r.json();
      const m = d.msgArray?.[0];
      if (!m || !m.z) continue;
      const p = parseFloat(m.z);
      if (!p || isNaN(p)) continue;
      return {
        code, name: m.n, price: p,
        yesterday: +m.y || 0, open: +m.o || 0, high: +m.h || 0, low: +m.l || 0,
        volume: m.v, time: m.t,
        market: prefix === "otc_" ? "上櫃" : "上市",
        source: "twse-mis",
      };
    } catch (_) {}
  }
  return null;
}

// Yahoo Finance v8 chart — 不擋 CF IP，興櫃/上櫃皆 .TWO，上市 .TW
// 沒有買賣價/當日均價，但 price/高低/昨收/量都有
async function fetchYahoo(code) {
  for (const suffix of [".TWO", ".TW"]) {
    try {
      const url = `https://query1.finance.yahoo.com/v8/finance/chart/${code}${suffix}?interval=1d&range=1d`;
      const r = await tfetch(url, { headers: { "User-Agent": UA, "Accept": "application/json" } });
      if (!r.ok) continue;
      const d = await r.json();
      const meta = d.chart?.result?.[0]?.meta;
      const p = meta?.regularMarketPrice;
      if (!p || p <= 0) continue;
      return {
        code, name: meta.shortName || meta.longName || code,
        price: p,
        yesterday: meta.chartPreviousClose || meta.previousClose || 0,
        high: meta.regularMarketDayHigh || 0,
        low: meta.regularMarketDayLow || 0,
        volume: meta.regularMarketVolume || 0,
        time: meta.regularMarketTime ? new Date(meta.regularMarketTime * 1000).toLocaleTimeString("zh-TW", { timeZone: "Asia/Taipei", hour12: false }) : "",
        market: suffix === ".TW" ? "上市" : "興櫃",
        source: "yahoo",
      };
    } catch (_) {}
  }
  return null;
}

async function handleStock(code) {
  try {
    const { data } = await getEmerging();
    if (data[code]) return { market: "興櫃", source: "tpex", ...data[code] };
  } catch (_) {}
  const listed = await getListed(code);
  if (listed) return listed;
  return await fetchYahoo(code);
}

// 各上游健康檢查：直連 TPEX / 3 個 proxy / TWSE MIS / Yahoo
async function runDebug() {
  const target = TPEX_TARGET();
  const probe = async (label, url, headers, validate) => {
    const t0 = Date.now();
    try {
      const r = await tfetch(url, { headers }, 8000);
      const text = await r.text();
      let note = "";
      if (validate) { try { validate(text); note = "valid"; } catch (e) { note = "invalid: " + e.message; } }
      return { label, ok: r.ok && !note.startsWith("invalid"), status: r.status, ms: Date.now() - t0, note, snippet: text.slice(0, 120) };
    } catch (e) {
      return { label, ok: false, status: 0, ms: Date.now() - t0, note: String(e?.message || e) };
    }
  };
  const jsonHdr = { "User-Agent": UA, "Accept": "application/json" };
  const checks = await Promise.all([
    probe("tpex-direct", target, { ...jsonHdr, "Referer": "https://www.tpex.org.tw/zh-tw/" }, parseTpexJson),
    ...PROXIES.map((p, i) => probe("proxy-" + ["corsproxy", "allorigins", "codetabs"][i], p(target), jsonHdr, parseTpexJson)),
    probe("twse-mis", `https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=otc_5274.tw&json=1&delay=0&_=${Date.now()}`, { ...jsonHdr, "Referer": "https://mis.twse.com.tw/" }, t => { if (!JSON.parse(t).msgArray) throw new Error("no msgArray"); }),
    probe("yahoo", "https://query1.finance.yahoo.com/v8/finance/chart/6986.TWO?interval=1d&range=1d", jsonHdr, t => { if (!JSON.parse(t).chart?.result?.[0]?.meta?.regularMarketPrice) throw new Error("no price"); }),
  ]);
  return checks;
}

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8", ...CORS },
  });
}

export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
    try {
      if (url.pathname.startsWith("/emerging")) {
        const { data, cached } = await getEmerging();
        return json({ ok: true, cached, count: Object.keys(data).length, data });
      }
      if (url.pathname.startsWith("/stock")) {
        const code = (url.searchParams.get("code") || "").trim();
        if (!/^\d{4,6}$/.test(code)) return json({ ok: false, error: "invalid code" }, 400);
        const result = await handleStock(code);
        if (!result) return json({ ok: false, error: "not found" }, 404);
        return json({ ok: true, data: result });
      }
      if (url.pathname.startsWith("/debug")) {
        return json({ ok: true, version: "v2-2026-06-11", checks: await runDebug() });
      }
      return json({
        ok: true,
        message: "Aurora Live Stock Proxy",
        version: "v2-2026-06-11",
        endpoints: {
          "/emerging": "all emerging (30s cache)",
          "/stock?code=XXXX": "single stock",
          "/debug": "upstream health checks",
        },
      });
    } catch (err) {
      return json({ ok: false, error: String(err?.message || err) }, 500);
    }
  },
};
