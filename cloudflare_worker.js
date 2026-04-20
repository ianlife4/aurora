// =========================================================
// Aurora Live Stock Proxy Worker
// Deployed at: https://aurora-live.ian-4k.workers.dev
//
// Endpoints:
//   GET /emerging         → 全部興櫃資料（30s cache）
//   GET /stock?code=XXXX  → 單股（興櫃→上櫃→上市）
//
// Note: TPEX 擋 Cloudflare IP 直連，所以透過公共 CORS proxy 跳板
// =========================================================

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36";
let CACHE = { data: null, ts: 0 };
const TTL = 30000;

// Fetch TPEX via public CORS proxies (TPEX blocks CF IPs directly).
// 多 proxy 競速 (Promise.any)
async function fetchTPEX() {
  const target = `https://www.tpex.org.tw/www/zh-tw/emerging/latest?type=EMG&response=json&_=${Date.now()}`;
  const proxies = [
    u => "https://corsproxy.io/?" + encodeURIComponent(u),
    u => "https://api.allorigins.win/raw?url=" + encodeURIComponent(u),
    u => "https://api.codetabs.com/v1/proxy?quest=" + encodeURIComponent(u),
  ];
  const attempts = proxies.map(async p => {
    const r = await fetch(p(target), { headers: { "User-Agent": UA, "Accept": "application/json" } });
    if (!r.ok) throw new Error("HTTP " + r.status);
    return JSON.parse(await r.text());
  });
  return await Promise.any(attempts);
}

async function getEmerging() {
  if (CACHE.data && (Date.now() - CACHE.ts) < TTL) return { data: CACHE.data, cached: true };
  const d = await fetchTPEX();
  if (!d.tables?.[0]?.data) throw new Error("no TPEX data");
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
  return { data: map, cached: false };
}

async function getListed(code) {
  for (const prefix of ["otc_", "tse_"]) {
    try {
      const url = `https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=${prefix}${code}.tw&json=1&delay=0&_=${Date.now()}`;
      const r = await fetch(url, { headers: { "User-Agent": UA, "Referer": "https://mis.twse.com.tw/", "Accept": "application/json" } });
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
      };
    } catch (_) {}
  }
  return null;
}

async function handleStock(code) {
  try {
    const { data } = await getEmerging();
    if (data[code]) return { market: "興櫃", ...data[code] };
  } catch (_) {}
  return await getListed(code);
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
      return json({
        ok: true,
        message: "Aurora Live Stock Proxy",
        endpoints: {
          "/emerging": "all emerging (30s cache)",
          "/stock?code=XXXX": "single stock",
        },
      });
    } catch (err) {
      return json({ ok: false, error: String(err?.message || err) }, 500);
    }
  },
};
