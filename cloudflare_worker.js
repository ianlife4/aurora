// =========================================================
// Aurora Live Stock Proxy Worker
// 放在 Cloudflare Worker 當 CORS proxy，穩定毫秒級打 TPEX/TWSE
//
// Endpoints:
//   GET /emerging           → 全部 350+ 檔興櫃資料（cache 30 秒）
//   GET /stock?code=7768    → 單一股（興櫃優先 → 上櫃 → 上市）
// =========================================================

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Max-Age": "86400",
};

const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36";

// 簡易記憶體快取（每個 Worker instance 獨立，30 秒 TTL）
let EMERGING_CACHE = { data: null, ts: 0 };
const CACHE_TTL = 30_000;

async function getEmerging() {
  const now = Date.now();
  if (EMERGING_CACHE.data && (now - EMERGING_CACHE.ts) < CACHE_TTL) {
    return { data: EMERGING_CACHE.data, cached: true };
  }
  const url = `https://www.tpex.org.tw/www/zh-tw/emerging/latest?type=EMG&response=json&_=${now}`;
  const r = await fetch(url, {
    headers: {
      "User-Agent": UA,
      "Referer": "https://www.tpex.org.tw/",
      "Accept": "application/json, text/plain, */*",
      "Accept-Language": "zh-TW,zh;q=0.9",
    },
    cf: { cacheTtl: 30, cacheEverything: true },
  });
  if (!r.ok) throw new Error(`TPEX ${r.status}`);
  const d = await r.json();
  const table = d.tables?.[0];
  if (!table?.data) throw new Error("TPEX no data");

  // 轉為 code-keyed 簡化格式
  const map = {};
  for (const row of table.data) {
    const code = String(row[0]).trim();
    map[code] = {
      code,
      name: row[1],
      prev_avg: parseFloat(String(row[2]).replace(/,/g, "")) || 0,
      bid_price: parseFloat(String(row[3]).replace(/,/g, "")) || 0,
      bid_vol: row[4],
      ask_price: parseFloat(String(row[5]).replace(/,/g, "")) || 0,
      ask_vol: row[6],
      high: parseFloat(String(row[7]).replace(/,/g, "")) || 0,
      low: parseFloat(String(row[8]).replace(/,/g, "")) || 0,
      avg: parseFloat(String(row[9]).replace(/,/g, "")) || 0,
      price: parseFloat(String(row[10]).replace(/,/g, "")) || 0,
      volume: row[13],
    };
  }
  EMERGING_CACHE = { data: map, ts: now };
  return { data: map, cached: false };
}

async function getListed(code) {
  // 嘗試上櫃 + 上市
  for (const prefix of ["otc_", "tse_"]) {
    const url = `https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=${prefix}${code}.tw&json=1&delay=0&_=${Date.now()}`;
    try {
      const r = await fetch(url, {
        headers: {
          "User-Agent": UA,
          "Referer": "https://mis.twse.com.tw/",
          "Accept": "application/json",
        },
      });
      if (!r.ok) continue;
      const d = await r.json();
      const m = d.msgArray?.[0];
      if (!m || !m.z) continue;
      const price = parseFloat(m.z);
      if (!price || isNaN(price)) continue;
      return {
        code,
        name: m.n,
        price,
        yesterday: parseFloat(m.y) || 0,
        open: parseFloat(m.o) || 0,
        high: parseFloat(m.h) || 0,
        low: parseFloat(m.l) || 0,
        volume: m.v,
        time: m.t,
        market: prefix === "otc_" ? "上櫃" : "上市",
      };
    } catch (_) { /* try next */ }
  }
  return null;
}

async function handleStock(code) {
  // 1) 興櫃優先
  try {
    const { data } = await getEmerging();
    if (data[code]) {
      return { market: "興櫃", ...data[code] };
    }
  } catch (_) { /* fallthrough */ }
  // 2) 上市/上櫃
  return await getListed(code);
}

function json(body, status = 200, extra = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8", ...CORS_HEADERS, ...extra },
  });
}

export default {
  async fetch(request) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    try {
      if (url.pathname === "/emerging" || url.pathname === "/emerging/") {
        const { data, cached } = await getEmerging();
        return json({ ok: true, cached, count: Object.keys(data).length, data });
      }

      if (url.pathname === "/stock" || url.pathname === "/stock/") {
        const code = (url.searchParams.get("code") || "").trim();
        if (!/^\d{4,6}$/.test(code)) {
          return json({ ok: false, error: "invalid code" }, 400);
        }
        const result = await handleStock(code);
        if (!result) return json({ ok: false, error: "not found" }, 404);
        return json({ ok: true, data: result });
      }

      // 根目錄顯示簡易說明
      return json({
        ok: true,
        message: "Aurora Live Stock Proxy",
        endpoints: {
          "/emerging": "全部興櫃即時（30s cache）",
          "/stock?code=XXXX": "單股即時（興櫃→上櫃→上市）",
        },
      });
    } catch (err) {
      return json({ ok: false, error: String(err?.message || err) }, 500);
    }
  },
};
