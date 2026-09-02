"""掛牌雷達資料抓取: 證交所「申請上市」+ 櫃買「申請上櫃」→ ipo-radar/data.json

來源:
  TWSE  https://www.twse.com.tw/rwd/zh/company/applylisting?response=json&yy=<民國年>
        (乾淨 JSON, 按年查; 抓近三年涵蓋在途案件)
  TPEX  https://www.tpex.org.tw/zh-tw/mainboard/applying/status/company.html
        (資料 server-render 在 HTML 表格裡, 一頁含全部年份 800+ 列)

⚠ TPEX 擋 Cloudflare Worker (aurora-live 踩過), 只能在 GHA/本機抓 — 這支永遠跑在 GHA。
Idempotent: 內容沒變就不改檔 (交給 workflow 的 git diff 判斷要不要 commit)。
"""
import json, re, sys, datetime, os

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass
import requests
import urllib3
urllib3.disable_warnings()

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ipo-radar/1.0"}
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")


def roc_today_year():
    return datetime.date.today().year - 1911


def fetch_twse():
    """申請上市案件。⚠ 這支 API 不理 yy 參數, 每次都回整段歷史 (~870 筆) —
    打一次就好, 並以 (代號,申請日) 去重。"""
    r = requests.get("https://www.twse.com.tw/rwd/zh/company/applylisting",
                     params={"response": "json"}, headers=UA, timeout=45)
    r.raise_for_status()
    d = r.json()
    if d.get("stat") != "ok":
        raise RuntimeError(f"TWSE stat={d.get('stat')}")
    rows, seen = [], set()
    # fields: 索引,公司代號,公司簡稱,申請日期,董事長,申請時股本,審議會日期,
    #         董事會通過日期,主管機關備查(核准)日期,買賣日期,承銷商,承銷價,備註
    for it in d.get("data", []):
        key = (str(it[1]).strip(), str(it[3]).strip())
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            "code": str(it[1]).strip(), "name": str(it[2]).strip(), "market": "twse",
            "apply": str(it[3]).strip(), "chair": str(it[4]).strip(),
            "committee": str(it[6]).strip(), "board": str(it[7]).strip(),
            "approve": str(it[8]).strip(), "listing": str(it[9]).strip(),
            "uw": str(it[10]).strip(), "price": str(it[11]).strip(),
            "note": str(it[12]).strip(),
        })
    if len(rows) < 30:
        raise RuntimeError(f"TWSE 只拿到 {len(rows)} 列, 疑似 API 改版")
    return rows


def fetch_tpex():
    """申請上櫃: POST /www/zh-tw/company/applicant, 年份參數是 `date` (民國年)。
    ⚠ 頁面表格是 JS 灌的, requests 抓 HTML 拿不到; 一定要打這支 API。
    ⚠ headers 抄 fetch_auction_monitor.py 的成功配方 (少了會被 Cloudflare 520)。"""
    s = requests.Session()
    s.verify = False
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-TW,zh;q=0.9",
        "Referer": "https://www.tpex.org.tw/zh-tw/mainboard/applying/status/company.html",
        "X-Requested-With": "XMLHttpRequest",
    })
    rows, seen = [], set()
    for yy in range(roc_today_year(), roc_today_year() - 3, -1):
        r = s.post("https://www.tpex.org.tw/www/zh-tw/company/applicant",
                   data={"date": str(yy), "response": "json"}, timeout=45)
        r.raise_for_status()
        for c in (r.json().get("tables") or [{}])[0].get("data", []):
            c = [str(x).strip() for x in c]
            # 索引,代號,名稱,申請日,董事長,股本,審議會日,董事會通過日,核准契約日,買賣日,主辦承銷商,承銷價,備註
            if len(c) < 12 or not re.match(r"^\d{4,5}$", c[1]):
                continue
            key = (c[1], c[3])
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "code": c[1], "name": re.sub(r"\s+", "", c[2]), "market": "tpex",
                "apply": c[3], "chair": c[4],
                "committee": c[6], "board": c[7], "approve": c[8], "listing": c[9],
                "uw": c[10], "price": c[11],
                "note": c[12] if len(c) > 12 else "",
            })
    if len(rows) < 30:       # 近三年正常 100+ 筆, 太少 = API 改版, 寧可失敗別寫壞檔
        raise RuntimeError(f"TPEX 只拿到 {len(rows)} 列, 疑似 API 改版")
    return rows


def status_of(r):
    if re.search(r"自撤|退件|撤件|退回", r["note"]):
        return "dead"
    if r["listing"]:
        return "listed"
    if r["approve"] or r["board"]:
        return "pass"
    return "wait"


def stage_of(r):
    if r["listing"]:
        return 4
    if r["approve"] or r["board"]:
        return 3
    if r["committee"]:
        return 2
    return 1


def main():
    twse, tpex = fetch_twse(), fetch_tpex()
    rows = twse + tpex
    for r in rows:
        r["status"] = status_of(r)
        r["stage"] = stage_of(r)
    def roc_key(s):
        m = re.match(r"(\d{2,3})/(\d{2})/(\d{2})", s or "")
        return (int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else (0, 0, 0)
    # 只留「在途案件 (不管多老)」+「近兩年申請的」— 十年前的已掛牌歷史對雷達沒意義, 徒增載入
    cut = (roc_today_year() - 2, 1, 1)
    rows = [r for r in rows if r["status"] in ("wait", "pass") or roc_key(r["apply"]) >= cut]
    # 申請日新→舊
    rows.sort(key=lambda r: roc_key(r["apply"]), reverse=True)
    twse, tpex = [r for r in rows if r["market"] == "twse"], [r for r in rows if r["market"] == "tpex"]

    payload = {"updated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
               "twse": len(twse), "tpex": len(tpex), "rows": rows}
    new = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    old = ""
    if os.path.exists(OUT):
        with open(OUT, encoding="utf-8") as f:
            old = f.read()
    # updated 時戳以外的內容相同就不重寫 (idempotent)
    strip = lambda s: re.sub(r'"updated":"[^"]*"', "", s)
    if strip(new) == strip(old):
        print(f"無變化: TWSE {len(twse)} + TPEX {len(tpex)}")
        return
    with open(OUT, "w", encoding="utf-8") as f:
        f.write(new)
    print(f"已更新 data.json: TWSE {len(twse)} + TPEX {len(tpex)} = {len(rows)} 筆")


if __name__ == "__main__":
    main()
