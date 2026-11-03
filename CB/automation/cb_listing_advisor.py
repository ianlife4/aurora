# -*- coding: utf-8 -*-
"""CB 掛牌前夜預告 — 每晚檢查明日(下一交易日)掛牌的 CB,
預估開盤分層 + 追價規則,推 TG。

回測依據(2018~2026, n=848):
- 首日開盤追 → T+3 收盤出:全樣本 +2.3%/勝率61%;熱區(開盤110~125)+3.96%/66%
- 競拍檔:開盤 ≈ 得標均價 +1.5(相關0.92)→ 提前一週可知
- 詢圈檔:開盤 ≈ 理論價 +11(相關0.59)→ 前一晚可估
- 開盤 vs 理論價:折價開 +7.2%/75%;溢價<8 可追;溢價>15 肉薄
獨立腳本,只讀 cb_data.db + FinMind;TG 憑證用 eps-cron/.env。
"""
import os, sys, json, sqlite3, io
import urllib.request
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HERE, "cb_data.db")
ENV = r"C:\Users\J.Chun\Desktop\進行中專案\eps-cron\.env"
FINMIND_TOKEN_FILE = os.path.join(HERE, "finmind_token.txt")


def load_env(path):
    env = {}
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def tg_send(text):
    env = load_env(ENV)
    token = env.get("TG_BOT_TOKEN", "")
    chat = env.get("TG_CHAT_ID", "")
    if not token or not chat:
        print("[cb_listing] TG env missing; print only:\n" + text)
        return False
    body = json.dumps({"chat_id": chat, "text": text, "parse_mode": "HTML",
                       "disable_web_page_preview": True}).encode("utf-8")
    req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage",
                                 data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        urllib.request.urlopen(req, timeout=15)
        return True
    except Exception as e:
        print("[cb_listing] tg fail:", e)
        return False


def stock_close(code):
    """今晚個股收盤(FinMind)"""
    try:
        token = open(FINMIND_TOKEN_FILE).read().strip()
        start = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
        url = ("https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockPrice"
               f"&data_id={code}&start_date={start}&token={token}")
        with urllib.request.urlopen(url, timeout=30) as r:
            data = json.load(r).get("data") or []
        closes = [d["close"] for d in data if d.get("close")]
        return float(closes[-1]) if closes else None
    except Exception:
        return None



def finmind_conv(cb_code):
    """FinMind DailyOverview 的轉換價(訂價後最權威的免費源)"""
    try:
        token = open(FINMIND_TOKEN_FILE).read().strip()
        url = ("https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockConvertibleBondDailyOverview"
               f"&data_id={cb_code}&start_date=2024-01-01&token={token}")
        with urllib.request.urlopen(url, timeout=30) as r:
            data = json.load(r).get("data") or []
        for d in reversed(data):
            cp = float(d.get("ConversionPrice") or 0)
            if 0.01 < cp < 100000:
                return cp
    except Exception:
        pass
    return None

def next_trading_day():
    """明天之後的第一個【交易日】。

    🔴 2026-09-28:原本只跳週末 → 連假前一天會把「明天」算成休市日,
       掛牌前夜預告就推在錯的日子 (或該推的那天不推)。改用共用行事曆。
    """
    try:
        import tw_calendar
        return tw_calendar.next_trading_day().strftime("%Y-%m-%d")
    except Exception:
        d = datetime.now().date() + timedelta(days=1)      # 退路:至少跳週末
        while d.weekday() >= 5:
            d += timedelta(days=1)
        return d.strftime("%Y-%m-%d")


def tier_advice(est_open, theory):
    if est_open is None:
        return "無法估價,開盤自行比對理論價"
    if est_open < 102:
        return "❄️ 冷區(&lt;102)→ 跳過"
    if est_open < 110:
        return "😐 一般區(102~110)→ 小注或跳過"
    if est_open < 125:
        return "🔥 熱區(110~125)→ 開盤限價追,T+3收盤出(歷史+4.0%/66%)"
    return "⚠️ 過熱(&gt;125)→ 追價肉薄,只在折價/低溢價時進"



def sibling_history(db, stock4, exclude_cb, limit=4):
    """同一檔股票過去 CB 的首日表現(開盤買進 → 首日收/T+2收/T+3收)"""
    try:
        sibs = db.execute("""SELECT cb_code, company, substr(listing_date,1,10), method FROM issued
            WHERE substr(cb_code,1,4)=? AND cb_code!=? AND listing_date < date('now')
            AND listing_date != '未定' ORDER BY listing_date DESC LIMIT ?""",
            (stock4, str(exclude_cb), limit)).fetchall()
    except Exception:
        return []
    out = []
    token = ""
    try:
        token = open(FINMIND_TOKEN_FILE).read().strip()
    except Exception:
        return []
    for cb, name, lst, mth in sibs:
        try:
            url = ("https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockConvertibleBondDaily"
                   f"&data_id={cb}&start_date=2017-01-01&token={token}")
            with urllib.request.urlopen(url, timeout=30) as r:
                data = json.load(r).get("data") or []
            rows = [d for d in data if float(d.get("open") or 0) > 0]
            if len(rows) < 4:
                continue
            o1 = float(rows[0]["open"])
            cl = [float(x["close"]) for x in rows[:4]]
            out.append(f"  {cb} {name}({lst[:7]}·{mth or '?'}) 開盤{o1:.0f} → "
                       f"首日{(cl[0]/o1-1)*100:+.1f}% / T+2 {(cl[2]/o1-1)*100:+.1f}% / T+3 {(cl[3]/o1-1)*100:+.1f}%")
        except Exception:
            continue
    return out

STATE = os.path.join(HERE, "cb_listing_state.txt")


def load_state():
    """去重檔:JSON {"nd": 掛牌日, "sent": [已推過的代號]}。相容舊格式 (整檔只有一個日期 = 那天全推過)。"""
    try:
        raw = open(STATE, encoding="utf-8").read().strip()
    except Exception:
        return None, set(), False
    if not raw:
        return None, set(), False
    try:
        j = json.loads(raw)
        return j.get("nd"), set(j.get("sent") or []), False
    except Exception:
        return raw, set(), True          # 舊格式:只知道日期 → 當作那天的全部都推過


def save_state(nd, sent):
    try:
        with open(STATE, "w", encoding="utf-8") as f:
            json.dump({"nd": nd, "sent": sorted(sent)}, f, ensure_ascii=False)
    except Exception as e:
        print("[cb_listing] state write fail:", e)

def main():
    if not os.path.exists(DB):
        return
    # --date YYYY-MM-DD:指定掛牌日 (補推用)。--force:忽略「已發過」的去重。
    # 2026-08-17:排程因電腦晚開機錯過 8/16 觸發 → 今天開盤前才發現漏推,用這兩個參數補。
    argv = sys.argv[1:]
    forced = '--force' in argv
    nd = None
    if '--date' in argv:
        nd = argv[argv.index('--date') + 1]
    if not nd:
        nd = next_trading_day()
    today_flag = (nd == datetime.now().strftime('%Y-%m-%d'))
    # 2026-11-03 審查:舊版只記日期 → 第一輪 (18:40) 推完後,晚到的掛牌日 (統一證週報約 19:06 才到、
    #   TWSE 臨時改期) 整晚都被「已發過」擋掉。改成記【已推過的代號】,之後每輪只補推新增的那幾檔。
    st_nd, st_sent, st_legacy = load_state()
    if st_nd != nd:
        st_sent, st_legacy = set(), False
    if not forced and st_legacy:
        print(f"[cb_listing] {nd} 已發過 (舊格式去重檔),略過")
        return
    db = sqlite3.connect(DB)
    # 2026-10-26 颱風稽核:撥券日撞休市 (颱風/假日) 時 TWSE 常不改日期 (啟碁五 62855 撥券 2026-10-23 颱風),
    #   慣例首日 = 下一個交易日;原本精確比對 listing_date=nd → 10-27 真正掛牌那天漏推。
    #   改掃 nd 往前 10 天內的掛牌日,順延到交易日後等於 nd 的都算 (還沒有首日成交的才算,免得重推已掛牌的)。
    try:
        import tw_calendar
        _eff = lambda s: tw_calendar.effective_trading_day(s).strftime("%Y-%m-%d")
    except Exception:
        _eff = lambda s: s
    lo = (datetime.strptime(nd, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
    rows = [r for r in db.execute("""SELECT cb_code, company, method, conv_price, stock_code, substr(listing_date,1,10)
        FROM issued WHERE substr(listing_date,1,10) BETWEEN ? AND ?
          AND (is_withdrawn IS NULL OR is_withdrawn != 1) AND fm_cb_first_close IS NULL""", (lo, nd)).fetchall()
            if _eff(r[5]) == nd]
    if not rows:
        print(f"[cb_listing] {nd} 無掛牌")
        return
    is_followup = bool(st_sent) and not forced
    if not forced:
        rows = [r for r in rows if str(r[0]) not in st_sent]
        if not rows:
            print(f"[cb_listing] {nd} 已發過 ({len(st_sent)} 檔),沒有新增,略過")
            return
    _hdr = f"⚡ <b>今日({nd[5:]})CB 掛牌 — 開盤前提醒</b>" if today_flag \
           else f"📦 <b>明日({nd[5:]})CB 掛牌預告</b>"
    if is_followup:
        _hdr += " (補推:前一輪之後才進來的)"
    msgs = [_hdr]
    for cb, name, method, conv, scode, orig_ld in rows:
        defer_note = f" ⚠️原定{orig_ld[5:]}休市→順延(推估,以TWSE公告為準)" if orig_ld != nd else ""
        try:
            conv = float(conv)
        except Exception:
            conv = None
        theory = None
        conv_note = ""
        sc = stock_close(str(scode)[:4]) if scode else None
        if sc and conv:
            theory = sc / conv * 100
            if theory < 90:   # 深度價外可疑:轉換價可能是暫定舊值 → 交叉驗證
                fmcp = finmind_conv(str(cb))
                if fmcp and abs(fmcp - conv) / conv > 0.05:
                    theory = sc / fmcp * 100
                    conv_note = f"(DB轉換價{conv:g}疑為暫定,改用FinMind {fmcp:g})"
                elif not fmcp:
                    conv_note = "(⚠️轉換價未經訂價公告確認,理論價可能失準)"
        auc = db.execute("SELECT actual_price FROM auctions WHERE cb_code=?", (str(cb),)).fetchone()
        won = None
        if auc and auc[0]:
            try:
                won = float(auc[0])
            except Exception:
                pass
        if won and 80 < won < 250:
            est = won + 1.5
            basis = f"競拍得標 {won:.1f} → 預估開盤 {est:.1f}"
        elif theory:
            est = max(theory + 11, 100.5)   # 價外CB有票面地板,開盤不會低於~100
            basis = f"理論價 {theory:.1f} → 預估開盤 {est:.1f}{conv_note}"
        else:
            est = None
            basis = "資料不足"
        line = [f"\n<b>{cb} {name}</b>({method or '?'}){defer_note}", basis, tier_advice(est, theory)]
        hist = sibling_history(db, str(cb)[:4], cb)
        if hist:
            line.append("📜 同公司歷史CB首日表現:")
            line.extend(hist)
        if theory and theory >= 98:   # 折價追規則只適用價內檔
            line.append(f"⚖️ 明早第一盤對照:&lt;{theory:.0f} 折價全力追(75%)/ "
                        f"&lt;{theory+8:.0f} 可追 / &gt;{theory+15:.0f} 肉薄")
        msgs.append("\n".join(line))
    if tg_send("\n".join(msgs)):
        # 去重 (2026-10-26 稽核:舊版從沒寫 STATE,一晚推 5 次;11-03 改記代號,晚到的才補推)
        save_state(nd, st_sent | {str(r[0]) for r in rows})
    print(f"[cb_listing] sent {len(rows)} 檔")


if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    main()
