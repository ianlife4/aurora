"""
IPO 競拍監控 + 拍價建議系統
- 抓取 TWSE 競拍公告
- 抓取興櫃/上櫃收盤價
- 抓取 MOPS 公開說明書財務摘要
- 計算綜合拍價建議
"""
import json, os, sys, time, re, math
from datetime import datetime, timedelta
from urllib.parse import urlencode

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

# Auto-detect: local or GitHub Actions
BASE = os.path.dirname(os.path.abspath(__file__))
OUTPUT = os.path.join(BASE, "_monitor_data.json")

SESSION = requests.Session()
SESSION.verify = False
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
})

# ============================================================
# Historical statistics (from existing 447-record analysis)
# ============================================================
HIST_PREMIUM = {
    "初上櫃": {"p25": 20.0, "p50": 35.0, "p75": 53.0, "mean": 38.7},
    "初上市": {"p25": 16.0, "p50": 30.0, "p75": 48.0, "mean": 33.2},
    "創新板": {"p25": 18.0, "p50": 34.0, "p75": 50.0, "mean": 36.5},
    "轉換公司債": {"p25": 2.0, "p50": 5.0, "p75": 8.0, "mean": 5.5},
}

MONTH_PREMIUM = {
    1: 33.0, 2: 42.0, 3: 44.0, 4: 35.0, 5: 32.0, 6: 34.0,
    7: 41.0, 8: 27.0, 9: 30.0, 10: 33.0, 11: 28.0, 12: 35.0
}

LOT_PREMIUM = {
    "small": {"label": "<=2000張", "p50": 38.7, "adj": 5.0},
    "mid":   {"label": "2000~5000張", "p50": 33.0, "adj": 0.0},
    "large": {"label": "5000~10000張", "p50": 27.0, "adj": -3.0},
    "xlarge":{"label": ">10000張", "p50": 20.5, "adj": -7.0},
}

DISCOUNT_STATS = {
    "safe_zone": {"min_discount": 0.10, "desc": "折扣10%以上，歷史正報酬率82%"},
    "ok_zone":   {"min_discount": 0.05, "desc": "折扣5~10%，歷史正報酬率67%"},
    "danger_zone": {"min_discount": 0.0, "desc": "折扣<5%或溢價，歷史正報酬率<15%"},
}

# ============================================================
# 1. Fetch TWSE Auction Data
# ============================================================
def fetch_twse_auction():
    """Fetch current auction announcements from TWSE."""
    url = "https://www.twse.com.tw/zh/announcement/auction"
    params = {"response": "json"}
    try:
        r = SESSION.get(url, params=params, timeout=20)
        r.encoding = 'utf-8'
        data = r.json()
        if data.get("stat") != "OK":
            print(f"[WARN] TWSE auction stat: {data.get('stat')}")
            return []

        fields = data.get("fields", [])
        rows = data.get("data", [])
        print(f"[INFO] TWSE auction: {len(rows)} entries, {len(fields)} fields")

        results = []
        for row in rows:
            # 26 fields: seq(0), open_date(1), name(2), code(3), market(4),
            # issue_type(5), auction_method(6), bid_start(7), bid_end(8),
            # lot_qty(9), min_bid_price(10), min_lot_unit(11), max_lot(12),
            # margin_pct(13), process_fee(14), listing_date(15), underwriter(16),
            # win_total_amt(17), win_fee_rate(18), qualified_bids(19),
            # qualified_qty(20), min_win_price(21), max_win_price(22),
            # weighted_avg_price(23), actual_underwrite_price(24), cancelled(25)
            def g(i): return row[i].strip() if len(row) > i else ""
            def gn(i): return row[i].strip().replace(",", "") if len(row) > i else ""

            entry = {}
            entry["seq"] = g(0)
            entry["open_date"] = g(1)
            entry["name"] = g(2)
            entry["code"] = g(3)
            entry["market"] = g(4)
            entry["issue_type"] = g(5)
            entry["auction_method"] = g(6)
            entry["bid_start"] = g(7)
            entry["bid_end"] = g(8)
            entry["lot_qty"] = gn(9)
            entry["min_bid_price"] = gn(10)
            entry["min_lot_unit"] = g(11)
            entry["max_lot"] = gn(12)
            entry["margin_pct"] = g(13)
            entry["process_fee"] = g(14)
            entry["listing_date"] = g(15)
            entry["underwriter"] = g(16)
            entry["win_total_amt"] = gn(17)
            entry["win_fee_rate"] = g(18)
            entry["qualified_bids"] = gn(19)
            entry["qualified_qty"] = gn(20)
            entry["min_win_price"] = gn(21)
            entry["max_win_price"] = gn(22)
            entry["weighted_avg_price"] = gn(23)
            entry["actual_underwrite_price"] = gn(24)
            entry["cancelled"] = g(25) if len(row) > 25 else ""

            # Classify
            entry["is_cb"] = len(entry["code"]) >= 5
            entry["status"] = classify_status(entry)
            entry["category"] = classify_category(entry)
            results.append(entry)

        return results
    except Exception as e:
        print(f"[ERROR] fetch_twse_auction: {e}")
        return []


def classify_status(entry):
    """Classify auction status: bidding / upcoming / awaiting_result / closed."""
    today = datetime.now().strftime("%Y/%m/%d")
    bid_start = entry.get("bid_start", "")
    bid_end = entry.get("bid_end", "")
    open_date = entry.get("open_date", "")
    min_win = entry.get("min_win_price", "")

    # "0" or empty means no result yet
    has_result = min_win and min_win not in ("", "0", "0.0")

    if has_result:
        return "closed"
    if bid_start and bid_end and bid_start <= today <= bid_end:
        return "bidding"
    if bid_start and today < bid_start:
        return "upcoming"
    if bid_end and today > bid_end and not has_result:
        return "awaiting_result"
    return "upcoming"


def classify_category(entry):
    """Classify as IPO type."""
    if entry["is_cb"]:
        return "轉換公司債"
    market = entry.get("market", "")
    issue = entry.get("issue_type", "")
    if "創新" in market or "創新" in issue:
        return "創新板"
    if "集中" in market or "上市" in market:
        return "初上市"
    return "初上櫃"


# ============================================================
# 2. Fetch Emerging Stock (興櫃) Price
# ============================================================
def fetch_emerging_price(stock_code):
    """Fetch latest emerging market (興櫃) price from TPEX new API."""
    now = datetime.now()

    # Strategy 1: TPEX emerging/historical — individual stock monthly data
    # Try current month, then previous month
    for month_offset in range(2):
        try:
            dt = now.replace(day=1) - timedelta(days=month_offset * 28)
            d_str = f"{dt.year}/{dt.month:02d}/01"
            url = "https://www.tpex.org.tw/www/zh-tw/emerging/historical"
            params = {'date': d_str, 'code': str(stock_code), 'response': 'json'}
            r = SESSION.get(url, params=params, timeout=15)
            data = r.json()
            # tables[0].data: rows of [date, volume, amount, high, low, avg_price, count, ...]
            if data.get('stat') == 'ok' and data.get('tables'):
                rows = data['tables'][0].get('data', [])
                if rows:
                    last_row = rows[-1]  # most recent trading day
                    avg_price = str(last_row[5]).replace(',', '').strip()
                    if avg_price and avg_price != '-' and avg_price != '0':
                        price = float(avg_price)
                        if price > 0:
                            return price
        except Exception:
            pass

    # Strategy 2: TPEX emerging/latest — all stocks for a given date
    # Try today, then up to 5 days back (weekends/holidays)
    for day_offset in range(6):
        try:
            dt = now - timedelta(days=day_offset)
            d_str = f"{dt.year}/{dt.month:02d}/{dt.day:02d}"
            url = "https://www.tpex.org.tw/www/zh-tw/emerging/latest"
            params = {'date': d_str, 'response': 'json'}
            r = SESSION.get(url, params=params, timeout=15)
            data = r.json()
            if data.get('stat') == 'ok' and data.get('tables'):
                rows = data['tables'][0].get('data', [])
                for row in rows:
                    if str(row[0]).strip() == str(stock_code).strip():
                        # index 9 = 日均價, index 10 = 成交(last trade)
                        avg_str = str(row[9]).replace(',', '').strip()
                        if avg_str and avg_str != '-' and avg_str != '0':
                            price = float(avg_str)
                            if price > 0:
                                return price
                        last_str = str(row[10]).replace(',', '').strip()
                        if last_str and last_str != '-' and last_str != '0':
                            price = float(last_str)
                            if price > 0:
                                return price
                if rows:  # had data for this date but stock not found — not emerging
                    break
        except Exception:
            pass

    # Strategy 3: TWSE listed market fallback (for stocks already listed)
    try:
        date_str = f"{now.year}{now.month:02d}01"
        url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
        params = {'response': 'json', 'date': date_str, 'stockNo': stock_code}
        r = SESSION.get(url, params=params, timeout=15)
        data = r.json()
        if 'data' in data and data['data']:
            last_row = data['data'][-1]
            close_str = last_row[6].replace(',', '').strip()
            return float(close_str)
    except Exception:
        pass

    return None


# ============================================================
# 3. Fetch MOPS Financial Summary
# ============================================================
def fetch_mops_financials(stock_code):
    """Fetch financial highlights from MOPS quarterly income statement (ajax_t164sb04)."""
    financials = {"eps": None, "revenue_growth": None, "gross_margin": None, "net_margin": None, "available": False}

    now = datetime.now()
    roc_year = now.year - 1911
    current_q = (now.month - 1) // 3  # 0-based: data lags by ~1 quarter

    # Build list of (year, season) to try, most recent first
    attempts = []
    for offset in range(4):
        q = current_q - offset
        y = roc_year
        while q <= 0:
            q += 4
            y -= 1
        attempts.append((y, q))

    # --- Strategy 1: Individual income statement (t164sb04) ---
    for year, season in attempts:
        try:
            url = "https://mopsov.twse.com.tw/mops/web/ajax_t164sb04"
            params = {'co_id': str(stock_code), 'year': str(year), 'season': str(season), 'step': '1', 'firstin': '1'}
            r = SESSION.get(url, params=params, timeout=20)
            r.encoding = 'utf-8'

            if '查無' in r.text:
                continue

            soup = BeautifulSoup(r.text, 'html.parser')
            tables = soup.find_all('table')
            if not tables:
                continue

            current_revenue = None
            prior_revenue = None

            for table in tables:
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 3:
                        continue
                    label = cells[0].get_text(strip=True)

                    # cells layout: label | curr_amt | curr_% | prev_amt | prev_%
                    if '營業收入' in label and '淨額' not in label and '成本' not in label:
                        try:
                            current_revenue = float(cells[1].get_text(strip=True).replace(',', ''))
                            if len(cells) >= 4:
                                prev_text = cells[3].get_text(strip=True).replace(',', '').replace('(', '-').replace(')', '')
                                if prev_text:
                                    prior_revenue = float(prev_text)
                        except (ValueError, IndexError):
                            pass

                    elif '營業毛利' in label and '率' not in label:
                        try:
                            pct_text = cells[2].get_text(strip=True).replace(',', '')
                            if pct_text:
                                financials["gross_margin"] = float(pct_text)
                                financials["available"] = True
                        except (ValueError, IndexError):
                            pass

                    elif any(k in label for k in ('本期淨利', '本期淨損', '本期稅後淨利', '本期稅後淨損')):
                        try:
                            pct_text = cells[2].get_text(strip=True).replace(',', '').replace('(', '-').replace(')', '')
                            if pct_text:
                                financials["net_margin"] = float(pct_text)
                                financials["available"] = True
                        except (ValueError, IndexError):
                            pass

                    elif '基本每股盈餘' in label:
                        try:
                            val = cells[1].get_text(strip=True).replace(',', '').replace('(', '-').replace(')', '')
                            if val:
                                financials["eps"] = float(val)
                                financials["available"] = True
                        except (ValueError, IndexError):
                            pass

            # Calculate revenue growth
            if current_revenue and prior_revenue and prior_revenue != 0:
                financials["revenue_growth"] = round((current_revenue / prior_revenue - 1) * 100, 2)
                financials["available"] = True

            if financials["available"]:
                print(f"  [INFO] MOPS: {stock_code} found data at {year}Q{season}")
                return financials

        except Exception as e:
            print(f"  [WARN] MOPS t164sb04 for {stock_code} ({year}Q{season}): {e}")
            continue

    # --- Strategy 2: Batch margin summary (t163sb06) fallback ---
    for year, season in attempts[:2]:
        try:
            url = "https://mopsov.twse.com.tw/mops/web/ajax_t163sb06"
            params = {'co_id': str(stock_code), 'year': str(year), 'season': str(season), 'step': '1', 'firstin': '1'}
            r = SESSION.get(url, params=params, timeout=20)
            r.encoding = 'utf-8'

            if '查無' in r.text:
                continue

            soup = BeautifulSoup(r.text, 'html.parser')
            for table in soup.find_all('table'):
                for row in table.find_all('tr'):
                    cells = row.find_all('td')
                    if len(cells) >= 7:
                        code_cell = cells[0].get_text(strip=True)
                        if code_cell == str(stock_code):
                            try:
                                gm = cells[3].get_text(strip=True)
                                nm = cells[6].get_text(strip=True)
                                if gm:
                                    financials["gross_margin"] = float(gm)
                                if nm:
                                    financials["net_margin"] = float(nm)
                                financials["available"] = True
                                print(f"  [INFO] MOPS fallback: {stock_code} found at {year}Q{season}")
                            except (ValueError, IndexError):
                                pass
                            return financials
        except Exception as e:
            print(f"  [WARN] MOPS t163sb06 for {stock_code} ({year}Q{season}): {e}")
            continue

    return financials


# ============================================================
# 4. Price Recommendation Model
# ============================================================
def lot_size_bucket(qty):
    """Classify lot quantity."""
    try:
        q = int(qty)
    except (ValueError, TypeError):
        return "mid"
    if q <= 2000:
        return "small"
    elif q <= 5000:
        return "mid"
    elif q <= 10000:
        return "large"
    else:
        return "xlarge"


def compute_recommendation(entry, emerging_price, financials):
    """Compute bid price recommendation using composite model."""
    rec = {
        "conservative": None, "moderate": None, "aggressive": None,
        "reasons": [], "risk_notes": [], "discount_ratio": None,
        "hist_premium_ref": None, "emerging_price": emerging_price,
    }

    min_bid = safe_float(entry.get("min_bid_price"))
    if not min_bid or min_bid <= 0:
        rec["reasons"].append("無法取得最低投標價，無法計算建議")
        return rec

    category = entry.get("category", "初上櫃")
    hist = HIST_PREMIUM.get(category, HIST_PREMIUM["初上櫃"])
    rec["hist_premium_ref"] = hist

    # --- Factor 1: Historical Premium by Category (weight: 0.35) ---
    base_p25 = hist["p25"]
    base_p50 = hist["p50"]
    base_p75 = hist["p75"]

    # --- Factor 2: Emerging Market Discount (weight: 0.35) ---
    discount_adj = 0
    if emerging_price and emerging_price > 0:
        discount_ratio = (min_bid / emerging_price - 1) * 100
        rec["discount_ratio"] = round(discount_ratio, 2)

        if discount_ratio <= -25:
            discount_adj = -8
            rec["reasons"].append(f"興櫃折扣比 {discount_ratio:.1f}%（大幅折扣），歷史正報酬率 >85%，可積極出價")
        elif discount_ratio <= -15:
            discount_adj = -4
            rec["reasons"].append(f"興櫃折扣比 {discount_ratio:.1f}%（適度折扣），歷史正報酬率 82%")
        elif discount_ratio <= -5:
            discount_adj = 0
            rec["reasons"].append(f"興櫃折扣比 {discount_ratio:.1f}%（小幅折扣），歷史正報酬率 67%")
        elif discount_ratio <= 0:
            discount_adj = 5
            rec["reasons"].append(f"興櫃折扣比 {discount_ratio:.1f}%（幾乎無折扣），歷史正報酬率僅 8%，建議保守")
            rec["risk_notes"].append("折扣不足5%，歷史統計顯示此區間多數虧損")
        else:
            discount_adj = 10
            rec["reasons"].append(f"最低投標價已高於興櫃價 {discount_ratio:.1f}%，歷史平均虧損 -19%")
            rec["risk_notes"].append("溢價投標，歷史數據顯示 85% 機率虧損")
    else:
        rec["reasons"].append("無興櫃價格資料，無法計算折扣比")

    # --- Factor 3: Financial Indicators (weight: 0.15) ---
    fin_adj = 0
    if financials and financials.get("available"):
        eps = financials.get("eps")
        rev_growth = financials.get("revenue_growth")
        gross_margin = financials.get("gross_margin")

        fin_notes = []
        if eps is not None:
            if eps > 5:
                fin_adj += 3
                fin_notes.append(f"EPS {eps:.2f} 元（優良）")
            elif eps > 2:
                fin_adj += 1
                fin_notes.append(f"EPS {eps:.2f} 元（中等）")
            elif eps > 0:
                fin_notes.append(f"EPS {eps:.2f} 元（偏低）")
            else:
                fin_adj -= 3
                fin_notes.append(f"EPS {eps:.2f} 元（虧損）")
                rec["risk_notes"].append("公司尚未獲利")

        if rev_growth is not None:
            if rev_growth > 30:
                fin_adj += 2
                fin_notes.append(f"營收成長 {rev_growth:.1f}%（高成長）")
            elif rev_growth > 10:
                fin_adj += 1
                fin_notes.append(f"營收成長 {rev_growth:.1f}%")
            elif rev_growth < 0:
                fin_adj -= 2
                fin_notes.append(f"營收衰退 {rev_growth:.1f}%")

        if gross_margin is not None:
            if gross_margin > 40:
                fin_adj += 1
                fin_notes.append(f"毛利率 {gross_margin:.1f}%（高毛利）")
            elif gross_margin < 15:
                fin_adj -= 1
                fin_notes.append(f"毛利率 {gross_margin:.1f}%（偏低）")

        if fin_notes:
            rec["reasons"].append("財務面：" + "、".join(fin_notes))
    else:
        rec["reasons"].append("公開說明書財務資料暫無法取得")

    # --- Factor 4: Month/Season (weight: 0.10) ---
    current_month = datetime.now().month
    month_base = MONTH_PREMIUM.get(current_month, 33.0)
    month_adj = (month_base - 33.0) * 0.3
    if abs(month_adj) > 1:
        rec["reasons"].append(f"{current_month}月歷史溢價中位 {month_base:.0f}%（{'偏高' if month_adj > 0 else '偏低'}）")

    # --- Factor 5: Lot Size (weight: 0.05) ---
    bucket = lot_size_bucket(entry.get("lot_qty"))
    lot_info = LOT_PREMIUM.get(bucket, LOT_PREMIUM["mid"])
    lot_adj = lot_info["adj"]
    if lot_adj != 0:
        rec["reasons"].append(f"競拍張數 {entry.get('lot_qty', '?')} 張（{lot_info['label']}），{'容易搶高' if lot_adj > 0 else '籌碼多可保守'}")

    # --- Composite ---
    total_adj = discount_adj * 0.35 + fin_adj * 0.15 + month_adj * 0.10 + lot_adj * 0.05

    p25_final = max(0, base_p25 + total_adj)
    p50_final = max(0, base_p50 + total_adj)
    p75_final = max(0, base_p75 + total_adj)

    # If we have emerging price, also use discount-based approach
    if emerging_price and emerging_price > 0:
        # Target: 得標價約興櫃價的 85-90% (折扣10-15%) for safe zone
        safe_price = emerging_price * 0.88
        ok_price = emerging_price * 0.92
        if safe_price > min_bid:
            alt_conservative = ((safe_price / min_bid) - 1) * 100
            alt_moderate = ((ok_price / min_bid) - 1) * 100
            alt_aggressive = ((emerging_price * 0.95 / min_bid) - 1) * 100

            # Blend with historical approach
            p25_final = (p25_final + alt_conservative) / 2
            p50_final = (p50_final + alt_moderate) / 2
            p75_final = (p75_final + alt_aggressive) / 2

    rec["conservative"] = round(min_bid * (1 + p25_final / 100), 2)
    rec["moderate"] = round(min_bid * (1 + p50_final / 100), 2)
    rec["aggressive"] = round(min_bid * (1 + p75_final / 100), 2)

    rec["premium_conservative"] = round(p25_final, 1)
    rec["premium_moderate"] = round(p50_final, 1)
    rec["premium_aggressive"] = round(p75_final, 1)

    return rec


def safe_float(v):
    try:
        return float(str(v).replace(",", ""))
    except (ValueError, TypeError):
        return None


# ============================================================
# Main
# ============================================================
def main():
    print("=" * 60)
    print("IPO 競拍監控系統 — 資料更新")
    print(f"時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Step 1: Fetch TWSE auction data
    print("\n[1/4] 抓取 TWSE 競拍公告...")
    auctions = fetch_twse_auction()
    if not auctions:
        print("[ERROR] 無法取得競拍公告資料")
        return

    # Separate by status
    bidding = [a for a in auctions if a["status"] == "bidding"]
    upcoming = [a for a in auctions if a["status"] == "upcoming"]
    awaiting = [a for a in auctions if a["status"] == "awaiting_result"]
    closed = [a for a in auctions if a["status"] == "closed"]
    print(f"  投標中: {len(bidding)}, 即將開標: {len(upcoming)}, 待開標: {len(awaiting)}, 已結標: {len(closed)}")

    # Step 2: For active/upcoming IPOs, fetch emerging prices
    active_ipos = [a for a in (bidding + upcoming + awaiting) if not a["is_cb"]]
    print(f"\n[2/4] 抓取興櫃價格 ({len(active_ipos)} 檔 IPO)...")
    for a in active_ipos:
        code = a["code"]
        price = fetch_emerging_price(code)
        a["emerging_price"] = price
        status_str = f"${price:.2f}" if price else "N/A"
        print(f"  {a['name']} ({code}): 興櫃價 {status_str}")
        time.sleep(1.5)

    # Step 3: Fetch MOPS financials for active IPOs
    print(f"\n[3/4] 抓取公開說明書財務資料...")
    for a in active_ipos:
        code = a["code"]
        fin = fetch_mops_financials(code)
        a["financials"] = fin
        if fin["available"]:
            parts = []
            if fin["eps"] is not None: parts.append(f"EPS={fin['eps']}")
            if fin["revenue_growth"] is not None: parts.append(f"營收成長={fin['revenue_growth']}%")
            if fin["gross_margin"] is not None: parts.append(f"毛利率={fin['gross_margin']}%")
            print(f"  {a['name']} ({code}): {', '.join(parts)}")
        else:
            print(f"  {a['name']} ({code}): 暫無財務資料")
        time.sleep(2)

    # Step 4: Compute recommendations
    print(f"\n[4/4] 計算拍價建議...")
    for a in active_ipos:
        emerging = a.get("emerging_price")
        fin = a.get("financials", {})
        rec = compute_recommendation(a, emerging, fin)
        a["recommendation"] = rec
        if rec["moderate"]:
            print(f"  {a['name']}: 保守 ${rec['conservative']:.2f} / 中性 ${rec['moderate']:.2f} / 積極 ${rec['aggressive']:.2f}")
        else:
            print(f"  {a['name']}: 無法計算建議")

    # Build output
    output = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "summary": {
            "total": len(auctions),
            "bidding": len(bidding),
            "upcoming": len(upcoming),
            "awaiting_result": len(awaiting),
            "closed": len(closed),
            "ipo_count": sum(1 for a in auctions if not a["is_cb"]),
            "cb_count": sum(1 for a in auctions if a["is_cb"]),
        },
        "auctions": auctions,
        "hist_premium": HIST_PREMIUM,
        "month_premium": {str(k): v for k, v in MONTH_PREMIUM.items()},
    }

    with open(OUTPUT, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 60}")
    print(f"完成！資料已儲存至: {OUTPUT}")
    print(f"共 {len(auctions)} 筆，其中 {len(active_ipos)} 檔 IPO 有拍價建議")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
