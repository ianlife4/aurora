"""
Fetch closing prices for auction stocks from TWSE/TPEX.
Uses individual stock monthly data API for efficiency.
"""
import json, os, sys, time, requests, re
from datetime import datetime, timedelta
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

BASE = "C:/Users/J.Chun/Desktop/競拍資歷"

with open(os.path.join(BASE, '_parsed_data.json'), 'r', encoding='utf-8') as f:
    records = json.load(f)

print(f"Total records: {len(records)}")

date_pattern = re.compile(r'^\d{4}/\d{1,2}/\d{1,2}$')

def parse_date(d):
    parts = d.split('/')
    return datetime(int(parts[0]), int(parts[1]), int(parts[2]))

def is_cb(code):
    """Check if this is a convertible bond (5+ digit code)"""
    return len(code) >= 5

def underlying_code(code):
    """Get underlying stock code for CB"""
    return code[:4]

def is_otc(market):
    return '櫃' in market or 'OTC' in market.upper()

# Build list of (stock_code, date, market_type) pairs we need
needs = []  # list of (lookup_code, date_str, market_is_otc, record_idx, field)
now = datetime.now()

for i, rec in enumerate(records):
    code = rec['code']
    market = rec.get('market', '')
    otc = is_otc(market)

    # For listing date: use the actual code for IPOs (first day of trading)
    # For CBs: use underlying stock code
    listing_date = rec.get('listing_date', '').strip()
    bid_end = rec.get('bid_end', '').strip()

    if is_cb(code):
        lookup = underlying_code(code)
    else:
        lookup = code

    if listing_date and date_pattern.match(listing_date):
        dt = parse_date(listing_date)
        if dt <= now:
            needs.append((lookup, listing_date, otc, i, 'listing'))

    if bid_end and date_pattern.match(bid_end):
        dt = parse_date(bid_end)
        if dt <= now:
            # For IPOs, stock doesn't trade yet on bid_end, but we still try
            # (it will just not find data, which is expected)
            needs.append((lookup, bid_end, otc, i, 'bidend'))

print(f"Total price lookups needed: {len(needs)}")

# Group by (lookup_code, year_month, is_otc) to minimize API calls
# TWSE API returns full month data per stock
month_groups = defaultdict(list)  # (code, YYYYMM, otc) -> [(date_str, rec_idx, field)]
for lookup, date_str, otc, idx, field in needs:
    dt = parse_date(date_str)
    ym = f"{dt.year}{dt.month:02d}"
    key = (lookup, ym, otc)
    month_groups[key].append((date_str, idx, field))

print(f"Unique (stock, month) pairs to fetch: {len(month_groups)}")

# Fetch functions
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
})

def fetch_twse_monthly(stock_code, year, month):
    """Fetch monthly trading data for a TWSE stock. Returns {date_str: close_price}"""
    date_str = f"{year}{month:02d}01"
    url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
    params = {
        'response': 'json',
        'date': date_str,
        'stockNo': stock_code
    }
    try:
        r = session.get(url, params=params, timeout=15)
        data = r.json()
        result = {}
        if 'data' in data:
            for row in data['data']:
                # Date is in ROC format like 113/12/31
                roc_date = row[0].strip()
                parts = roc_date.split('/')
                ad_year = int(parts[0]) + 1911
                date_key = f"{ad_year}/{int(parts[1]):02d}/{int(parts[2]):02d}"
                close_str = row[6].replace(',', '').strip()
                try:
                    result[date_key] = float(close_str)
                except:
                    pass
        return result
    except Exception as e:
        return {}

def fetch_tpex_monthly(stock_code, year, month):
    """Fetch monthly trading data for a TPEX (OTC) stock. Returns {date_str: close_price}"""
    roc_year = year - 1911
    d_str = f"{roc_year}/{month:02d}/01"
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
    params = {
        'l': 'zh-tw',
        'o': 'json',
        'd': d_str,
        'stkno': stock_code,
        's': '0,asc,0'
    }
    try:
        r = session.get(url, params=params, timeout=15)
        data = r.json()
        result = {}
        if 'aaData' in data:
            for row in data['aaData']:
                roc_date = row[0].strip()
                parts = roc_date.split('/')
                ad_year = int(parts[0]) + 1911
                date_key = f"{ad_year}/{int(parts[1]):02d}/{int(parts[2]):02d}"
                close_str = row[6].replace(',', '').strip()
                try:
                    result[date_key] = float(close_str)
                except:
                    pass
        return result
    except Exception as e:
        return {}

# Process all groups
price_data = {}  # (code, date_str) -> close_price
total_groups = len(month_groups)
fetched = 0
errors = 0

# Try to load existing cache
cache_file = os.path.join(BASE, '_price_cache2.json')
if os.path.exists(cache_file):
    with open(cache_file, 'r', encoding='utf-8') as f:
        price_data = json.load(f)
    print(f"Loaded {len(price_data)} cached prices")

for gi, ((code, ym, otc), lookups) in enumerate(sorted(month_groups.items())):
    year = int(ym[:4])
    month = int(ym[4:])

    # Check if all needed dates are already cached
    all_cached = True
    for date_str, idx, field in lookups:
        cache_key = f"{code}|{date_str}"
        if cache_key not in price_data:
            all_cached = False
            break

    if all_cached:
        continue

    # Fetch
    if otc:
        month_prices = fetch_tpex_monthly(code, year, month)
        time.sleep(1.5)
    else:
        month_prices = fetch_twse_monthly(code, year, month)
        time.sleep(3.2)

    fetched += 1

    # Store results
    for date_str, idx, field in lookups:
        cache_key = f"{code}|{date_str}"
        if date_str in month_prices:
            price_data[cache_key] = month_prices[date_str]
        else:
            # Try nearby dates (stock might not trade that exact day)
            price_data[cache_key] = None

    # If TWSE returned empty, try TPEX and vice versa
    if not month_prices:
        if otc:
            month_prices = fetch_twse_monthly(code, year, month)
            time.sleep(3.2)
        else:
            month_prices = fetch_tpex_monthly(code, year, month)
            time.sleep(1.5)
        fetched += 1

        for date_str, idx, field in lookups:
            cache_key = f"{code}|{date_str}"
            if date_str in month_prices:
                price_data[cache_key] = month_prices[date_str]

    if (gi + 1) % 50 == 0:
        print(f"  [{gi+1}/{total_groups}] fetched={fetched}, found prices={sum(1 for v in price_data.values() if v is not None)}")
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(price_data, f)

# Final save
with open(cache_file, 'w', encoding='utf-8') as f:
    json.dump(price_data, f)

print(f"\nTotal API calls: {fetched}")
print(f"Prices found: {sum(1 for v in price_data.values() if v is not None)}/{len(price_data)}")

# Build results
results = []
for i, rec in enumerate(records):
    code = rec['code']
    min_price = float(rec['min_win_price'].replace(',', ''))
    lookup = underlying_code(code) if is_cb(code) else code

    listing_date = rec.get('listing_date', '').strip()
    bid_end = rec.get('bid_end', '').strip()

    listing_close = price_data.get(f"{lookup}|{listing_date}")
    bidend_close = price_data.get(f"{lookup}|{bid_end}")

    listing_spread = None
    listing_spread_pct = None
    if listing_close and min_price:
        listing_spread = round(listing_close - min_price, 2)
        listing_spread_pct = round((listing_spread / min_price) * 100, 2)

    bidend_spread = None
    bidend_spread_pct = None
    if bidend_close and min_price:
        bidend_spread = round(bidend_close - min_price, 2)
        bidend_spread_pct = round((bidend_spread / min_price) * 100, 2)

    results.append({
        **rec,
        'is_cb': is_cb(code),
        'lookup_code': lookup,
        'min_price_float': min_price,
        'listing_close': listing_close,
        'bidend_close': bidend_close,
        'listing_spread': listing_spread,
        'listing_spread_pct': listing_spread_pct,
        'bidend_spread': bidend_spread,
        'bidend_spread_pct': bidend_spread_pct,
    })

with open(os.path.join(BASE, '_results.json'), 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

# Summary stats
listing_found = sum(1 for r in results if r['listing_close'] is not None)
bidend_found = sum(1 for r in results if r['bidend_close'] is not None)
print(f"\nResults: {len(results)} records")
print(f"Listing close prices found: {listing_found}/{len(results)}")
print(f"Bid-end close prices found: {bidend_found}/{len(results)}")
print("Saved to _results.json")
