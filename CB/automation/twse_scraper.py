#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TWSE 競價拍賣公告爬蟲
來源：https://www.twse.com.tw/zh/announcement/auction.html
API：https://www.twse.com.tw/rwd/zh/announcement/auction
"""
import re
import time
import requests
from datetime import datetime
from config import TWSE_AUCTION_URL, CB_KEYWORDS

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Referer': 'https://www.twse.com.tw/zh/announcement/auction.html',
}

# TWSE 回傳的欄位順序（依官方表頭）
TWSE_FIELDS = [
    'serial',           # 序號
    'open_date',        # 開標日期
    'sec_name',         # 證券名稱
    'sec_code',         # 證券代號
    'market',           # 發行市場
    'sec_type',         # 發行性質
    'method',           # 競拍方式
    'bid_start',        # 投標開始日
    'bid_end',          # 投標結束日
    'auction_lots',     # 競拍數量(張)
    'min_bid_price',    # 最低投標價格(元)
    'min_lot_per_bid',  # 最低每標單投標數量(張)
    'max_lot',          # 最高投(得)標數量(張)
    'deposit_pct',      # 保證金成數(%)
    'fee_per_bid',      # 每一投標單投標處理費(元)
    'transfer_date',    # 撥券日期(上市、上櫃日期)
    'lead_mgr',         # 主辦券商
    'total_award',      # 得標總金額(元)
    'award_fee_rate',   # 得標手續費率(%)
    'total_valid',      # 總合格件
    'valid_lots',       # 合格投標數量(張)
    'min_award',        # 最低得標價格(元)
    'max_award',        # 最高得標價格(元)
    'weighted_avg',     # 得標加權平均價格(元)
    'actual_price',     # 實際承銷價格(元)
    'cancelled',        # 取消競價拍賣(流標或取消)
]


def fetch_twse_auction(year_roc: int = None, month: int = None) -> list[dict]:
    """
    抓取 TWSE 競拍公告資料（指定民國年/月）
    不指定時抓當月
    """
    now = datetime.now()
    if year_roc is None:
        year_roc = now.year - 1911
    if month is None:
        month = now.month

    params = {
        'year':  str(year_roc),
        'month': f'{month:02d}',
        'type':  'html',
    }

    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        resp = requests.get(TWSE_AUCTION_URL, params=params, headers=HEADERS,
                            timeout=20, verify=False)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f'  TWSE 請求失敗：{e}，改用 HTML 解析')
        return _fetch_twse_html(year_roc, month)
    except ValueError:
        return _fetch_twse_html(year_roc, month)

    rows = data.get('data', [])
    records = []
    for row in rows:
        if len(row) < len(TWSE_FIELDS):
            row = list(row) + [''] * (len(TWSE_FIELDS) - len(row))
        rec = dict(zip(TWSE_FIELDS, row))
        records.append(rec)

    return records


def _fetch_twse_html(year_roc: int, month: int) -> list[dict]:
    """備用：用 HTML 方式抓取 TWSE 頁面"""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print('  ⚠️  需要 beautifulsoup4：pip install beautifulsoup4')
        return []

    url = f'https://www.twse.com.tw/zh/announcement/auction.html'
    params = {'year': str(year_roc), 'month': f'{month:02d}'}
    resp = requests.get(url, params=params, headers=HEADERS, timeout=20, verify=False)
    soup = BeautifulSoup(resp.text, 'html.parser')

    table = soup.find('table')
    if not table:
        return []

    records = []
    rows = table.find_all('tr')
    for tr in rows[1:]:  # 跳過表頭
        cells = [td.get_text(strip=True) for td in tr.find_all('td')]
        if len(cells) < 10:
            continue
        if len(cells) < len(TWSE_FIELDS):
            cells = cells + [''] * (len(TWSE_FIELDS) - len(cells))
        rec = dict(zip(TWSE_FIELDS, cells))
        records.append(rec)

    return records


def fetch_recent_cb_auctions(months_back: int = 3) -> list[dict]:
    """
    抓取最近 N 個月的 CB 競拍資料
    自動過濾只保留轉換公司債
    """
    now = datetime.now()
    all_records = []

    for i in range(months_back):
        month = now.month - i
        year  = now.year - 1911
        while month <= 0:
            month += 12
            year -= 1

        print(f'  抓取 民國{year}年{month:02d}月...')
        recs = fetch_twse_auction(year, month)
        time.sleep(0.5)

        cb_recs = [r for r in recs if _is_cb(r)]
        print(f'    共 {len(recs)} 筆，CB {len(cb_recs)} 筆')
        all_records.extend(cb_recs)

    return all_records


def _is_cb(rec: dict) -> bool:
    """判斷是否為可轉換公司債"""
    sec_type = rec.get('sec_type', '')
    return any(kw in sec_type for kw in CB_KEYWORDS)


def clean_twse_record(rec: dict) -> dict:
    """清理並標準化一筆 TWSE 記錄，為寫入 Excel 做準備"""
    out = dict(rec)

    # CB 代碼（sec_code）
    code = str(out.get('sec_code', '')).strip()
    out['cb_code']    = code
    out['stock_code'] = code[:4] if len(code) >= 4 else ''

    # 擔保判斷：發行性質含「無擔保」→ NO，否則 YES
    sec_type = out.get('sec_type', '')
    if '無擔保' in sec_type:
        out['guarantee'] = 'NO'
    elif '有擔保' in sec_type:
        out['guarantee'] = 'YES'
    else:
        out['guarantee'] = ''

    # 數字欄位
    for field in ['auction_lots', 'min_bid_price', 'total_award',
                  'total_valid', 'valid_lots', 'min_award',
                  'max_award', 'weighted_avg', 'actual_price']:
        out[field] = _parse_number(out.get(field, ''))

    # 手續費率
    out['award_fee_rate'] = _parse_number(out.get('award_fee_rate', ''))

    # 競拍量(億元) = 競拍數量(張) × 10萬 / 1億 = 競拍數量 / 1000
    lots = out.get('auction_lots')
    out['auction_amount_yi'] = round(lots / 1000, 3) if lots else None

    # 日期欄位
    for f in ['open_date', 'bid_start', 'bid_end', 'transfer_date']:
        out[f] = _normalize_twse_date(str(out.get(f, '')))

    # 投標期間字串
    bs = out.get('bid_start', '')
    be = out.get('bid_end', '')
    if bs and be:
        # 只保留 M/D~M/D 格式
        bs_short = _to_short_date(bs)
        be_short = _to_short_date(be)
        out['bid_period'] = f'{bs_short}~{be_short}' if bs_short and be_short else ''
    else:
        out['bid_period'] = ''

    return out


def _parse_number(val) -> float | None:
    if val is None:
        return None
    s = str(val).replace(',', '').strip()
    if s in ('', '-', '—', 'nan', 'None', '流標', '取消'):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _normalize_twse_date(val: str) -> str:
    """民國年日期 → YYYY-MM-DD"""
    val = val.strip()
    if not val or val in ('-', '—'):
        return ''
    # 民國年格式 1150101 或 115/01/01
    m = re.match(r'^(\d{3})/(\d{1,2})/(\d{1,2})$', val)
    if m:
        return f'{int(m.group(1))+1911}-{int(m.group(2)):02d}-{int(m.group(3)):02d}'
    m = re.match(r'^(\d{3})(\d{2})(\d{2})$', val)
    if m:
        return f'{int(m.group(1))+1911}-{m.group(2)}-{m.group(3)}'
    # 西元年格式
    m = re.match(r'^(\d{4})/(\d{1,2})/(\d{1,2})$', val)
    if m:
        return f'{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}'
    return val


def _to_short_date(date_str: str) -> str:
    """YYYY-MM-DD → M/D"""
    m = re.match(r'^\d{4}-(\d{2})-(\d{2})$', date_str)
    if m:
        return f'{int(m.group(1))}/{int(m.group(2))}'
    return date_str
