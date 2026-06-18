#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""從 TWSE 官方公告抓「即將開標」CB 競拍清單。

來源: https://www.twse.com.tw/rwd/zh/announcement/auction?response=json&yy=YYYY
判斷「即將開標」: col[23] (得標加權平均價) = 0/'-' (尚未開標)

寫入 SQLite 表 `upcoming_auctions`:
  cb_code, company, stock_code, auction_date, bid_start, bid_end,
  amount_lots, min_bid_price, lead_mgr, listing_date, market, etc.

執行: py -3.12 fetch_twse_upcoming.py
"""
import io
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import requests

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / 'cb_data.db'
URL = 'https://www.twse.com.tw/rwd/zh/announcement/auction'


def init_table(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS upcoming_auctions (
        cb_code        TEXT PRIMARY KEY,
        company        TEXT,
        stock_code     TEXT,
        auction_date   TEXT,    -- 開標日期 YYYY-MM-DD
        bid_start      TEXT,    -- 投標開始日 YYYY-MM-DD
        bid_end        TEXT,    -- 投標結束日 YYYY-MM-DD
        amount_lots    INTEGER, -- 競拍張數
        min_bid_price  REAL,    -- 最低投標價
        margin_pct     REAL,    -- 保證金成數%
        lead_mgr       TEXT,    -- 主辦券商
        listing_date   TEXT,    -- 撥券/上市日 YYYY-MM-DD
        market         TEXT,    -- 發行市場 (上市/上櫃/興櫃/公發)
        nature         TEXT,    -- 發行性質
        method         TEXT,    -- 競拍方式
        is_cancelled   INTEGER, -- 取消旗標 1=流標/取消, 0=正常
        updated_at     TEXT
    )''')
    conn.commit()


def roc_to_iso(s: str) -> str:
    """民國日期 'YYYY/MM/DD' → ISO 'YYYY-MM-DD' (這個 API 已是西元，直接 / → -)"""
    if not s or s in ('-', '—', ''):
        return ''
    return str(s).replace('/', '-')


def to_int(s) -> int | None:
    if s in (None, '', '-', '—'): return None
    try:
        return int(str(s).replace(',', ''))
    except: return None


def to_float(s) -> float | None:
    if s in (None, '', '-', '—'): return None
    try:
        return float(str(s).replace(',', ''))
    except: return None


def fetch_year_all(yy: int) -> tuple[list[dict], list[dict]]:
    """抓某年 (西元) TWSE 競拍公告。回傳 (upcoming, opened) 兩 list。
    upcoming: 還沒開標 (加權平均=0) → 寫 upcoming_auctions
    opened: 已開標 (加權平均>0) → 同步回 auctions 表（修正 stale 加權）"""
    try:
        r = requests.get(URL, params={'response':'json','yy':str(yy)}, timeout=30)
        r.raise_for_status()
        d = r.json()
    except Exception as e:
        print(f'  [ERR] yy={yy}: {e}')
        return [], []
    data = d.get('data', [])
    upcoming, opened = [], []
    for row in data:
        nature = str(row[5] if len(row) > 5 else '').strip()
        if '轉換公司債' not in nature:
            continue
        avg_price_raw = row[23] if len(row) > 23 else ''
        is_upcoming = avg_price_raw in ('0', '0.00', '0.0', 0, '-', '—', '', None)
        cancelled_flag = row[25] if len(row) > 25 else ''
        is_cancelled = 1 if cancelled_flag and cancelled_flag not in ('-', '—', '') else 0
        if not is_upcoming:
            # 已開標：收集到 opened (所有 TWSE 欄位)
            opened.append({
                'cb_code':         str(row[3]).strip(),
                'company':         str(row[2]).strip(),
                'auction_date':    roc_to_iso(row[1]),
                'auction_lots':    to_int(row[9]),
                'min_bid_pct':     to_float(row[10]),
                'total_award_amt': to_float(row[17]),
                'total_valid':     to_int(row[19]),
                'valid_lots':      to_int(row[20]),
                'min_award_pct':   to_float(row[21]),
                'max_award_pct':   to_float(row[22]),
                'weighted_avg':    to_float(row[23]),
                'avg_award_pct':   to_float(row[23]),
                'actual_price':    to_float(row[24]),
            })
            continue
        upcoming.append({
            'cb_code':       str(row[3]).strip(),
            'company':       str(row[2]).strip(),
            'stock_code':    str(row[3])[:4] if len(str(row[3])) >= 4 else '',
            'auction_date':  roc_to_iso(row[1]),
            'bid_start':     roc_to_iso(row[7]),
            'bid_end':       roc_to_iso(row[8]),
            'amount_lots':   to_int(row[9]),
            'min_bid_price': to_float(row[10]),
            'margin_pct':    to_float(row[13]),
            'lead_mgr':      str(row[16]).strip(),
            'listing_date':  roc_to_iso(row[15]),
            'market':        str(row[4]).strip(),
            'nature':        str(row[5]).strip(),
            'method':        str(row[6]).strip(),
            'is_cancelled':  is_cancelled,
        })
    return upcoming, opened


def main():
    conn = sqlite3.connect(str(DB_PATH))
    init_table(conn)
    c = conn.cursor()

    now = datetime.now()
    years = [now.year]
    if now.month >= 11:
        years.append(now.year + 1)
    # 也抓上一年 (回填可能 stale 的開標結果)
    years.append(now.year - 1)

    print(f'從 TWSE 抓 競拍公告 (西元年: {sorted(set(years))})')
    all_upcoming = []
    all_opened = []
    for yy in sorted(set(years)):
        upcoming, opened = fetch_year_all(yy)
        print(f'  yy={yy}: {len(upcoming)} 筆即將開標 / {len(opened)} 筆已開標')
        all_upcoming.extend(upcoming)
        all_opened.extend(opened)

    # 去重 (跨年 bucket 重複)
    all_upcoming = list({r['cb_code']: r for r in all_upcoming}.values())
    all_opened = list({r['cb_code']: r for r in all_opened}.values())

    # 寫入 upcoming_auctions (清空重寫)
    c.execute('DELETE FROM upcoming_auctions')
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    issued_synced = 0
    for r in all_upcoming:
        c.execute('''INSERT OR REPLACE INTO upcoming_auctions
            (cb_code, company, stock_code, auction_date, bid_start, bid_end,
             amount_lots, min_bid_price, margin_pct, lead_mgr, listing_date,
             market, nature, method, is_cancelled, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (r['cb_code'], r['company'], r['stock_code'], r['auction_date'],
             r['bid_start'], r['bid_end'], r['amount_lots'], r['min_bid_price'],
             r['margin_pct'], r['lead_mgr'], r['listing_date'],
             r['market'], r['nature'], r['method'], r['is_cancelled'], now_str))
        # 把 bid/listing 同步回 issued 表 (timeline 渲染靠這些欄位,空著就退化成預估)。
        # 只填空白/未定欄位,避免覆蓋既有值。
        if c.execute('SELECT 1 FROM issued WHERE cb_code=?', (r['cb_code'],)).fetchone():
            upd = c.execute('''UPDATE issued SET
                       fm_bid_start_date = COALESCE(NULLIF(fm_bid_start_date,''), ?),
                       fm_bid_end_date   = COALESCE(NULLIF(fm_bid_end_date,''),   ?),
                       listing_date      = CASE WHEN listing_date IS NULL OR listing_date='' OR listing_date='未定'
                                                THEN ? || ' 00:00:00' ELSE listing_date END,
                       bid_period        = CASE WHEN bid_period IS NULL OR bid_period='' OR bid_period IN ('競拍','詢圈')
                                                THEN ? || '~' || ? ELSE bid_period END,
                       updated_at = ?
                       WHERE cb_code=?''',
                (r['bid_start'], r['bid_end'],
                 r['listing_date'] or '',
                 r['bid_start'] or '', r['bid_end'] or '',
                 now_str, r['cb_code']))
            if upd.rowcount > 0:
                issued_synced += 1

    # 同步 opened 到 auctions 表 (修正 stale 任何欄位)
    sync_count = 0
    sync_fields = ['auction_lots','min_bid_pct','total_award_amt','total_valid','valid_lots',
                   'min_award_pct','max_award_pct','weighted_avg','avg_award_pct','actual_price']
    for op in all_opened:
        cur = c.execute(f"SELECT {','.join(sync_fields)} FROM auctions WHERE cb_code=?", (op['cb_code'],))
        r = cur.fetchone()
        if not r:
            continue  # 還沒進 auctions 表 (twse_scraper 會處理)
        db_vals = dict(zip(sync_fields, r))
        # 任一欄位有差就 update 全部 (確保一致)
        diff_fields = []
        for f in sync_fields:
            tv = op[f]
            dv = db_vals[f] or 0
            if tv is None: continue
            tol = max(0.5, abs(tv) * 0.001)  # 0.1% 容差或 0.5
            if abs(dv - tv) > tol:
                diff_fields.append(f'{f}({dv}→{tv})')
        if not diff_fields:
            continue
        sets = ', '.join(f'{f}=?' for f in sync_fields) + ', updated_at=?'
        vals = [op[f] for f in sync_fields] + [now_str, op['cb_code']]
        c.execute(f"UPDATE auctions SET {sets} WHERE cb_code=?", vals)
        sync_count += 1
        if sync_count <= 10:
            print(f'  sync {op["cb_code"]} {op["company"]}: {", ".join(diff_fields[:3])}')
    conn.commit()

    print()
    print('=== 結果 ===')
    print(f'  共寫入 {len(all_upcoming)} 筆 upcoming_auctions')
    print(f'  同步 {issued_synced} 筆到 issued 表 (bid/listing 帶回供 timeline 用)')
    print(f'  同步修正 {sync_count} 筆 auctions (TWSE 加權平均更新)')
    # 列出所有 (按開標日排序)
    print()
    print('=== 即將開標清單 ===')
    for r in sorted(all_upcoming, key=lambda x: x['auction_date']):
        flag = ' [取消]' if r['is_cancelled'] else ''
        print(f'  {r["auction_date"]}  {r["cb_code"]:6} {r["company"]:14} 投標={r["bid_start"]}~{r["bid_end"]} 額={r["amount_lots"]:>6} 主辦={r["lead_mgr"]} 撥券={r["listing_date"]}{flag}')

    conn.close()


if __name__ == '__main__':
    main()
