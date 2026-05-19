#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日抓 TWSE 競拍結果 → 補進 auctions 表
來源: https://www.twse.com.tw/zh/announcement/auction.html

對「已開標但 actual_price 是 NULL」的 CB,從 TWSE 公告抓:
  - min_award (最低得標元價) → min_award_pct
  - max_award (最高得標元價) → max_award_pct
  - weighted_avg (加權平均) → weighted_avg + actual_price

執行:
  py -3.12 fetch_twse_auction_results.py              # 抓近 3 個月
  py -3.12 fetch_twse_auction_results.py --months 6   # 抓近 6 個月
  py -3.12 fetch_twse_auction_results.py --force      # 覆寫已有
"""
import argparse
import io
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

HERE = Path(__file__).parent
DB_PATH = HERE / 'cb_data.db'

sys.path.insert(0, str(HERE))
from twse_scraper import fetch_twse_auction, clean_twse_record


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--months', type=int, default=3, help='抓近 N 個月 (default 3)')
    ap.add_argument('--force', action='store_true', help='覆寫已有 actual_price')
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    now = datetime.now()
    yr_roc = now.year - 1911
    all_rows = []
    for mo_off in range(args.months):
        yr, mo = yr_roc, now.month - mo_off
        while mo < 1:
            mo += 12
            yr -= 1
        rows = fetch_twse_auction(yr, mo)
        all_rows.extend(rows)
        print(f'  TWSE 民 {yr}/{mo:02d}: {len(rows)} 筆')

    print(f'\n總共 {len(all_rows)} 筆 TWSE 公告')

    updated = 0
    inserted = 0
    skipped = 0
    not_in_issued = 0
    no_result = 0
    ts = now.strftime('%Y-%m-%d %H:%M:%S')

    for raw in all_rows:
        cb = str(raw.get('sec_code', '')).strip()
        if not cb or len(cb) < 5:
            continue  # 非 CB (IPO/現增 等)
        clean = clean_twse_record(raw)
        wa = clean.get('weighted_avg')
        if not wa or wa == 0:
            no_result += 1
            continue  # 還沒開標

        min_award = clean.get('min_award')
        max_award = clean.get('max_award')
        actual = clean.get('actual_price') or wa
        # 也帶其他可用欄位 (給 INSERT 新案用)
        open_date = clean.get('open_date') or ''
        company = clean.get('company') or raw.get('sec_name', '')
        stock_code = (cb[:4]) if len(cb) >= 5 else ''  # CB code 前 4 碼 = 股票代號
        issue_amount = clean.get('issue_amount')
        auction_amount = clean.get('auction_amount')
        bid_date = clean.get('bid_end') or clean.get('bid_start') or ''
        listing_date = clean.get('listing_date') or ''
        lead_mgr = clean.get('lead_mgr') or ''
        total_award_amt = clean.get('total_award')
        auction_lots = clean.get('auction_lots')
        min_bid_pct = clean.get('min_bid_price')

        existing = cur.execute(
            'SELECT actual_price FROM auctions WHERE cb_code=?', (cb,)
        ).fetchone()

        if not existing:
            # 新案 → INSERT (剛開標但 auctions 表沒紀錄,例 47491 第一次開標)
            # 從 issued 取必要欄位補齊 (term/tcri/guarantee)
            iss = cur.execute(
                'SELECT term, tcri, fm_bid_start_date, fm_bid_end_date FROM issued WHERE cb_code=?', (cb,)
            ).fetchone()
            if not iss:
                not_in_issued += 1
                print(f'  ⚠ {cb} 在 TWSE 公告但 issued 表沒紀錄 — skip')
                continue
            cur.execute('''INSERT INTO auctions (
                cb_code, company, stock_code, auction_date, bid_date, listing_date,
                issue_amount, auction_amount, auction_lots, min_bid_pct,
                term, tcri, guarantee, lead_mgr, total_award_amt,
                actual_price, min_award_pct, max_award_pct,
                updated_at
            ) VALUES (?,?,?,?,?,?, ?,?,?,?, ?,?,?,?,?, ?,?,?, ?)''',
                (cb, company, stock_code, open_date, bid_date, listing_date,
                 issue_amount, auction_amount, auction_lots, min_bid_pct,
                 iss['term'], iss['tcri'], clean.get('guarantee'), lead_mgr, total_award_amt,
                 actual, min_award, max_award,
                 ts))
            inserted += 1
            print(f'  + INSERT {cb} {company}: min={min_award} avg={actual} max={max_award} open={open_date}')
            continue

        if not args.force and existing['actual_price'] and existing['actual_price'] > 0:
            skipped += 1
            continue

        # 不寫 weighted_avg!那欄位命名誤導 (老資料是換算回的股價,新資料是元價)
        # build_html.py 用它當 conv_price 會炸 (158.4 元被當 conv_price 158.4 → premium 660%)
        cur.execute('''UPDATE auctions SET
                       actual_price=?, min_award_pct=?, max_award_pct=?,
                       updated_at=? WHERE cb_code=?''',
                    (actual, min_award, max_award, ts, cb))
        updated += 1
        print(f'  ✓ UPDATE {cb} {raw.get("sec_name") or ""}: min={min_award} avg={actual} max={max_award}')

    conn.commit()
    conn.close()
    print(f'\n=== DONE ===')
    print(f'  新增 (INSERT): {inserted} 筆')
    print(f'  更新 (UPDATE): {updated} 筆')
    print(f'  跳過 (已有): {skipped} 筆')
    print(f'  尚未開標: {no_result} 筆')
    print(f'  TWSE 有但 issued 表無: {not_in_issued} 筆')


if __name__ == '__main__':
    main()
