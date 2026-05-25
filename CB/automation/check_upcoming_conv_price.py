#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
即將開標 CB 缺 conv_price 預警 monitor

每天 mops_daily 結束跑,掃「投標日 ≤ today+5 + conv_price=None + 非 legacy」CB:
  - 有缺就 TG alert (附 CB 列表 + 來源建議)
  - 修補 conv_price 缺料是「沒競拍建議價」的根因 (computeBidSuggestion 需要 theory_price = stock_close/conv_price*100)

退出碼:
  0 = 沒缺 / 或有缺已 TG (運行 OK)
  非 0 = monitor 本身爆掉
"""
import sqlite3
import sys
import io
from datetime import datetime, timedelta
from pathlib import Path

# Windows cp950 → utf-8
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

HERE = Path(__file__).parent
DB = HERE / 'cb_data.db'

try:
    sys.path.insert(0, str(HERE))
    from notify_tg import send_tg
except ImportError:
    def send_tg(text, **kw):
        print(f'[TG mock] {text}')
        return False


def scan_missing(window_days: int = 5) -> list[dict]:
    """掃即將開標但缺 conv_price 的 CB。
    判定條件: 任一為真 ⇒「即將開標」:
      a) upcoming_auctions 表有 bid_start ≤ today+N 的 row
      b) issued.listing_date ≤ today+N (掛牌日近)
      c) issued.fm_bid_end_date >= today (投標中)
    AND issued.conv_price IS NULL OR ≤ 0
    AND (is_legacy IS NULL OR != 1)
    AND (is_withdrawn IS NULL OR != 1)
    """
    today = datetime.now().date()
    horizon = (today + timedelta(days=window_days)).isoformat()

    con = sqlite3.connect(str(DB))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    rows = cur.execute(f'''
        SELECT DISTINCT i.cb_code, i.company, i.stock_code, i.method,
               i.eff_date, i.listing_date, i.fm_bid_start_date, i.fm_bid_end_date,
               u.bid_start AS u_bid_start, u.bid_end AS u_bid_end,
               u.auction_date AS u_auction_date
        FROM issued i
        LEFT JOIN upcoming_auctions u ON u.cb_code = i.cb_code AND u.is_cancelled = 0
        WHERE (i.conv_price IS NULL OR i.conv_price <= 0)
          AND (i.is_legacy IS NULL OR i.is_legacy != 1)
          AND (i.is_withdrawn IS NULL OR i.is_withdrawn != 1)
          AND (
            (u.bid_start IS NOT NULL AND u.bid_start <= ?)
            OR (i.fm_bid_start_date IS NOT NULL AND i.fm_bid_start_date <= ?)
            OR (i.listing_date != '' AND i.listing_date != '未定'
                AND substr(i.listing_date,1,10) <= ?
                AND substr(i.listing_date,1,10) >= ?)
          )
        ORDER BY COALESCE(u.bid_start, i.fm_bid_start_date, i.listing_date)
    ''', (horizon, horizon, horizon, today.isoformat())).fetchall()

    con.close()
    return [dict(r) for r in rows]


def main(window_days: int = 5, dry_run: bool = False) -> int:
    missing = scan_missing(window_days)
    today_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    if not missing:
        print(f'[{today_str}] ✅ 即將開標 (within {window_days} 天) 全部有 conv_price')
        return 0

    print(f'[{today_str}] ⚠️  {len(missing)} 筆即將開標但缺 conv_price:')
    lines = []
    for r in missing:
        bid = r['u_bid_start'] or r['fm_bid_start_date'] or '?'
        lst = (r['listing_date'] or '')[:10] or '未定'
        line = f"  {r['cb_code']:7s} {r['company'] or '?':10s}  bid={bid}  list={lst}  method={r['method'] or '?'}"
        print(line)
        lines.append(line)

    # TG 通知
    msg_lines = [
        f'⚠️ <b>CB 即將開標但缺轉換價</b>  ({len(missing)} 筆,window {window_days}d)',
        '',
        '<pre>',
        *[f"{r['cb_code']:7s} {r['company'] or '?':10s} bid={r['u_bid_start'] or r['fm_bid_start_date'] or '?'} list={(r['listing_date'] or '')[:10] or '未定'}"
          for r in missing],
        '</pre>',
        '',
        '⚙️ 對策:',
        '1) 元富/統一證 mail 收到後重跑 self_update',
        '2) 等 MOPS「訂定轉換價格」公告 (生效後 7-14 天上)',
        '3) 手動 SQL UPDATE conv_price (用截圖數值)',
        '',
        f'🔗 web: https://stock-dash.ian-4k.workers.dev/',
    ]
    msg = '\n'.join(msg_lines)

    if not dry_run:
        ok = send_tg(msg, parse_mode='HTML')
        print(f'\n  TG 通知: {"OK" if ok else "FAIL (no token?)"}')
    else:
        print('\n--- TG msg (dry-run) ---')
        print(msg)
    return 0


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--days', type=int, default=5, help='開標日 N 天內視為「即將」')
    p.add_argument('--dry-run', action='store_true', help='只印不寄 TG')
    args = p.parse_args()
    sys.exit(main(args.days, args.dry_run))
