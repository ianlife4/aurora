#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""scan_prospectus_avail.py — 掃「還沒下載公開說明書」的 CB,標記哪些 TWSE 已經有得抓。

## 為什麼要這支 (2026-08-03 用戶要求)
儀表板要有一個「待下載說明書」清單讓用戶勾選下載。但光列「沒有 analysis_md 的 CB」不夠用 —
**多數是 TWSE 根本還沒上稿**(董事會剛過、B021 要等一陣子),點下去只會白等 3 分鐘然後 SKIP。
→ 本支先便宜地打 doc.twse 清單頁 (純 HTTP、零 API 成本),把「真的有得抓」的標出來,
   前端就能分成【可下載】vs【TWSE 尚未上稿】兩區,不會浪費你的時間和 API 費用。

判定用 `fetch_prospectus_pdf.pick_best_cb_prospectus()`,跟 auto_analyze_cb 實際會抓的是同一支,
所以「顯示可下載」= 真的按下去會成功 (它已含 board±6月 的同案過濾)。

## 用法
  py scan_prospectus_avail.py                 # 掃近 9 個月無分析的
  py scan_prospectus_avail.py --days 180
  py scan_prospectus_avail.py --stale-hours 12  # 只重掃超過 12 小時沒檢查的 (預設 24)
  py scan_prospectus_avail.py --dry-run
"""
import argparse
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).parent
DB_PATH = HERE / 'cb_data.db'
LOG_PATH = HERE / 'prospectus_avail.log'


def log(msg):
    line = f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] {msg}'
    print(line, flush=True)
    try:
        with open(LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=270, help='只掃董事會近 N 天內的案 (預設 270)')
    ap.add_argument('--stale-hours', type=int, default=24, help='幾小時內檢查過就跳過 (預設 24)')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    sys.path.insert(0, str(HERE))
    import fetch_prospectus_pdf as fpp

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute('''
        SELECT cb_code, company, stock_code, fm_board_decision_date bd, prospectus_checked_at
        FROM issued
        WHERE (analysis_md IS NULL OR analysis_md = '')
          AND (is_legacy IS NULL OR is_legacy != 1)
          AND (is_withdrawn IS NULL OR is_withdrawn != 1)
          AND stock_code IS NOT NULL AND stock_code != ''
          AND fm_board_decision_date >= date('now', ?)
        ORDER BY fm_board_decision_date DESC
    ''', (f'-{args.days} days',)).fetchall()

    cutoff = (datetime.now() - timedelta(hours=args.stale_hours)).strftime('%Y-%m-%d %H:%M:%S')
    todo = [r for r in rows if not (r['prospectus_checked_at'] or '') or r['prospectus_checked_at'] < cutoff]
    log(f'無分析的 CB {len(rows)} 檔,其中需要(重)檢查 {len(todo)} 檔')

    cache, avail, none_yet, failed = {}, 0, 0, 0
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for i, r in enumerate(todo, 1):
        cb, stock = r['cb_code'], str(r['stock_code'])
        try:
            if stock not in cache:
                # doc.twse 連打會被擋且【回空清單不報錯】→ 重試退避 (同 check_prospectus_freshness)
                got = []
                for attempt in range(1, 4):
                    got = fpp.list_prospectuses(stock) or []
                    if got:
                        break
                    time.sleep(attempt * 4)
                cache[stock] = got
                time.sleep(1.2)
            if not cache[stock]:
                log(f'  [WARN] {cb} {r["company"]}: 抓不到清單 (不動,下次再試)')
                failed += 1
                continue
            pick = fpp.pick_best_cb_prospectus(stock, cb_code=cb,
                                               board_ym=(r['bd'] or '')[:7] or None,
                                               items=cache[stock])
        except Exception as e:
            log(f'  [WARN] {cb} {r["company"]}: {e}')
            failed += 1
            continue

        if pick:
            avail += 1
            vals = (pick['filename'], pick.get('upload_date'), (pick.get('note') or '')[:60], now, cb)
            log(f'  ✅ {cb} {r["company"][:10]:<11} 可下載 ← {pick["filename"]} [{(pick.get("note") or "")[:22]}]')
        else:
            none_yet += 1
            vals = (None, None, None, now, cb)
        if not args.dry_run:
            conn.execute('''UPDATE issued SET prospectus_avail=?, prospectus_avail_date=?,
                            prospectus_avail_note=?, prospectus_checked_at=? WHERE cb_code=?''', vals)
            if i % 10 == 0:
                conn.commit()

    if not args.dry_run:
        conn.commit()
    total_avail = conn.execute("SELECT COUNT(*) FROM issued WHERE (analysis_md IS NULL OR analysis_md='') "
                               "AND prospectus_avail IS NOT NULL").fetchone()[0]
    conn.close()
    log('')
    log(f'=== 本次檢查 {len(todo)} 檔:可下載 {avail} · TWSE尚未上稿 {none_yet} · 查詢失敗 {failed} ===')
    log(f'=== 全庫「無分析且可下載」累計: {total_avail} 檔 ===')
    return 0


if __name__ == '__main__':
    sys.exit(main())
