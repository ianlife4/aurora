#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""batch_upgrade_v2.py — 把舊格式 (v1) 分析報告批次重跑成 v2 格式。

v2 格式 (2026-07-30 改版):一眼判讀置頂 + 發債動機五分類 + 產業應用地圖 + 同業對照,
警示訊號降到中段。詳見 auto_analyze_cb.py 的 SYSTEM_PROMPT。

順便一起修的事:重跑時會用 pick_best_cb_prospectus 挑【最新最完整】的說明書
(定價版 B022/B023、生效版 B05),所以同時解決「分析用過期 PDF」的問題。

**可續跑**:已是 v2 的自動跳過 → 中斷後直接再執行即可,不會重複花錢。

用法:
  py batch_upgrade_v2.py --since 2026-01-30            # 近半年 (預設)
  py batch_upgrade_v2.py --since 2026-01-30 --dry-run  # 只列出要跑哪些
  py batch_upgrade_v2.py --since 2026-01-30 --limit 5  # 成本護欄
"""
import argparse
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).parent
DB_PATH = HERE / 'cb_data.db'
LOG_PATH = HERE / 'batch_upgrade_v2.log'
V2_MARKER = '一眼判讀'          # v2 報告一定有這段


def log(msg):
    line = f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] {msg}'
    print(line, flush=True)
    try:
        with open(LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass


def get_targets(since):
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute('''
        SELECT cb_code, company, stock_code, fm_board_decision_date bd, analysis_md
        FROM issued
        WHERE analysis_md IS NOT NULL AND analysis_md != ''
          AND (is_legacy IS NULL OR is_legacy != 1)
          AND (is_withdrawn IS NULL OR is_withdrawn != 1)
          AND fm_board_decision_date >= ?
        ORDER BY fm_board_decision_date DESC
    ''', (since,)).fetchall()
    conn.close()
    return [r for r in rows if V2_MARKER not in (r['analysis_md'] or '')]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--since', default='2026-01-30', help='board 決議日下限 (YYYY-MM-DD)')
    ap.add_argument('--limit', type=int, help='最多跑幾檔 (成本護欄)')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    sys.path.insert(0, str(HERE))
    import fetch_prospectus_pdf as fpp

    targets = get_targets(args.since)
    if args.limit:
        targets = targets[:args.limit]
    log('=' * 60)
    log(f'v1 → v2 批次升級 · since {args.since} · 待處理 {len(targets)} 檔')
    log('=' * 60)
    if args.dry_run:
        for t in targets:
            log(f"  {t['cb_code']} {t['company']} (board {t['bd'][:10]})")
        return 0

    ok = fail = 0
    for i, t in enumerate(targets, 1):
        cb, stock = t['cb_code'], t['stock_code']
        log('')
        log(f"── [{i}/{len(targets)}] {cb} {t['company']} ──")
        # 順便挑最新最完整的說明書版本 (定價版/生效版),一併解決過期 PDF
        best = None
        try:
            pick = fpp.pick_best_cb_prospectus(stock, cb_code=cb,
                                               board_ym=(t['bd'] or '')[:7] or None)
            best = pick['filename'] if pick else None
            if best:
                log(f'   最佳說明書版本: {best}')
        except Exception as e:
            log(f'   [WARN] 挑版本失敗 (讓 auto_analyze 自己抓): {e}')

        conn = sqlite3.connect(str(DB_PATH))
        if best:
            conn.execute('UPDATE issued SET analysis_md=NULL, analysis_updated_at=NULL, '
                         'prospectus_filename=? WHERE cb_code=?', (best, cb))
        else:
            conn.execute('UPDATE issued SET analysis_md=NULL, analysis_updated_at=NULL '
                         'WHERE cb_code=?', (cb,))
        conn.commit()
        conn.close()

        rc = subprocess.run([sys.executable, 'auto_analyze_cb.py', '--cb', cb],
                            cwd=str(HERE)).returncode
        # 驗證真的產出 v2
        conn = sqlite3.connect(str(DB_PATH))
        md = conn.execute('SELECT analysis_md FROM issued WHERE cb_code=?', (cb,)).fetchone()[0]
        conn.close()
        if rc == 0 and md and V2_MARKER in md:
            ok += 1
            log(f'   ✓ v2 完成')
        else:
            fail += 1
            log(f'   ✗ 失敗 (exit={rc}, v2 marker={"有" if md and V2_MARKER in md else "無"})')
        time.sleep(2)

    log('')
    log('=' * 60)
    log(f'批次結束 · 成功 {ok} · 失敗 {fail}')
    log('=' * 60)
    return 0


if __name__ == '__main__':
    sys.exit(main())
