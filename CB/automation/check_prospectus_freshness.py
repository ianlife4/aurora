#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""check_prospectus_freshness.py — 找出「分析用的公開說明書已過期」的 CB,必要時自動重跑。

## 為什麼要這支 (2026-07-27 辛耘三 35833 血案)
分析報告只做一次,做完就 `已有 analysis_md → SKIP` 永遠不再更新。但公開說明書會**一直長新版**:
    B021 申報稿本 (最早,轉換價只有預估)
      → B022/B023 定價版 (訂價後,含【定案轉換價】;同次申報多檔會分 CB1/CB2/CB3 版)
        → B05 生效版 (最終)
35833 辛耘三 的分析是 2026-05-20 產的,當時只有 202405 那份 (而且還是 2024 年【上一檔】CB 的),
之後 doc.twse 陸續上了 202606_B021 (第三+四次) 和 202607_B022 (CB3定價版) —— 全都沒補進來,
報告裡還寫著「最新公開說明書尚未取得」。

## 為什麼不「排固定時間全部重爬重分析」
每份分析要 ~200 秒 + Claude API 成本;全部盲目重跑又慢又貴。
但**列出某公司的說明書清單只是一個 HTTP 請求、零 API 成本**。
→ 所以策略是:**便宜地比對版本,只有真的出新版才重跑分析**。

## 做法
1. DB `issued.prospectus_filename` 記錄「這份分析用了哪個版本」(auto_analyze_cb.py 分析完會寫)
2. 本支對每檔打 doc.twse 清單,用 `pick_best_cb_prospectus` 算出「現在最好的版本」
3. 兩者不同 → STALE。`--fix` 會清掉 analysis_md 並重跑 auto_analyze_cb.py --cb X

## 用法
  py check_prospectus_freshness.py                # 只報告 (預設掃在途 + 近 90 天掛牌)
  py check_prospectus_freshness.py --fix          # 報告 + 自動重跑過期的
  py check_prospectus_freshness.py --fix --limit 3
  py check_prospectus_freshness.py --all          # 掃所有有分析的 (慢,會打很多次 doc.twse)
  py check_prospectus_freshness.py --cb 35833     # 只檢查一檔
"""
import argparse
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).parent
DB_PATH = HERE / 'cb_data.db'
LOG_PATH = HERE / 'prospectus_freshness.log'


def log(msg):
    line = f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] {msg}'
    print(line, flush=True)
    try:
        with open(LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass


def _norm(name):
    """'202607_3583_B022_20260727_143133.pdf' → '202607_3583_B022.pdf' (去下載時間戳)"""
    if not name:
        return None
    m = re.match(r'(\d{6}_\d+_B\w+?)(?:_\d{8}_\d{6})?\.pdf$', name.strip())
    return (m.group(1) + '.pdf') if m else name.strip()


def _used_from_analysis(md):
    """分析報告開頭有『📄 來源:[202405_3583_B021_20260520_143133.pdf]』→ 回推用過的版本。
    (給 prospectus_filename 還沒記錄的舊分析做 backfill)"""
    if not md:
        return None
    m = re.search(r'(\d{6}_\d+_B\w+?(?:_\d{8}_\d{6})?\.pdf)', md[:1500])
    return _norm(m.group(1)) if m else None


def get_rows(conn, cb_only, scan_all):
    cols = ('cb_code, stock_code, company, fm_board_decision_date, listing_date, '
            'prospectus_filename, analysis_md, analysis_updated_at')
    if cb_only:
        return conn.execute(f'SELECT {cols} FROM issued WHERE cb_code=?', (cb_only,)).fetchall()
    base = f'''SELECT {cols} FROM issued
        WHERE analysis_md IS NOT NULL AND analysis_md != ''
          AND (is_legacy IS NULL OR is_legacy != 1)
          AND (is_withdrawn IS NULL OR is_withdrawn != 1)'''
    if not scan_all:
        # 只顧「還有用的」:未掛牌 / 未定 / 近 90 天內掛牌 (老案的說明書不會再變)
        base += '''
          AND (listing_date IS NULL OR listing_date='' OR listing_date='未定'
               OR substr(listing_date,1,10) >= date('now','-90 days'))'''
    return conn.execute(base + ' ORDER BY cb_code').fetchall()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--fix', action='store_true', help='發現過期就清掉分析並重跑')
    ap.add_argument('--all', action='store_true', help='掃所有有分析的 CB (慢)')
    ap.add_argument('--cb', type=str, help='只檢查指定 cb_code')
    ap.add_argument('--limit', type=int, help='--fix 時最多重跑幾檔 (成本護欄)')
    args = ap.parse_args()

    sys.path.insert(0, str(HERE))
    import fetch_prospectus_pdf as fpp

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = get_rows(conn, args.cb, args.all)
    log(f'檢查 {len(rows)} 檔 CB 的公開說明書版本…')

    stale, ok, unknown = [], 0, 0
    cache = {}
    for r in rows:
        cb, stock = r['cb_code'], r['stock_code']
        if not stock:
            continue
        used = _norm(r['prospectus_filename']) or _used_from_analysis(r['analysis_md'])
        board_ym = (r['fm_board_decision_date'] or '')[:7]
        try:
            if stock not in cache:
                # doc.twse 連打會擋 (回空清單、不報錯) → 重試 + 退避;空清單當失敗不當「沒文件」
                got = []
                for attempt in range(1, 4):
                    got = fpp.list_prospectuses(stock) or []
                    if got:
                        break
                    log(f'    {stock} 清單抓到空 (第 {attempt} 次) — 可能被 doc.twse 擋,等 {attempt*5}s')
                    time.sleep(attempt * 5)
                cache[stock] = got
                time.sleep(1.2)      # 對 doc.twse 客氣點 (0.3s 太快會被擋)
            if not cache[stock]:
                log(f'  [WARN] {cb} {r["company"]}: 抓不到說明書清單')
                unknown += 1
                continue
            pick = fpp.pick_best_cb_prospectus(stock, cb_code=cb, board_ym=board_ym or None,
                                               items=cache[stock])
        except Exception as e:
            log(f'  [WARN] {cb} {r["company"]}: 查清單失敗 {e}')
            unknown += 1
            continue
        best = _norm(pick['filename']) if pick else None
        if not best:
            unknown += 1
            continue
        if used == best:
            ok += 1
            continue
        stale.append({'cb': cb, 'company': r['company'], 'used': used, 'best': best,
                      'note': (pick.get('note') or '')[:40],
                      'upload': pick.get('upload_date', '')})

    log('')
    log(f'=== 結果: 最新 {ok} · 過期 {len(stale)} · 無法判定 {unknown} ===')
    for s in stale:
        log(f'  🔴 {s["cb"]} {s["company"][:10]:<11} 用了 {s["used"] or "(未記錄)"}')
        log(f'      → 應改用 {s["best"]}  [{s["note"]}] 上傳 {s["upload"]}')

    if not stale or not args.fix:
        if stale:
            log('')
            log('(唯讀模式。加 --fix 會清掉舊分析並重跑)')
        conn.close()
        return 0

    todo = stale[:args.limit] if args.limit else stale
    log('')
    log(f'--fix: 重跑 {len(todo)} 檔…')
    done = 0
    for s in todo:
        log(f'  ── {s["cb"]} {s["company"]} 重跑中 (清舊分析 → auto_analyze) ──')
        conn.execute('UPDATE issued SET analysis_md=NULL, analysis_updated_at=NULL, '
                     'prospectus_filename=? WHERE cb_code=?', (s['best'], s['cb']))
        conn.commit()
        rc = subprocess.run([sys.executable, 'auto_analyze_cb.py', '--cb', s['cb']],
                            cwd=str(HERE)).returncode
        log(f'     exit={rc}')
        done += (rc == 0)
    conn.close()
    log(f'完成 {done}/{len(todo)}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
