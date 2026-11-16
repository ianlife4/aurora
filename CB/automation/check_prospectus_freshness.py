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
3. 兩者不同 (或分析當時用的轉換價已被更正) → STALE。`--fix` 用 auto_analyze_cb.py --cb X --force 重跑,
   成功才換掉舊分析;失敗就留著舊的,明天再試

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


def _conv_stale(r):
    """分析當時用的 DB 轉換價 (issued.analysis_conv_price,auto_analyze_cb 寫) 跟現值差超過 0.5%,
    而且現值已經是權威來源確認過的 → 報告裡的轉換價/稀釋率/隱含成本都是舊的,要重跑。
    現值還是券商檔暫定值 (db.conv_unconfirmed) 時先不算 — 它還會變,等定案再重跑一次就好。
    (2026-11-15:聯亞一 1665→1757.9、巨漢二 406.6→369.8、晟田六 68.8→62.8、欣大健康 45→54.6 更正後,
     分析報告還寫著舊值是「DB 定案」。)"""
    try:
        used, now = r['analysis_conv_price'], r['conv_price']
        if not used or not now or abs(float(now) / float(used) - 1) <= 0.005:
            return False
        import db as _dbm
        return not _dbm.conv_unconfirmed(r['note'], r['fm_conv_price_set_date'], now)
    except Exception:
        return False


def get_rows(conn, cb_only, scan_all):
    if 'analysis_conv_price' not in {x[1] for x in conn.execute('PRAGMA table_info(issued)')}:
        conn.execute('ALTER TABLE issued ADD COLUMN analysis_conv_price REAL')
        conn.commit()
    cols = ('cb_code, stock_code, company, fm_board_decision_date, listing_date, '
            'prospectus_filename, analysis_md, analysis_updated_at, '
            'conv_price, analysis_conv_price, note, fm_conv_price_set_date')
    if cb_only:
        return conn.execute(f'SELECT {cols} FROM issued WHERE cb_code=?', (cb_only,)).fetchall()
    base = f'''SELECT {cols} FROM issued
        WHERE analysis_md IS NOT NULL AND analysis_md != ''
          AND (is_legacy IS NULL OR is_legacy != 1)
          AND (is_withdrawn IS NULL OR is_withdrawn != 1)'''
    if not scan_all:
        # 只顧「還有用的」:未掛牌 / 未定 / 近 90 天內掛牌 (老案的說明書不會再變);
        # 轉換價被更正過的 (跟分析當時用的值不同) 不管多舊都要看
        base += '''
          AND (listing_date IS NULL OR listing_date='' OR listing_date='未定'
               OR substr(listing_date,1,10) >= date('now','-90 days')
               OR (analysis_conv_price > 0 AND conv_price > 0
                   AND abs(conv_price / analysis_conv_price - 1) > 0.005))'''
    return conn.execute(base + ' ORDER BY cb_code').fetchall()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--fix', action='store_true', help='發現過期就重跑 (成功才換掉舊分析)')
    ap.add_argument('--all', action='store_true', help='掃所有有分析的 CB (慢)')
    ap.add_argument('--cb', type=str, help='只檢查指定 cb_code')
    ap.add_argument('--limit', type=int, help='--fix 時最多重跑幾檔 (成本護欄)')
    args = ap.parse_args()

    sys.path.insert(0, str(HERE))
    import fetch_prospectus_pdf as fpp

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
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
        conv_stale = _conv_stale(r)
        if used == best and not conv_stale:
            ok += 1
            continue
        stale.append({'cb': cb, 'company': r['company'], 'used': used, 'best': best,
                      'note': (pick.get('note') or '')[:40],
                      'upload': pick.get('upload_date', ''),
                      'conv': (r['analysis_conv_price'], r['conv_price']) if conv_stale else None})

    log('')
    log(f'=== 結果: 最新 {ok} · 過期 {len(stale)} · 無法判定 {unknown} ===')
    for s in stale:
        if s['used'] != s['best']:
            log(f'  🔴 {s["cb"]} {s["company"][:10]:<11} 用了 {s["used"] or "(未記錄)"}')
            log(f'      → 應改用 {s["best"]}  [{s["note"]}] 上傳 {s["upload"]}')
        if s.get('conv'):
            log(f'  🔴 {s["cb"]} {s["company"][:10]:<11} 分析當時的轉換價 {s["conv"][0]:g},DB 現在是 {s["conv"][1]:g} → 重跑')

    if not stale or not args.fix:
        if stale:
            log('')
            log('(唯讀模式。加 --fix 會重跑,成功才換掉舊分析)')
        conn.close()
        return 0

    todo = stale[:args.limit] if args.limit else stale
    log('')
    log(f'--fix: 重跑 {len(todo)} 檔…')
    done = 0
    for s in todo:
        log(f'  ── {s["cb"]} {s["company"]} 重跑中 (auto_analyze --force,成功才換掉舊分析) ──')
        # 2026-11-15:以前是「先清掉 analysis_md 再重跑」— 重跑失敗 (API 錯誤、PDF 抓不到) 分析就整份消失,
        #   掛牌超過 30 天的案 auto_analyze 預設名單又不會再撿 → 永遠沒有分析。轉換價更正的重跑會碰到舊案,
        #   改成用 --force 覆寫:失敗就留著舊分析、把版本記錄還原,明天再試。
        _old = conn.execute('SELECT prospectus_filename, analysis_updated_at FROM issued WHERE cb_code=?',
                            (s['cb'],)).fetchone()
        # 2026-11-16:新版檔名用參數帶給 auto_analyze,不先寫 DB — 以前先寫再跑,這 5~25 分鐘內被砍
        #   (關機/睡眠/拔電源) 就留下「版本是新的、分析是舊的」,隔天判成最新、不再重跑。
        #   prospectus_filename 只由 auto_analyze 在分析進 DB 之後寫。
        rc = subprocess.run([sys.executable, 'auto_analyze_cb.py', '--cb', s['cb'], '--force',
                             '--prospectus', s['best']],
                            cwd=str(HERE)).returncode
        _new = conn.execute('SELECT prospectus_filename, analysis_updated_at FROM issued WHERE cb_code=?',
                            (s['cb'],)).fetchone()
        _ok = rc == 0 and _new and _old and _new['analysis_updated_at'] != _old['analysis_updated_at']
        if not _ok and _new and _old and _new['prospectus_filename'] != _old['prospectus_filename']:
            # 保險:分析沒換、版本記錄卻被動到 → 還原 (auto_analyze 現在只在成功後才寫,正常不會進來)
            try:
                conn.execute('UPDATE issued SET prospectus_filename=? WHERE cb_code=?',
                             (_old['prospectus_filename'], s['cb']))
                conn.commit()
            except sqlite3.Error as e:
                log(f'     [WARN] 還原版本記錄失敗: {e}')
        log(f'     exit={rc} {"✓ 已換新分析" if _ok else "✗ 沒成功 — 舊分析保留,下次再試"}')
        done += bool(_ok)
    conn.close()
    log(f'完成 {done}/{len(todo)}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
