#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""rescan_missed_cb.py — 補漏掃描:找出「MOPS 有公告但 DB 沒有」的 CB。

## 為什麼要這支 (2026-08-06 用戶問「3260 要發第9次CB為什麼沒更新到」)
`scan_cb_disclosures.py` 全市場掃描時,1879 家 × 4 workers 猛打 MOPS,
**部分公司的查詢會被擋而回【空清單】(不是拋例外)** → 被當成「這家沒公告」靜默跳過,
log 裡零 WARN、統計也看不出來。3260 威剛九 7/28 就公告了卻一直沒進 DB 就是這樣漏的。
(同一個病灶先前也在 doc.twse、櫃買 cbSuspend 出現過 — 台灣官網被擋時多半回空而非報錯。)

## 做法
只掃「已經有 CB 的公司」(比全市場快 3 倍,而且新案多半來自這些老發行人),
逐家查 MOPS、放慢速度 + 空結果重試,把 DB 缺的董事會公告補進來。

## 用法
  py rescan_missed_cb.py --days 90              # 報告缺哪些 (不寫)
  py rescan_missed_cb.py --days 90 --fix        # 補進 DB
  py rescan_missed_cb.py --stock 3260 --fix     # 只補一家
"""
import argparse
import datetime as dt
import sqlite3
import sys
import time
from pathlib import Path

import requests

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
DB_PATH = HERE / 'cb_data.db'
LOG_PATH = HERE / 'rescan_missed.log'

import scan_cb_disclosures as S
import discover_new_cbs as D      # to_iso / PAT_CB_NUM 等共用工具


def log(m):
    line = f'[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {m}'
    print(line, flush=True)
    try:
        with open(LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass


def query_with_retry(sess, code, yms, tries=3):
    """查某家公司的 CB 公告,回 (cb公告清單, 這次查詢是否可信)。

    🔴 關鍵區分 (2026-08-06 修):不能用「CB 公告數 = 0」判斷查詢成敗 —
       589 家裡絕大多數本來就沒有 CB 公告,空是【正常】的。
       舊寫法把它們全標成「未確認」→ 報出「530 家未確認」的假警報,真正被擋的反而看不出來。
       正確做法:看 MOPS 有沒有回【任何】公告 (不限 CB)。
         有公告但沒 CB 相關 → 查詢成功,這家確實沒發 CB ✅
         連一則公告都沒有   → 可疑 (被擋 or 該月真的沒公告) → 重試,仍空才標未確認
    """
    import fetch_mops_milestones as M
    last_raw = 0
    for i in range(1, tries + 1):
        try:
            raw = 0
            for yr, mo in yms:
                raw += len(M.query_mops(sess, code, yr, mo) or [])
                time.sleep(0.35)
            last_raw = raw
            items = S.query_company(sess, code, yms) if raw else []
        except Exception as e:
            raw, items = 0, []
            if i == tries:
                log(f'    [ERR] {code}: {e}')
        if last_raw > 0:
            return items, True          # MOPS 有回東西 → 查詢可信 (不管有沒有 CB 公告)
        if i < tries:
            time.sleep(1.5 * i)
    return [], False                     # 連續三次一則公告都沒有 → 真的未確認


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=90)
    ap.add_argument('--stock', help='只掃單一股票代號')
    ap.add_argument('--fix', action='store_true', help='把缺的補進 DB')
    ap.add_argument('--sleep', type=float, default=0.7, help='每家間隔秒數 (放慢避免被擋)')
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    if args.stock:
        stocks = [(args.stock, '')]
    else:
        stocks = [(r[0], r[1]) for r in conn.execute('''
            SELECT DISTINCT i.stock_code, COALESCE(s.company,'')
            FROM issued i LEFT JOIN stocks s ON s.stock_code=i.stock_code
            WHERE i.stock_code GLOB '[0-9][0-9][0-9][0-9]'
              AND (i.is_legacy IS NULL OR i.is_legacy!=1)
            ORDER BY i.stock_code
        ''')]

    yms = sorted(S.months_back(dt.datetime.now(), args.days))
    cutoff = (dt.date.today() - dt.timedelta(days=args.days)).isoformat()
    log('=' * 60)
    log(f'補漏掃描 · {len(stocks)} 家有 CB 的公司 × {len(yms)} 個月 · 近 {args.days} 天')
    log('=' * 60)

    sess = requests.Session()
    sess.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
    missing, unconfirmed, now = [], 0, dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for n, (code, nm) in enumerate(stocks, 1):
        items, ok = query_with_retry(sess, code, yms)
        if not ok:
            unconfirmed += 1
        for it in items:
            iso = D.to_iso(it['date_roc'])
            if not iso or iso < cutoff:
                continue
            if S.classify(it['title']) != 'board':
                continue
            for cb in S.derive_codes(code, it['title']):
                row = conn.execute('SELECT cb_code, fm_board_decision_date FROM issued WHERE cb_code=?',
                                   (cb,)).fetchone()
                if row and row['fm_board_decision_date']:
                    continue
                missing.append((cb, code, nm, iso, it['title'][:48], bool(row)))
        if n % 50 == 0:
            log(f'  …{n}/{len(stocks)} · 目前發現缺 {len(missing)} 筆')
        time.sleep(args.sleep)

    # 去重 (同一 CB 可能被多則公告命中)
    seen, uniq = set(), []
    for m in missing:
        if m[0] in seen:
            continue
        seen.add(m[0])
        uniq.append(m)

    log('')
    log(f'=== DB 缺少的 CB 董事會公告: {len(uniq)} 筆 (查詢未確認 {unconfirmed} 家) ===')
    for cb, code, nm, iso, title, exists in uniq:
        log(f'  🔴 {cb} {nm[:10]:<11} {iso} · {"補董事會" if exists else "全新案"} · {title}')

    if uniq and args.fix:
        for cb, code, nm, iso, title, exists in uniq:
            if exists:
                conn.execute('''UPDATE issued SET fm_board_decision_date=?, fm_mops_updated_at=?,
                                last_status_update=?, last_status_note=? WHERE cb_code=?''',
                             (iso, now, now, f'董事會決議 {iso}', cb))
            else:
                conn.execute('''INSERT INTO issued
                                (cb_code, stock_code, company, fm_board_decision_date,
                                 fm_mops_updated_at, updated_at, last_status_update, last_status_note)
                                VALUES (?,?,?,?,?,?,?,?)''',
                             (cb, code, nm, iso, now, now, now, f'新案 董事會決議 {iso}'))
        conn.commit()
        log(f'✅ 已補入 DB: {len(uniq)} 筆')
    elif uniq:
        log('(唯讀模式,加 --fix 才會寫入)')
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
