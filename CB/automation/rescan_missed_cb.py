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
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
DB_PATH = HERE / 'cb_data.db'
LOG_PATH = HERE / 'rescan_missed.log'

import scan_cb_disclosures as S
import db as _dbm                  # resolve_new_cb_code:第N次推代號撞號 (2026-10-05 光譜 53814)
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

    2026-08-17:原本這裡自己先 probe 一輪 query_mops 拿 raw,再呼叫 query_company
    【重查一次】同樣的月份 → 每家打 4~6 次 MOPS,589 家跑 3 小時撞逾時。
    改成讓 query_company 用 with_raw 一趟回傳兩者 (raw 判可信、items 拿結果)。

    2026-10-06:「raw > 0」本身也判錯 — MOPS 對沒公告的月份會明確回「資料庫中查無需求資料」,
    被擋則回 HTTP 200 的「Overrun - 查詢過於頻繁」,兩者 raw 都是 0。10 月初大半公司當月沒公告 →
    每家白重試 3 輪,本支從每家 3.6 秒拖到 14 秒,連三天撞 2700s 逾時。改由 mops_client 逐月判讀,
    「可信」= 每個月都拿到明確答覆 (有公告/查無/不繼續公開發行)。見 mops_client.py 檔頭。
    """
    try:
        items, unconf = S.query_company(sess, code, yms, tries=tries, with_status=True)
    except Exception as e:
        log(f'    [ERR] {code}: {e}')
        return [], False
    return items, not unconf


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=90)
    ap.add_argument('--stock', help='只掃單一股票代號')
    ap.add_argument('--fix', action='store_true', help='把缺的補進 DB')
    # 2026-10-06:預設 0.7 → 0 — 放慢改由 mops_client 全域節流閥統一控制 (≈1.2 次/秒、被擋自動冷卻),
    #   這裡再睡只是白耗時間 (607 家 × 0.7 = 7 分鐘)。
    ap.add_argument('--sleep', type=float, default=0.0, help='每家額外間隔秒數 (MOPS 節流已由 mops_client 控制)')
    # 缺的案要等全部掃完才寫 DB → 被 cron_pulse 2700s 逾時砍掉 = 已找到的全丟。超過期限就停,剩下的列未確認。
    ap.add_argument('--deadline', type=int, default=2400, help='最多跑幾秒 (預設 2400,cron_pulse 逾時 2700)')
    # 2026-10-06 單線程實跑 2181s (兩個冷月份):串行 = 等節流間隔 + 等 MOPS 回應,實際只有 0.54 次/秒,
    #   節流閥 1.2 次/秒的額度用不到一半。2 個 worker 共用同一個節流閥 → 總速率仍 ≤1.2 次/秒,只是不浪費等待時間。
    ap.add_argument('--workers', type=int, default=2, help='平行查詢數 (MOPS 總速率由 mops_client 節流閥統一控制)')
    ap.add_argument('--limit', type=int, help='只掃前 N 家 (測速用)')
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
    if args.limit:
        stocks = stocks[:args.limit]

    yms = sorted(S.months_back(dt.datetime.now(), args.days))
    cutoff = (dt.date.today() - dt.timedelta(days=args.days)).isoformat()
    log('=' * 60)
    log(f'補漏掃描 · {len(stocks)} 家有 CB 的公司 × {len(yms)} 個月 · 近 {args.days} 天')
    log('=' * 60)

    missing, unconfirmed, now = [], [], dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    t0 = time.time()

    # worker 只做網路查詢 (各自一個 Session);DB 比對/resolve_new_cb_code 留在主線程 (sqlite 連線不跨線程)。
    _tls = threading.local()

    def _sess():
        s = getattr(_tls, 's', None)
        if s is None:
            s = _tls.s = requests.Session()
            s.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        return s

    def job(code):
        if time.time() - t0 > args.deadline:
            return [], False, True          # 過期限 → 不發查詢,標未確認
        if args.sleep:
            time.sleep(args.sleep)
        items, ok = query_with_retry(_sess(), code, yms)
        return items, ok, False

    n_dl = 0
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futures = {ex.submit(job, code): (code, nm) for code, nm in stocks}
        for n, fut in enumerate(as_completed(futures), 1):
            code, nm = futures[fut]
            try:
                items, ok, skipped = fut.result()
            except Exception as e:
                log(f'    [ERR] {code}: {e}')
                items, ok, skipped = [], False, False
            if skipped:
                n_dl += 1
            if not ok:
                unconfirmed.append(code)
            for it in items:
                iso = D.to_iso(it['date_roc'])
                if not iso or iso < cutoff:
                    continue
                if S.classify(it['title']) != 'board':
                    continue
                for cb in S.derive_codes(code, it['title']):
                    # 「股票+第N次」撞到同公司更早的舊債 → 改指在途案/下一個流水號 (2026-10-05 光譜 53814 vs 合正三 53813)
                    cb = _dbm.resolve_new_cb_code(conn, code, cb, iso, 'rescan 董事會')
                    row = conn.execute('SELECT cb_code, fm_board_decision_date FROM issued WHERE cb_code=?',
                                       (cb,)).fetchone()
                    if row and row['fm_board_decision_date']:
                        continue
                    missing.append((cb, code, nm, iso, it['title'][:48], bool(row)))
            if n % 50 == 0:
                log(f'  …{n}/{len(stocks)} · 目前發現缺 {len(missing)} 筆 ({time.time() - t0:.0f}s)')
    if n_dl:
        log(f'  ⚠ 超過 {args.deadline}s 上限 → {n_dl} 家沒查,列入未確認 (已找到的照常寫入)')
    unconfirmed.sort()

    # 去重 (同一 CB 可能被多則公告命中)
    seen, uniq = set(), []
    for m in missing:
        if m[0] in seen:
            continue
        seen.add(m[0])
        uniq.append(m)

    log('')
    log(f'MOPS 回應:{S.MC.DEFAULT.summary()} · 快取命中 {S.MC.CACHE.hits} 次 · 耗時 {time.time() - t0:.0f}s')
    if unconfirmed:
        log(f'⚠ 查詢未確認 (重試後 MOPS 仍沒明確答覆): {", ".join(unconfirmed[:40])}')
    log(f'=== DB 缺少的 CB 董事會公告: {len(uniq)} 筆 (查詢未確認 {len(unconfirmed)} 家) ===')
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
