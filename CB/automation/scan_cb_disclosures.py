#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""每天掃 MOPS 全市場 CB 公開資訊,偵測新里程碑並標記「狀態更新」。

**2026-06-18 改版**:原版用 `ajax_t05sr01_1?keyword=` 全市場關鍵字搜尋,
MOPS API 2026 年 5/22 後失效(回 3 筆雜公告而非 keyword 過濾結果)。
改為「逐家公司」用 `ajax_t05st01`(per-company endpoint, 仍可用)
平行掃描,雖然慢一倍但可靠。

分類路由(與舊版同):
  - 「董事會決議發行...轉換公司債」→ INSERT 新案 / 補 fm_board_decision_date
  - 「確定專戶/代收價款」→ 補 fm_account_setup_date
  - 「訂定轉換價格」→ 從 detail body 解析,補 conv_price

偵測到「新資訊」(欄位從空→有 或 新案) 才設 last_status_update + last_status_note
→ HTML 已發行列表把近期更新的浮到頂端 + 🆕 badge。

**2026-10-06 改版 (掃描 2402s → 6092s → 撞 7200s 逾時)**:MOPS 查詢改走 `mops_client`
(回應判讀 + 全域節流 ≈1.2 次/秒 + 被擋全體冷卻 + 已結束月份快取),月份改成精確涵蓋 --days,
沒拿到明確答覆的月份最後再補查一輪,仍失敗的印「MOPS 查詢未確認 N 家」給 audit_cb_coverage 告警。
根因與實測數據見 mops_client.py 檔頭。

執行: py -3.12 scan_cb_disclosures.py [--days 30] [--dry-run]
                                      [--only-unknown] (只掃 issued 表沒的股票)
                                      [--workers 4] [--limit N] (只掃前 N 家,測速用)
                                      [--no-cache] (舊月份也查即時,不用 mops_cache.db)
"""
import argparse
import io
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import requests

import db as _dbm                  # resolve_new_cb_code:第N次推代號撞號 (2026-10-05 光譜 53814 vs 2008 合正三 53813)
import discover_new_cbs as D
import fetch_mops_milestones as M
import fetch_mops_conv_price as P
import board_attrs as B
import mops_client as MC           # 2026-10-06:MOPS 回應判讀 + 全域節流 + 舊月份快取 (掃描撞逾時的根治)

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

HERE = Path(__file__).parent
DB_PATH = HERE / 'cb_data.db'


def ensure_cols(conn):
    cols = {r[1] for r in conn.execute('PRAGMA table_info(issued)').fetchall()}
    for col in ('last_status_update', 'last_status_note'):
        if col not in cols:
            conn.execute(f'ALTER TABLE issued ADD COLUMN {col} TEXT')
    conn.commit()


def parse_seqs(title):
    seqs = []
    for m in D.PAT_CB_NUM.finditer(title):
        raw = m.group(1)
        n = int(raw) if raw.isdigit() else D.ZH_NUM.get(raw)
        if n and 1 <= n <= 10 and n not in seqs:
            seqs.append(n)
    return seqs


def derive_codes(stock, title):
    if not (stock and stock.isdigit() and len(stock) == 4):
        return []
    return [f'{stock}{n}' for n in parse_seqs(title)]


def classify(title):
    if P.PAT_CONV_PRICE_TITLE.search(title):
        return 'convprice'
    if M.PAT_BOARD_EXCLUDE.search(title):
        return None
    if D.PAT_BOARD.search(title):
        return 'board'
    if M.PAT_ACCOUNT.search(title):
        return 'account'
    return None


# ── 全市場 per-company sweep ──────────────────────────────────────

# CB 相關公告的 fast-pre-filter (省 classify 開銷)
CB_TITLE_KEYS = re.compile(
    r'轉換公司債|可轉換|存儲|代收價款|代收股款|代收款|專戶|訂定轉換|轉換價格.*?(?:溢價率|及.*?率|訂定)'
)


def query_company(session, co_id, ym_list, tries=4, with_status=False, throttle=None, cache=True):
    """對單家公司,跨 N 個月查 MOPS,回傳所有 CB 相關公告。
    with_status=True → 回 (CB公告清單, 未確認月份 [(民國年, 月, 狀態)…]);未確認 = 重試後仍沒拿到明確答覆。

    🔴 MOPS 被擋時【回空清單而不是拋例外】(HTTP 200 但無資料),舊版直接當成「這家沒公告」
       靜默跳過 → 3260 威剛九 7/28 就公告,全市場掃描卻連續多天沒抓到 (2026-08-06 用戶發現)。

    🔴 重試的觸發條件必須是【MOPS 有沒有回答】,不是【有沒有我要的東西】:
       2026-08-06 初版 `if items` (CB 公告數==0 就重試) → 每家白重試 3 次 → 撞 3600s 逾時、停擺 12 天 (08-17)。
       08-17 改成 `raw == 0` (原始公告數) 仍是同一個錯的變體 — 2026-10-06 實測:
         「資料庫中查無需求資料」(這個月真的沒公告,MOPS 有回答) 和「Overrun - 查詢過於頻繁」(被擋,
         也是 HTTP 200) 在 raw 上都是 0。10-01 換成「9+10 月」後大半公司兩個月都沒公告 → 每家白重試
         3 輪,重試又只等 1~2 秒、4 workers 繼續猛打 → 越打越被擋:2482s → 4794s → 6092s → 撞 7200s。
         還有反方向的洞:某月有公告、另一月被擋 → raw>0 不重試 → 被擋那個月的公告【靜默丟失】。
       → 改由 mops_client 逐月判讀:ok / 查無 / 不繼續公開發行 = 明確答覆;Overrun / 逾時 / 認不得的頁 = 重試,
         被擋時全部 worker 一起冷卻 (節流閥),不再各自猛打。
    """
    out, unconfirmed = [], []
    for yr_roc, mo in ym_list:
        items, st = MC.fetch_month_retry(session, co_id, yr_roc, mo, tries=tries,
                                         throttle=throttle, cache=cache)
        if st not in MC.FINAL:
            unconfirmed.append((yr_roc, mo, st))
        for it in items:
            if CB_TITLE_KEYS.search(it.get('title', '')):
                out.append({
                    'code': co_id, 'name': None,  # name 之後補
                    'date_roc': it.get('date', ''),
                    'time': it.get('time', ''),
                    'title': re.sub(r'\s+', ' ', it.get('title', '')),
                })
    return (out, unconfirmed) if with_status else out


def get_stock_list(conn, only_unknown=False):
    """回傳要掃的 (stock_code, company)。
    only_unknown=True → 只掃 issued 表沒有的股票(catch 全新 CB 發行人)。
    only_unknown=False → 全市場 1878 檔都掃(慢但更完整)。"""
    if only_unknown:
        rows = conn.execute('''
            SELECT s.stock_code, s.company FROM stocks s
            LEFT JOIN (SELECT DISTINCT stock_code FROM issued WHERE (is_legacy IS NULL OR is_legacy != 1)) i
              ON i.stock_code = s.stock_code
            WHERE i.stock_code IS NULL
              AND s.stock_code GLOB '[0-9][0-9][0-9][0-9]'
            ORDER BY s.stock_code
        ''').fetchall()
    else:
        rows = conn.execute('''
            SELECT stock_code, company FROM stocks
            WHERE stock_code GLOB '[0-9][0-9][0-9][0-9]'
            ORDER BY stock_code
        ''').fetchall()
    return [(r[0], r[1]) for r in rows]


def months_back(today, days):
    """回傳精確涵蓋 [today - days, today] 的 (民國年, 月) list (由舊到新)。

    2026-10-06 改:舊版「至少包 2 個月 buffer」— 但命中本來就會再用 cutoff 日期過濾,多查的月份
    純屬浪費 (每月 15 號以後 --days 14 只需當月,卻每天多打 1879 次 MOPS);反過來 rescan --days 45
    在月初卻只包 2 個月 (10/06 只查 9、10 月,cutoff 08/22 起的 8 月尾巴根本沒查到)。
    """
    start = today - timedelta(days=days)
    y, m = start.year, start.month
    out = []
    while (y, m) <= (today.year, today.month):
        out.append((y - 1911, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=30, help='往前掃幾天 (預設 30)')
    ap.add_argument('--workers', type=int, default=4)
    ap.add_argument('--only-unknown', action='store_true',
                    help='只掃 issued 表沒的股票(catch 全新發行人;速度快 25 percent)')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--limit', type=int, help='只掃前 N 家 (測速/除錯用)')
    ap.add_argument('--no-cache', action='store_true', help='已結束的月份也查即時 (不用 mops_cache.db)')
    # 命中要等掃完才寫 DB → 被 cron_pulse 的 7200s 逾時砍掉 = 已抓到的新案全丟、未確認行也印不出來。
    #   超過期限就不再發新查詢,剩下的列「未確認 (deadline)」→ 照常寫 DB + audit 照常告警 (2026-10-06 審查建議)。
    ap.add_argument('--deadline', type=int, default=6300,
                    help='掃描最多跑幾秒 (預設 6300,cron_pulse 逾時 7200 留時間寫 DB);超過的公司列入未確認')
    args = ap.parse_args()

    today = datetime.now()
    cutoff_iso = (today - timedelta(days=args.days)).strftime('%Y-%m-%d')
    ym_list = months_back(today, args.days)
    now = today.strftime('%Y-%m-%d %H:%M:%S')
    use_cache = not args.no_cache

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    ensure_cols(conn)
    stocks = get_stock_list(conn, only_unknown=args.only_unknown)
    if args.limit:
        stocks = stocks[:args.limit]
    name_by_code = {s[0]: s[1] for s in stocks}
    conn.close()

    label = '僅未知發行人' if args.only_unknown else '全市場'
    print(f'掃 {label} CB 公告 {cutoff_iso} ~ {today:%Y-%m-%d} ({args.days} 天)')
    n_cached = sum(MC.month_closed(y, m) for y, m in ym_list) if use_cache else 0
    print(f'  {len(stocks)} 家公司 × {len(ym_list)} 個月 {ym_list} (已結束可走快取 {n_cached} 個月),'
          f'workers={args.workers},MOPS 節流 ≥{MC.DEFAULT.gap:.2f}s/次')

    # 平行掃描
    def make_sess():
        s = requests.Session()
        s.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        return s

    # 每個 worker thread 用自己的 session (舊版 sessions[i % workers] 會讓兩個 thread 共用同一個 Session)
    _tls = threading.local()

    def thread_sess():
        s = getattr(_tls, 's', None)
        if s is None:
            s = _tls.s = make_sess()
        return s

    def past_deadline():
        return time.time() - t0 > args.deadline

    def job(code, months):
        if past_deadline():
            return [], [(y, m, 'deadline') for y, m in months]
        return query_company(thread_sess(), code, months, with_status=True, cache=use_cache)

    def collect(code, items):
        for it in items:
            it['name'] = name_by_code.get(code, code)
            # 日期過濾 (在 days 範圍內)
            iso = D.to_iso(it['date_roc'])
            if iso and iso >= cutoff_iso:
                all_hits.append(it)

    all_hits = []
    unconfirmed = {}       # code → [(民國年, 月, 狀態)]:重試後 MOPS 仍沒給明確答覆的月份
    t0 = time.time()
    done = 0

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(job, code, ym_list): code for code, _ in stocks}
        for fut in as_completed(futures):
            code = futures[fut]
            try:
                items, unc = fut.result()
            except Exception as e:
                items, unc = [], [(y, m, 'exception') for y, m in ym_list]
                print(f'  [WARN] {code} 失敗: {e}')
            collect(code, items)
            if unc:
                unconfirmed[code] = unc
            done += 1
            if done % 200 == 0:
                el = time.time() - t0
                eta = el / done * (len(stocks) - done)
                print(f'  進度 {done}/{len(stocks)} ({el:.0f}s, eta {eta:.0f}s) · 累計 {len(all_hits)} 命中'
                      f' · 未確認 {len(unconfirmed)} 家')

    # 補查:第一輪沒拿到明確答覆的月份,等 MOPS 冷靜一下再逐家慢查一次 (只查那幾個月)。
    #   仍失敗的列進「未確認」— 那些公司這段期間的新公告可能漏了,audit_cb_coverage 會發 TG。
    n_dl = sum(1 for v in unconfirmed.values() if any(s == 'deadline' for _, _, s in v))
    if n_dl:
        print(f'  ⚠ 掃描超過 {args.deadline}s 上限 → {n_dl} 家沒查到,列入未確認 (已抓到的命中照常寫入)')
    if unconfirmed and not past_deadline():
        print(f'  第一輪 {len(unconfirmed)} 家有月份沒拿到明確答覆 → 30 秒後補查')
        time.sleep(30)
        sess2 = make_sess()
        for code in sorted(unconfirmed):
            if past_deadline():
                print(f'  ⚠ 補查到一半超過 {args.deadline}s 上限 → 剩下的維持未確認')
                break
            months = [(y, m) for y, m, _ in unconfirmed[code]]
            try:
                items, unc = query_company(sess2, code, months, tries=4, with_status=True, cache=use_cache)
            except Exception as e:
                items, unc = [], [(y, m, 'exception') for y, m in months]
                print(f'  [WARN] {code} 補查失敗: {e}')
            collect(code, items)        # 同一則公告若第一輪已收,後面 seen 去重
            if unc:
                unconfirmed[code] = unc
            else:
                del unconfirmed[code]

    elapsed = time.time() - t0
    print(f'\nMOPS 回應:{MC.DEFAULT.summary()} · 快取命中 {MC.CACHE.hits} 次')
    # ⚠ 「MOPS 查詢未確認 N 家」是 audit_cb_coverage 告警的依據,別改字樣;一定要印在「掃描完成」之前
    if unconfirmed:
        lst = ', '.join(f'{c}({"/".join(f"{m}月{s}" for _, m, s in v)})' for c, v in sorted(unconfirmed.items()))
        print(f'⚠ MOPS 查詢未確認 {len(unconfirmed)} 家 (重試+補查後仍被擋/逾時,這些公司的新公告可能漏抓): {lst[:600]}')
    else:
        print('MOPS 查詢未確認 0 家')
    print(f'\n掃描完成 ({elapsed:.0f}s),共 {len(all_hits)} 筆 CB 相關公告')

    # === 處理 hits → INSERT / UPDATE issued ===
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    ensure_cols(conn)

    n_account = n_board = n_new = n_conv = n_attr = 0
    updates = []
    convprice_hits = []
    board_hits = []      # (cb, stock, iso) → 之後進內文抓 方式/承銷商/發行額/年期

    sess_detail = make_sess()  # 用來抓 detail body 解析轉換價

    seen = set()
    for it in all_hits:
        key = (it['code'], it['date_roc'], it['title'][:30])
        if key in seen:
            continue
        seen.add(key)
        kind = classify(it['title'])
        if not kind:
            continue
        iso = D.to_iso(it['date_roc'])
        if not iso:
            continue
        for cb in derive_codes(it['code'], it['title']):
            # 「股票+第N次」推出的代號撞到同公司更早的舊債 → 改指在途案/下一個流水號 (2026-10-05 光譜:第三次有擔保
            #   推成 53813 = 2008 合正三,櫃買代號其實是累計檔數 → 53814)。舊債不動。
            cb = _dbm.resolve_new_cb_code(conn, it['code'], cb, iso, f'MOPS {kind}')
            row = conn.execute(
                'SELECT cb_code, fm_board_decision_date, fm_account_setup_date FROM issued WHERE cb_code=?',
                (cb,)).fetchone()
            if kind == 'account':
                if row and (not row['fm_account_setup_date'] or row['fm_account_setup_date'][:10] != iso):
                    note = f'確定專戶 {iso}'
                    if not args.dry_run:
                        conn.execute('''UPDATE issued SET fm_account_setup_date=?, fm_mops_updated_at=?,
                                        last_status_update=?, last_status_note=? WHERE cb_code=?''',
                                     (iso, now, now, note, cb))
                    n_account += 1
                    updates.append((cb, (it['name'] or '')[:10], note))
            elif kind == 'board':
                board_hits.append((cb, it['code'], iso))   # 新案/舊案都收,屬性齊的 fill 會直接跳過
                if not row:
                    note = f'董事會決議 {iso}'
                    if not args.dry_run:
                        conn.execute('''INSERT INTO issued
                                        (cb_code, stock_code, company, fm_board_decision_date,
                                         fm_mops_updated_at, updated_at, last_status_update, last_status_note)
                                        VALUES (?,?,?,?,?,?,?,?)''',
                                     (cb, it['code'], it['name'] or '', iso, now, now, now, '新案 ' + note))
                    n_new += 1
                    updates.append((cb, (it['name'] or '')[:10], '🆕新案 ' + note))
                elif not row['fm_board_decision_date']:
                    note = f'董事會決議 {iso}'
                    if not args.dry_run:
                        conn.execute('''UPDATE issued SET fm_board_decision_date=?, fm_mops_updated_at=?,
                                        last_status_update=?, last_status_note=? WHERE cb_code=?''',
                                     (iso, now, now, note, cb))
                    n_board += 1
                    updates.append((cb, (it['name'] or '')[:10], note))
            elif kind == 'convprice':
                convprice_hits.append((cb, it['code'], iso, (it['name'] or '')[:10]))

    # 董事會公告【內文】→ 方式/承銷商/發行額/年期 (2026-09-17 用戶:「華邦電是中國信託承銷,
    #   未來請看資料也要進來看一下」)。以前只看標題,新案進 DB 只有董事會日,要等每週一次的
    #   券商 xlsx 才補 (還會寫錯代號,見威剛九)。內文第 4/7/11/13 項當天就有。
    #   只對屬性有缺的案發 detail 請求;屬性齊的 fill 直接回 skip,零網路成本。
    #   順手:若 DB 董事會日其實是海外CB公告日 (華邦電四 02-10),改成國內公告日。
    seen_b = set()
    for cb, stock, iso in board_hits:
        if cb in seen_b:
            continue
        seen_b.add(cb)
        try:
            res = B.fill_from_announcement(conn, sess_detail, cb, stock, iso, dry_run=args.dry_run)
            if res.get('note'):
                n_attr += 1
                updates.append((cb, '', '內文屬性 ' + res['note']))
            if res.get('board_fixed'):
                updates.append((cb, '', f'董事會日更正 {res["board_fixed"][0]} → {res["board_fixed"][1]} (原為海外CB)'))
            for w in res.get('warns') or []:
                print(f'  ⚠ {cb} 公告與 DB 不符 → {w} (未覆寫)')
        except Exception as e:
            print(f'  [WARN] {cb} 董事會內文解析失敗: {e}')

    # 訂定轉換價 — 抓 detail body 解析
    seen_conv = set()
    for cb, stock, iso, nm in convprice_hits:
        if cb in seen_conv:
            continue
        seen_conv.add(cb)
        row = conn.execute('SELECT conv_price FROM issued WHERE cb_code=?', (cb,)).fetchone()
        if not row or (row['conv_price'] and row['conv_price'] > 0):
            continue
        try:
            yr, mo = int(iso[:4]) - 1911, int(iso[5:7])
            target_seq = P.cb_code_seq(cb)
            for itm in P.query_mops_list(sess_detail, stock, yr, mo):
                if not P.PAT_CONV_PRICE_TITLE.search(itm['title']):
                    continue
                seqs = P.parse_cb_seqs(itm['title'])
                if target_seq and seqs and target_seq not in seqs:
                    continue
                time.sleep(0.3)
                cp, _ = P.parse_body_conv_price(P.fetch_mops_detail(sess_detail, itm))
                if cp:
                    note = f'訂定轉換價 {cp}'
                    if not args.dry_run:
                        # 2026-09-10:順手補 fm_conv_price_set_date (公告日)。舊版只寫 conv_price,
                        # 儀表板「近5營業日剛訂價」列是看 fmConvPriceSetDate 的 → 掃描抓到的訂價
                        # 永遠不會出現在那一列,只有 fetch_mops_conv_price 走到的才會。
                        conn.execute('''UPDATE issued SET conv_price=?,
                                          fm_conv_price_set_date=COALESCE(fm_conv_price_set_date, ?),
                                          last_status_update=?, last_status_note=?
                                        WHERE cb_code=?''', (cp, iso, now, note, cb))
                    n_conv += 1
                    updates.append((cb, nm, note))
                    break
            time.sleep(0.3)
        except Exception as e:
            print(f'  [WARN] {cb} 訂定轉換價解析失敗: {e}')

    if not args.dry_run:
        conn.commit()

    print('\n=== 偵測到新狀態 ===' if updates else '\n(無新狀態)')
    for cb, nm, note in updates:
        print(f'  🆕 {cb} {nm}  {note}')
    tag = '  [dry-run]' if args.dry_run else ''
    # ⚠ 「新案 X / 補董事會 Y」前綴是 audit_cb_coverage 判「掃描完成」的標記,別改動順序
    print(f'\n新案 {n_new} / 補董事會 {n_board} / 補確定專戶 {n_account} / 補訂定轉換價 {n_conv} / 補內文屬性 {n_attr}{tag}')
    conn.close()


if __name__ == '__main__':
    main()
