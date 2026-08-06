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

執行: py -3.12 scan_cb_disclosures.py [--days 30] [--dry-run]
                                      [--only-unknown] (只掃 issued 表沒的股票)
                                      [--workers 4]
"""
import argparse
import io
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import requests

import discover_new_cbs as D
import fetch_mops_milestones as M
import fetch_mops_conv_price as P

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


def query_company(session, co_id, ym_list, retry_empty=2):
    """對單家公司,跨 N 個月查 MOPS,回傳所有 CB 相關公告。

    🔴 retry_empty:MOPS 被擋時【回空清單而不是拋例外】(HTTP 200 但無資料),
       舊版直接當成「這家沒公告」靜默跳過,log 零 WARN 完全看不出來 →
       3260 威剛九 7/28 就公告,全市場掃描卻連續多天沒抓到 (2026-08-06 用戶發現)。
       1879 家 × 4 workers 猛打 MOPS 時這種擋很常見,所以空結果要重試幾次再放棄。
    """
    for _attempt in range(retry_empty + 1):
        items = _query_company_once(session, co_id, ym_list)
        if items or _attempt == retry_empty:
            return items
        time.sleep(1.0 + _attempt)
    return []


def _query_company_once(session, co_id, ym_list):
    out = []
    for yr_roc, mo in ym_list:
        items = M.query_mops(session, co_id, yr_roc, mo)
        for it in items:
            if CB_TITLE_KEYS.search(it.get('title', '')):
                out.append({
                    'code': co_id, 'name': None,  # name 之後補
                    'date_roc': it.get('date', ''),
                    'time': it.get('time', ''),
                    'title': re.sub(r'\s+', ' ', it.get('title', '')),
                })
        time.sleep(0.4)
    return out


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
    """根據 days 算要查的 (year_roc, month) list。"""
    months = set()
    cursor = today
    for _ in range(days // 25 + 2):  # 多包 1 個月 buffer
        months.add((cursor.year - 1911, cursor.month))
        if cursor.day > 5 and len(months) > 1:
            break
        cursor = (cursor.replace(day=1) - timedelta(days=1))
    return sorted(months)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=30, help='往前掃幾天 (預設 30)')
    ap.add_argument('--workers', type=int, default=4)
    ap.add_argument('--only-unknown', action='store_true',
                    help='只掃 issued 表沒的股票(catch 全新發行人;速度快 25 percent)')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    today = datetime.now()
    cutoff_iso = (today - timedelta(days=args.days)).strftime('%Y-%m-%d')
    ym_list = months_back(today, args.days)
    now = today.strftime('%Y-%m-%d %H:%M:%S')

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    ensure_cols(conn)
    stocks = get_stock_list(conn, only_unknown=args.only_unknown)
    name_by_code = {s[0]: s[1] for s in stocks}
    conn.close()

    label = '僅未知發行人' if args.only_unknown else '全市場'
    print(f'掃 {label} CB 公告 {cutoff_iso} ~ {today:%Y-%m-%d} ({args.days} 天)')
    print(f'  {len(stocks)} 家公司 × {len(ym_list)} 個月,workers={args.workers}')

    # 平行掃描
    def make_sess():
        s = requests.Session()
        s.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        return s

    sessions = [make_sess() for _ in range(args.workers)]
    all_hits = []
    t0 = time.time()
    done = 0

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {}
        for i, (code, _) in enumerate(stocks):
            sess = sessions[i % args.workers]
            futures[ex.submit(query_company, sess, code, ym_list)] = code
        for fut in as_completed(futures):
            code = futures[fut]
            try:
                items = fut.result()
            except Exception as e:
                items = []
                if done < 10:
                    print(f'  [WARN] {code} 失敗: {e}')
            for it in items:
                it['name'] = name_by_code.get(code, code)
                # 日期過濾 (在 days 範圍內)
                iso = D.to_iso(it['date_roc'])
                if iso and iso >= cutoff_iso:
                    all_hits.append(it)
            done += 1
            if done % 200 == 0:
                el = time.time() - t0
                eta = el / done * (len(stocks) - done)
                print(f'  進度 {done}/{len(stocks)} ({el:.0f}s, eta {eta:.0f}s) · 累計 {len(all_hits)} 命中')

    elapsed = time.time() - t0
    print(f'\n掃描完成 ({elapsed:.0f}s),共 {len(all_hits)} 筆 CB 相關公告')

    # === 處理 hits → INSERT / UPDATE issued ===
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    ensure_cols(conn)

    n_account = n_board = n_new = n_conv = 0
    updates = []
    convprice_hits = []

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
                        conn.execute('''UPDATE issued SET conv_price=?, last_status_update=?, last_status_note=?
                                        WHERE cb_code=?''', (cp, now, note, cb))
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
    print(f'\n新案 {n_new} / 補董事會 {n_board} / 補確定專戶 {n_account} / 補訂定轉換價 {n_conv}{tag}')
    conn.close()


if __name__ == '__main__':
    main()
