#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""每天掃 MOPS 全市場 CB 公開資訊,偵測新里程碑並標記「狀態更新」。

跟逐檔輪詢 (fetch_mops_milestones) 的差別:
  - 逐檔輪詢: 對 DB 內已知 CB 一檔一檔查 → 沒被 targeting 到的會漏 (如 52892 listing='' 被跳過)
  - 本支: 用全市場關鍵字 feed (ajax_t05sr01_1) 一次抓「所有公司」近 N 天的 CB 公告,
          不依賴某檔有沒有被 targeting,不會漏,也快 (~8 個查詢 vs 194 檔輪詢)

分類路由:
  - 「董事會決議發行...轉換公司債」→ 新案 INSERT / 補 fm_board_decision_date
  - 「確定專戶/代收價款」→ 補 fm_account_setup_date
  (轉換價的「值」由 fetch_mops_conv_price 填,那邊也會設 last_status_update,此處不重複)

偵測到「新資訊」(欄位從空→有 或 新案) 才設 last_status_update + last_status_note
→ HTML 已發行列表把近期更新的浮到頂端 + 🆕 badge。已捕捉過的不會重複觸發 (避免舊聞一直浮頂)。

執行: py -3.12 scan_cb_disclosures.py [--days 7] [--dry-run]
"""
import argparse
import io
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests

import time

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
    """抓標題所有「第N次」(1~10,含「第十次」6 碼型);discover 版只到 9,這裡擴到 10。"""
    seqs = []
    for m in D.PAT_CB_NUM.finditer(title):
        raw = m.group(1)
        n = int(raw) if raw.isdigit() else D.ZH_NUM.get(raw)
        if n and 1 <= n <= 10 and n not in seqs:
            seqs.append(n)
    return seqs


def derive_codes(stock, title):
    """stock(4碼) + 第N次 → cb_code。第十次 → stock+'10' (6 碼,如 622010 岳豐十)。"""
    if not (stock and stock.isdigit() and len(stock) == 4):
        return []
    return [f'{stock}{n}' for n in parse_seqs(title)]


def classify(title):
    """'board' / 'account' / 'convprice' / None。市場全掃三類核心 CB 公告。"""
    if P.PAT_CONV_PRICE_TITLE.search(title):   # 「...轉換價格及溢價率」訂定公告 (先判,標題夠特定)
        return 'convprice'
    if M.PAT_BOARD_EXCLUDE.search(title):
        return None
    if D.PAT_BOARD.search(title):
        return 'board'
    if M.PAT_ACCOUNT.search(title):
        return 'account'
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=7, help='往前掃幾天 (預設 7)')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    today = datetime.now()
    d_from = today - timedelta(days=args.days)
    date1 = f'{d_from.year - 1911}/{d_from.month:02d}/{d_from.day:02d}'
    date2 = f'{today.year - 1911}/{today.month:02d}/{today.day:02d}'
    now = today.strftime('%Y-%m-%d %H:%M:%S')

    sess = requests.Session()
    sess.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})

    print(f'掃全市場 CB 公告 {date1} ~ {date2} ({args.days} 天)...')
    hits = {}
    for kw in ['轉換公司債', '可轉換公司債']:
        for tk in ['sii', 'otc', 'rotc', 'pub']:
            for it in D.search_market(sess, tk, date1, date2, kw):
                hits[(it['code'], it['date_roc'], it['title'][:30])] = it
    print(f'  共 {len(hits)} 筆 CB 相關公告')

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    ensure_cols(conn)

    n_account = n_board = n_new = n_conv = 0
    updates = []
    convprice_hits = []  # (cb, stock, iso, name) — 稍後抓 detail body 解析價格

    for it in hits.values():
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
                if row and not row['fm_account_setup_date']:
                    note = f'確定專戶 {iso}'
                    if not args.dry_run:
                        conn.execute('''UPDATE issued SET fm_account_setup_date=?, fm_mops_updated_at=?,
                                        last_status_update=?, last_status_note=? WHERE cb_code=?''',
                                     (iso, now, now, note, cb))
                    n_account += 1
                    updates.append((cb, it['name'][:10], note))
            elif kind == 'board':
                if not row:
                    note = f'董事會決議 {iso}'
                    if not args.dry_run:
                        conn.execute('''INSERT INTO issued
                                        (cb_code, stock_code, company, fm_board_decision_date,
                                         fm_mops_updated_at, updated_at, last_status_update, last_status_note)
                                        VALUES (?,?,?,?,?,?,?,?)''',
                                     (cb, it['code'], it['name'], iso, now, now, now, '新案 ' + note))
                    n_new += 1
                    updates.append((cb, it['name'][:10], '🆕新案 ' + note))
                elif not row['fm_board_decision_date']:
                    note = f'董事會決議 {iso}'
                    if not args.dry_run:
                        conn.execute('''UPDATE issued SET fm_board_decision_date=?, fm_mops_updated_at=?,
                                        last_status_update=?, last_status_note=? WHERE cb_code=?''',
                                     (iso, now, now, note, cb))
                    n_board += 1
                    updates.append((cb, it['name'][:10], note))
            elif kind == 'convprice':
                convprice_hits.append((cb, it['code'], iso, it['name'][:10]))

    # 訂定轉換價: 對偵測到的案抓 MOPS detail body 解析價格 (reuse fetch_mops_conv_price 的 per-CB 解析)
    seen_conv = set()
    for cb, stock, iso, nm in convprice_hits:
        if cb in seen_conv:
            continue
        seen_conv.add(cb)
        row = conn.execute('SELECT conv_price FROM issued WHERE cb_code=?', (cb,)).fetchone()
        if not row or (row['conv_price'] and row['conv_price'] > 0):
            continue  # 不在 issued 或已有價 → 跳過 (已有價的更新交給 fetch_mops_conv_price --force)
        try:
            yr, mo = int(iso[:4]) - 1911, int(iso[5:7])
            target_seq = P.cb_code_seq(cb)
            for itm in P.query_mops_list(sess, stock, yr, mo):
                if not P.PAT_CONV_PRICE_TITLE.search(itm['title']):
                    continue
                seqs = P.parse_cb_seqs(itm['title'])
                if target_seq and seqs and target_seq not in seqs:
                    continue
                time.sleep(0.3)
                cp, _ = P.parse_body_conv_price(P.fetch_mops_detail(sess, itm))
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
