#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""掃 MOPS「全市場關鍵字搜尋」找 issued 表還沒收到的新發 CB 案。

來源：MOPS ajax_t05sr01_1（歷史重大訊息查詢，全市場關鍵字搜尋）
篩選：「董事會決議/通過/同意 發行 (可)轉換公司債」標題
排除：轉換普通股 / 轉換結果 / 債權人異議 / 提前贖回 / 註銷 等非「新發行」訊息

對每個命中：
  cb_code = stock_code + 末位 (從「第N次」抓)
  比對 issued 表 → 沒有的 INSERT 新案
  寫入: cb_code / stock_code / company / fm_board_decision_date / fm_mops_updated_at

下游：
  - fetch_mops_milestones.py 接續抓 fm_account_setup_date
  - fetch_premium_rally.py 接續抓 stock 漲跌（若該案後續 eff_date 補上）
  - 元富 mail 進來時，masterlink_parser merge 補齊發行條件 (issue_price/term/method 等)

執行：py -3.12 discover_new_cbs.py [--days 90]
"""
import argparse
import io
import re
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / 'cb_data.db'
URL = 'https://mopsov.twse.com.tw/mops/web/ajax_t05sr01_1'

# 中文數字
ZH_NUM = {'一':1,'二':2,'三':3,'四':4,'五':5,'六':6,'七':7,'八':8,'九':9,'十':10,
          '壹':1,'貳':2,'參':3,'肆':4,'伍':5,'陸':6,'柒':7,'捌':8,'玖':9,'拾':10}

PAT_BOARD = re.compile(r'董事會.*?(?:決議|通過|同意).*?發行.*?(?:可轉換|轉換)\s*公司債|決議發行.*?(?:可轉換|轉換)\s*公司債')
PAT_CB_NUM = re.compile(r'第\s*([一二三四五六七八九十壹貳參肆伍陸柒捌玖拾\d]+)\s*次')
EXCLUDE_KW = ['轉換普通股','轉換結果','轉換為普通股','債權人異議','更正','提前贖回',
              '收回註銷','註銷','撤銷','停止','轉換比率','終止','員工認股權']


def to_iso(roc):
    m = re.match(r'(\d{3})/(\d{2})/(\d{2})', roc)
    if not m: return None
    yr = int(m.group(1)) + 1911
    return f'{yr:04d}-{m.group(2)}-{m.group(3)}'


def parse_cb_seq_int(title: str) -> int | None:
    """單一 seq (legacy / 簡化用)。多 seq 用 parse_cb_seqs。"""
    m = PAT_CB_NUM.search(title)
    if not m: return None
    raw = m.group(1)
    if raw.isdigit():
        return int(raw)
    return ZH_NUM.get(raw)


def parse_cb_seqs(title: str) -> list[int]:
    """從標題抓所有「第N次」的 N (一張公告可能含多次發行,如「第二次及第三次」)。"""
    seqs = []
    for m in PAT_CB_NUM.finditer(title):
        raw = m.group(1)
        if raw.isdigit():
            n = int(raw)
        elif raw in ZH_NUM:
            n = ZH_NUM[raw]
        else:
            continue
        if 1 <= n <= 9 and n not in seqs:
            seqs.append(n)
    return seqs


def search_market(session, typek: str, date1_roc: str, date2_roc: str, keyword: str) -> list[dict]:
    """ajax_t05sr01_1 全市場關鍵字搜尋。"""
    try:
        r = session.post(URL, data={
            'encodeURIComponent':'1','firstin':'1','step':'00','off':'1',
            'TYPEK': typek,
            'date1': date1_roc, 'date2': date2_roc,
            'keyword': keyword,
        }, timeout=40)
        r.encoding = 'utf-8'
    except Exception as e:
        print(f'  [ERR] {typek} {keyword}: {e}')
        return []
    soup = BeautifulSoup(r.text, 'html.parser')
    items = []
    for tr in soup.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) < 5: continue
        cells = [td.get_text(' ', strip=True) for td in tds[:5]]
        code, name, date, t, title = cells[0], cells[1], cells[2], cells[3], cells[4]
        if re.match(r'^\d{4,6}$', code) and re.match(r'\d{3}/\d{2}/\d{2}', date):
            items.append({
                'code': code, 'name': name,
                'date_roc': date, 'time': t,
                'title': re.sub(r'\s+',' ', title),
            })
    return items


def discover(days_back: int = 90) -> list[dict]:
    """找最近 days_back 天內所有市場的「董事會決議發行 CB」公告。"""
    today = datetime.now()
    d_from = today - timedelta(days=days_back)
    # 民國日期
    date1 = f'{d_from.year-1911}/{d_from.month:02d}/{d_from.day:02d}'
    date2 = f'{today.year-1911}/{today.month:02d}/{today.day:02d}'

    session = requests.Session()
    session.headers.update({'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})

    all_hits = {}
    for kw in ['轉換公司債', '可轉換公司債']:
        for typek in ['sii', 'otc', 'rotc', 'pub']:
            items = search_market(session, typek, date1, date2, kw)
            for it in items:
                if not PAT_BOARD.search(it['title']):
                    continue
                if any(bad in it['title'] for bad in EXCLUDE_KW):
                    continue
                key = (it['code'], it['date_roc'], it['title'][:30])
                all_hits[key] = {**it, 'typek': typek}
    return list(all_hits.values())


def derive_cb_code(stock_code: str, title: str) -> str | None:
    """從 stock_code + 「第N次」推 cb_code (單檔 only,legacy)。"""
    seq = parse_cb_seq_int(title)
    if seq is None or seq < 1 or seq > 9:
        return None
    if not (stock_code and stock_code.isdigit() and len(stock_code) == 4):
        return None
    return f'{stock_code}{seq}'


def derive_cb_codes(stock_code: str, title: str) -> list[str]:
    """從 stock_code + 標題所有「第N次」推所有 cb_code。
    處理「第二次及第三次」這類同公告多檔的 case。
    回傳 [cb_code, ...]。"""
    if not (stock_code and stock_code.isdigit() and len(stock_code) == 4):
        return []
    return [f'{stock_code}{n}' for n in parse_cb_seqs(title)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=90, help='往前搜幾天 (預設 90)')
    parser.add_argument('--dry-run', action='store_true', help='只列候選，不寫 DB')
    args = parser.parse_args()

    print(f'掃 MOPS 全市場「董事會決議發行 CB」(近 {args.days} 天)...')
    hits = discover(args.days)
    print(f'  共找到 {len(hits)} 筆候選')

    if not hits:
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    existing_cbs = {row[0] for row in conn.execute('SELECT cb_code FROM issued WHERE cb_code IS NOT NULL').fetchall()}

    new_cases = []
    skipped_known = []
    skipped_invalid = []
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for it in hits:
        cb_codes = derive_cb_codes(it['code'], it['title'])
        if not cb_codes:
            skipped_invalid.append(it)
            continue
        # 同公告可能有「第N次及第M次」多檔,每檔分別處理
        for cb in cb_codes:
            if cb in existing_cbs:
                # 已知 cb_code,但檢查 fm_board_decision_date 是否已寫;若沒寫就 update
                r = conn.execute('SELECT fm_board_decision_date FROM issued WHERE cb_code=?', (cb,)).fetchone()
                if r and not r['fm_board_decision_date']:
                    if not args.dry_run:
                        conn.execute('UPDATE issued SET fm_board_decision_date=?, fm_mops_updated_at=? WHERE cb_code=?',
                                     (to_iso(it['date_roc']), now, cb))
                        conn.commit()
                    print(f'  ✓ 補 board_decision: {cb} {it["name"][:8]} {it["date_roc"]}')
                skipped_known.append({**it, 'cb_code': cb})
                continue
            new_cases.append({**it, 'cb_code': cb})

    print()
    print(f'=== 新案: {len(new_cases)} 筆 (將 INSERT 進 issued 表) ===')
    for it in new_cases:
        print(f'  ★ {it["cb_code"]}  {it["code"]} {it["name"][:8]:10} {it["date_roc"]}  {it["title"][:70]}')

    if new_cases and not args.dry_run:
        print()
        print('寫入 DB...')
        for it in new_cases:
            try:
                conn.execute('''INSERT INTO issued (cb_code, stock_code, company, fm_board_decision_date, fm_mops_updated_at, updated_at)
                                VALUES (?,?,?,?,?,?)''',
                             (it['cb_code'], it['code'], it['name'], to_iso(it['date_roc']), now, now))
            except sqlite3.IntegrityError as e:
                print(f'  [SKIP] {it["cb_code"]} 已存在: {e}')
        conn.commit()

    print()
    print('=== 統計 ===')
    print(f'  全部候選     : {len(hits)}')
    print(f'  已在 issued  : {len(skipped_known)}')
    print(f'  cb_code 推導失敗: {len(skipped_invalid)}')
    print(f'  新案 INSERT  : {0 if args.dry_run else len(new_cases)} (--dry-run)' if args.dry_run else f'  新案 INSERT  : {len(new_cases)}')
    conn.close()


if __name__ == '__main__':
    main()
