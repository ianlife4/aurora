#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""從 MOPS 重大訊息抓「董事會決議發行 CB」+「確定存儲專戶」公告日。

對「未上市」+「listing_date 在最近 12 個月內或未定」的 issued CB：
  1. 對每家 stock_code，查 (listing_date - 6 個月 ~ listing_date + 0) 範圍的重大訊息
  2. 抓兩個關鍵節點：
     - "董事會決議發行...可轉換公司債" → fm_board_decision_date
     - "存儲專戶...代收價款" → fm_account_setup_date
  3. 用「第N次無擔保」配對 cb_code 末位（如「第三次」→ cb_code 末碼 = 3）
  4. UPDATE issued

執行：py -3.12 fetch_mops_milestones.py [--all] [--limit N]
資料源：MOPS https://mopsov.twse.com.tw/mops/web/ajax_t05st01
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
from bs4 import BeautifulSoup

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / 'cb_data.db'
URL = 'https://mopsov.twse.com.tw/mops/web/ajax_t05st01'
MAX_WORKERS = 4   # MOPS 不像 FinMind 開放，並行少一點
TIMEOUT = 25
SLEEP_BETWEEN = 0.4

# 中文數字 → 阿拉伯
ZH_NUM = {'一':1,'二':2,'三':3,'四':4,'五':5,'六':6,'七':7,'八':8,'九':9,'十':10,
          '壹':1,'貳':2,'參':3,'肆':4,'伍':5,'陸':6,'柒':7,'捌':8,'玖':9,'拾':10}

# 關鍵字 patterns（標題已經 _clean_title 把 \s+ 壓成單空格，pattern 用 \s* 容許）
# 涵蓋 MOPS 標題各種寫法：
#   "本公司董事會決議發行國內第N次無擔保轉換公司債"
#   "公告本公司董事會通過/同意發行..."
#   "本公司擬發行國內第N次無擔保轉換公司債" (沒有"董事會"前綴!)
PAT_BOARD = re.compile(r'(?:董事會.*?(?:決議|通過|同意).*?發行|擬發行|決議發行).*?(?:可轉換|轉換)\s*公司債')
# 排除誤判 (這些都是已發行後事項，不是 board)
PAT_BOARD_EXCLUDE = re.compile(r'轉換普通股|轉換結果|轉換為普通股|債權人異議|更正|提前贖回|收回註銷|註銷|撤銷|停止|轉換比率|終止|員工認股權|備案|備查|收足|繳款')
# 含「存儲專戶」「代收價款專戶」「存儲帳戶」「代收款專戶」「代收價款及專戶存儲」「專戶存儲」「存儲及代收」
PAT_ACCOUNT = re.compile(r'存儲\s*(?:專戶|帳戶)|代收\s*(?:價款|款)\s*(?:及|之)?\s*(?:專戶|存儲)|專戶\s*存儲|代收\s*(?:價款|款).{0,15}(?:存儲|專戶)')
PAT_CB_NUM = re.compile(r'第\s*([一二三四五六七八九十壹貳參肆伍陸柒捌玖拾\d]+)\s*次')


def _clean_title(t: str) -> str:
    """壓縮多重 whitespace（含換行）→ 單空格，改善 regex 匹配。"""
    return re.sub(r'\s+', ' ', t or '')


def parse_cb_seqs(title: str) -> list[int]:
    """從標題抓所有「第N次」的 N（一張公告可能含多次發行如「第五次及第六次」），回傳 list[int]。"""
    seqs = []
    for m in PAT_CB_NUM.finditer(title):
        raw = m.group(1)
        if raw.isdigit():
            seqs.append(int(raw))
        elif raw in ZH_NUM:
            seqs.append(ZH_NUM[raw])
    return seqs


def cb_code_seq(cb_code: str) -> int | None:
    """cb_code 末位 = 第幾次發行（4123<n>，n=1..9）"""
    s = (cb_code or '').strip()
    if len(s) < 5 or not s.isdigit():
        return None
    return int(s[-1])


def query_mops(session: requests.Session, co_id: str, year_roc: int, month: int) -> list[dict]:
    """查 MOPS 個股某月重大訊息。回傳 [{date, time, title}]"""
    try:
        r = session.post(URL, data={
            'encodeURIComponent':'1','step':'1','firstin':'1','off':'1',
            'queryName':'co_id','inpuType':'co_id','TYPEK':'all','isnew':'false',
            'co_id': str(co_id),
            'year': str(year_roc),
            'month': f'{month:02d}',
        }, timeout=TIMEOUT)
        r.encoding = 'utf-8'
    except Exception:
        return []
    soup = BeautifulSoup(r.text, 'html.parser')
    items = []
    for tr in soup.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) < 5:
            continue
        cells = [td.get_text(' ', strip=True) for td in tds[:5]]
        code, name, date, t, title = cells[0], cells[1], cells[2], cells[3], cells[4]
        if str(co_id) in code and re.match(r'\d{3}/\d{2}/\d{2}', date):
            items.append({'date': date, 'time': t, 'title': title})
    return items


def roc_to_iso(roc: str) -> str | None:
    """民國日期 '115/03/05' → '2026-03-05'"""
    m = re.match(r'(\d{3})/(\d{2})/(\d{2})', roc)
    if not m:
        return None
    yr = int(m.group(1)) + 1911
    return f'{yr:04d}-{m.group(2)}-{m.group(3)}'


def months_between(d1_iso: str, d2_iso: str) -> list[tuple[int, int]]:
    """回傳 d1~d2 之間每個月份的 (民國年, 月)。"""
    d1 = datetime.strptime(d1_iso, '%Y-%m-%d')
    d2 = datetime.strptime(d2_iso, '%Y-%m-%d')
    if d1 > d2:
        d1, d2 = d2, d1
    out = []
    cur = datetime(d1.year, d1.month, 1)
    end = datetime(d2.year, d2.month, 1)
    while cur <= end:
        out.append((cur.year - 1911, cur.month))
        # next month
        if cur.month == 12:
            cur = datetime(cur.year + 1, 1, 1)
        else:
            cur = datetime(cur.year, cur.month + 1, 1)
    return out


def normalize_date(s):
    if not s:
        return None
    raw = str(s).strip().split(' ')[0].split('T')[0].replace('/', '-')
    parts = raw.split('-')
    if len(parts) != 3:
        return None
    try:
        y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
        return f'{y:04d}-{m:02d}-{d:02d}' if 1900 < y < 2100 else None
    except ValueError:
        return None


def get_targets(force_all: bool, limit: int | None) -> list[dict]:
    """取需要抓 MOPS 的 issued。
    預設只抓「未上市 + listing_date 推估在過去 12 個月內或未定」的新案。"""
    today = datetime.now().strftime('%Y-%m-%d')
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    if force_all:
        rows = conn.execute('''
            SELECT cb_code, company, stock_code, eff_date, listing_date, fm_board_decision_date, fm_account_setup_date
            FROM issued
            WHERE stock_code IS NOT NULL AND stock_code != ''
            ORDER BY listing_date DESC
        ''').fetchall()
    else:
        # 抓條件（任一）：
        #   (a) 未抓過 MOPS  (新 INSERT 的案，例如 discover_new_cbs 剛塞進來)
        #   (b) listing_date 在最近 12 個月內或未來  (還在發行中)
        #   (c) listing_date = '未定' (還在等待中的案)
        # 注意：移除「fm_board_decision_date IS NULL」這條 OR，因為它會每天重抓 94 筆
        #       「已嘗試但抓不到」的老案 (例如 2019 年發行但 MOPS 找不到對應公告)
        #       要重試這些案請用 --all (覆蓋整張表)
        rows = conn.execute('''
            SELECT cb_code, company, stock_code, eff_date, listing_date, fm_board_decision_date, fm_account_setup_date
            FROM issued
            WHERE stock_code IS NOT NULL AND stock_code != ''
              AND (is_legacy IS NULL OR is_legacy != 1)  -- 排除 2004-2010 回填老案 (MOPS 無其公告,白抓 773 筆害 Step 2 逾時)
              AND (
                fm_mops_updated_at IS NULL
                OR (substr(listing_date,1,10) >= ? AND substr(listing_date,1,10) <= '2099-12-31')
                OR listing_date = '未定'
                OR (
                  -- 還沒掛牌 (listing 空字串/NULL/未來) 且近 12 個月才公告 → 仍在發行中,要持續追 milestone
                  -- (修補: 原本只認 '未定',漏掉 listing_date='' 的進行中案,如 52892 宜鼎二的確定專戶)
                  (listing_date IS NULL OR listing_date = '' OR substr(listing_date,1,10) > ?)
                  AND (fm_board_decision_date >= ? OR substr(eff_date,1,10) >= ?)
                )
              )
            ORDER BY listing_date DESC
        ''', (one_year_ago, today, one_year_ago, one_year_ago)).fetchall()
    conn.close()
    targets = [dict(r) for r in rows]
    if limit:
        targets = targets[:limit]
    return targets


def update_db(cb_code: str, fields: dict) -> bool:
    if not fields:
        return False
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    cols, vals = [], []
    for k, v in fields.items():
        cols.append(f'{k}=?')
        vals.append(v)
    vals.append(cb_code)
    try:
        c.execute(f'UPDATE issued SET {", ".join(cols)} WHERE cb_code=?', vals)
        conn.commit()
        return c.rowcount > 0
    finally:
        conn.close()


def find_milestones(items: list[dict], cb_seq: int | None,
                     eff_iso: str | None = None,
                     listing_iso: str | None = None) -> tuple[str | None, str | None]:
    """從訊息列表找 (board_decision_date, account_setup_date)。

    若標題有「第N次」list + cb_seq 已知 → 接受 cb_seq 在 list 中的（如「第五次及第六次」抓到 [5,6]）。
    若標題沒有「第N次」 → 放寬接受（單一發行公司只發過一檔的情況）。

    Sanity filter (避免抓到不同次發行的公告):
      - board 必須在 eff 之前 (若 eff 已知)
      - account 必須在 listing 之前 (若 listing 已知)
      - board / account 也不能比 eff/listing 早超過 12 個月 (避免抓到太老的)
    """
    board_date = None
    account_date = None
    items_sorted = sorted(items, key=lambda x: (x['date'], x['time']))
    for it in items_sorted:
        title = _clean_title(it['title'])
        # 先過濾誤判: 「轉換普通股」「更正」「提前贖回」等已發行後事項
        if PAT_BOARD_EXCLUDE.search(title):
            continue
        is_board = bool(PAT_BOARD.search(title))
        is_account = bool(PAT_ACCOUNT.search(title))
        if not (is_board or is_account):
            continue
        seqs = parse_cb_seqs(title)
        # cb_seq 已知時：seqs 為空（無「第N次」）→ 放寬接受；seqs 有值 → 必須包含 cb_seq
        if cb_seq is not None and seqs and cb_seq not in seqs:
            continue
        iso = roc_to_iso(it['date'])
        if not iso:
            continue
        # Sanity filter
        if is_board and eff_iso and iso > eff_iso:
            continue  # board 不可能在 eff 之後
        if is_account and listing_iso and iso > listing_iso:
            continue  # account 不可能在 listing 之後
        # 太久遠的也排除 (>12 個月差距)
        if is_board and eff_iso:
            try:
                d1 = datetime.strptime(iso, '%Y-%m-%d')
                d2 = datetime.strptime(eff_iso, '%Y-%m-%d')
                if (d2 - d1).days > 365:
                    continue
            except ValueError: pass
        if is_account and listing_iso:
            try:
                d1 = datetime.strptime(iso, '%Y-%m-%d')
                d2 = datetime.strptime(listing_iso, '%Y-%m-%d')
                if (d2 - d1).days > 365:
                    continue
            except ValueError: pass
        if is_board and not board_date:
            board_date = iso
        if is_account and not account_date:
            account_date = iso
    return (board_date, account_date)


def process_one(t: dict, session: requests.Session) -> tuple[str, dict]:
    cb = t['cb_code']
    stk = t['stock_code']
    cb_seq = cb_code_seq(cb)
    fields = {'fm_mops_updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

    listing = normalize_date(t.get('listing_date'))
    eff = normalize_date(t.get('eff_date'))
    # 範圍：listing_date 前 12 個月 ~ listing_date+1 月
    # (KY 股 board → listing 可達 200+ 天，普通案 ~65 天，給足 365d 涵蓋)
    if listing:
        d_to = (datetime.strptime(listing, '%Y-%m-%d') + timedelta(days=10)).strftime('%Y-%m-%d')
        d_from = (datetime.strptime(listing, '%Y-%m-%d') - timedelta(days=365)).strftime('%Y-%m-%d')
    else:
        d_to = datetime.now().strftime('%Y-%m-%d')
        d_from = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

    months = months_between(d_from, d_to)
    all_items = []
    for yr_roc, m in months:
        items = query_mops(session, stk, yr_roc, m)
        all_items.extend(items)
        time.sleep(SLEEP_BETWEEN)

    board, account = find_milestones(all_items, cb_seq, eff_iso=eff, listing_iso=listing)
    # 新里程碑 → 設 last_status_update + note,讓 HTML「已發行近期更新浮頂」抓得到
    # (本來只有 scan_cb_disclosures 全市場掃描器會寫;但全市場 keyword 失效後,
    # 逐檔輪詢成主力,沒寫的話新案抓到 milestone 也不會浮頂)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    new_notes = []
    if board:
        fields['fm_board_decision_date'] = board
        if not t.get('fm_board_decision_date'):
            new_notes.append(f'董事會決議 {board}')
    if account:
        fields['fm_account_setup_date'] = account
        if not t.get('fm_account_setup_date'):
            new_notes.append(f'確定專戶 {account}')
    if new_notes:
        fields['last_status_update'] = now
        fields['last_status_note'] = ' / '.join(new_notes)
    return (cb, fields)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--all', action='store_true', help='抓全部 issued')
    parser.add_argument('--limit', type=int, help='只抓前 N 筆')
    parser.add_argument('--cb', type=str, help='只抓指定 cb_code (debug)')
    args = parser.parse_args()

    if args.cb:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        r = conn.execute('SELECT cb_code, company, stock_code, eff_date, listing_date, fm_board_decision_date, fm_account_setup_date FROM issued WHERE cb_code=?', (args.cb,)).fetchone()
        conn.close()
        if not r:
            print(f'cb_code {args.cb} 不存在')
            return
        targets = [dict(r)]
    else:
        targets = get_targets(args.all, args.limit)

    print(f'準備抓 {len(targets)} 筆 issued (MOPS 重大訊息)')

    t0 = time.time()
    counters = {'updated': 0, 'board': 0, 'account': 0}

    # 多 worker session
    def make_session():
        s = requests.Session()
        s.headers.update({'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        return s

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = []
        for t in targets:
            futures.append(pool.submit(process_one, t, make_session()))
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                cb, fields = fut.result()
            except Exception as e:
                print(f'  [ERR] {e}')
                continue
            if 'fm_board_decision_date' in fields:
                counters['board'] += 1
            if 'fm_account_setup_date' in fields:
                counters['account'] += 1
            if update_db(cb, fields):
                counters['updated'] += 1
            if i % 20 == 0:
                print(f'  進度 {i}/{len(targets)}  board={counters["board"]}  account={counters["account"]}')

    elapsed = time.time() - t0
    print()
    print('=== 結果 ===')
    print(f'  總共      : {len(targets)}')
    print(f'  抓到董事會 : {counters["board"]}')
    print(f'  抓到專戶   : {counters["account"]}')
    print(f'  DB updated : {counters["updated"]}')
    print(f'  耗時       : {elapsed:.1f}s')


if __name__ == '__main__':
    main()
