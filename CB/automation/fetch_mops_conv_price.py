#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自動抓 CB 轉換價格 (從 MOPS 重大訊息,比 PDF 快 30 倍)

優勢:
  - MOPS 重大訊息每頁 5 KB,而 PDF 13 MB
  - HTML 結構化「(3) 訂定轉換價格為每股新台幣 1,072 元」清楚
  - 抓兩種公告:
    a) 董事會決議發行 → 「暫定」conv_price
    b) 「公告本公司...轉換公司債之轉換價格及溢價率」 → 「訂定」conv_price (權威,訂價版本)

對象:
  - upcoming_auctions 表 (TWSE 已公告即將開標)
  - issued 表 conv_price IS NULL 且 eff_date 近 6 個月

執行:
  py -3.12 fetch_mops_conv_price.py           # auto
  py -3.12 fetch_mops_conv_price.py 47491     # 指定 CB
  py -3.12 fetch_mops_conv_price.py --force   # 覆寫已有
"""
import argparse
import io
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings()
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

HERE = Path(__file__).parent
DB_PATH = HERE / 'cb_data.db'
MOPS_URL = 'https://mopsov.twse.com.tw/mops/web/ajax_t05st01'
TIMEOUT = 25

ZH_NUM = {'一':1,'二':2,'三':3,'四':4,'五':5,'六':6,'七':7,'八':8,'九':9,'十':10}

PAT_CONV_PRICE_TITLE = re.compile(r'轉換價格.*?(?:溢價率|及.*?率|訂定)')
PAT_BOARD_DECISION = re.compile(r'(?:董事會.*?(?:決議|通過|同意).*?發行|擬發行).*?(?:可轉換|轉換)\s*公司債')
PAT_CB_NUM_TITLE = re.compile(r'第\s*([一二三四五六七八九十\d]+)\s*次')

# Body parse: priority order
# 注意:「新台幣」/「新臺幣」/「新臺幤」都要接受 (MOPS 公告用字不統一)
NTD = r'新[台臺][幣幤]'
BODY_PATTERNS = [
    (re.compile(rf'訂定轉換價格為每股{NTD}\s*([\d,]+(?:\.\d+)?)\s*元'), 'mops-definite'),
    (re.compile(rf'訂定.*?轉換價格為每股{NTD}\s*([\d,]+(?:\.\d+)?)\s*元'), 'mops-definite'),
    (re.compile(rf'轉換價格.*?訂定為每股{NTD}\s*([\d,]+(?:\.\d+)?)\s*元'), 'mops-definite'),
    (re.compile(rf'暫定轉換價格為每股{NTD}\s*([\d,]+(?:\.\d+)?)\s*元'), 'mops-preliminary'),
    (re.compile(rf'暫定.*?轉換價格為每股{NTD}\s*([\d,]+(?:\.\d+)?)\s*元'), 'mops-preliminary'),
    (re.compile(rf'轉換公司債之?轉換價格暫定為[每股{NTD}\s]*([\d,]+(?:\.\d+)?)\s*元'), 'mops-preliminary'),
]


def cb_code_seq(cb_code: str) -> int | None:
    """cb_code 末碼 = 第 N 次發行"""
    s = (cb_code or '').strip()
    return int(s[-1]) if len(s) >= 5 and s.isdigit() else None


def parse_cb_seqs(title: str) -> list[int]:
    seqs = []
    for m in PAT_CB_NUM_TITLE.finditer(title):
        raw = m.group(1)
        if raw.isdigit(): seqs.append(int(raw))
        elif raw in ZH_NUM: seqs.append(ZH_NUM[raw])
    return seqs


def query_mops_list(session, co_id: str, year_roc: int, month: int) -> list[dict]:
    """查某月重大訊息清單, 回傳含 seq_no/spoke_date/spoke_time 的 detail 觸發資訊"""
    try:
        r = session.post(MOPS_URL, data={
            'encodeURIComponent':'1','step':'1','firstin':'1','off':'1',
            'queryName':'co_id','inpuType':'co_id','TYPEK':'all','isnew':'false',
            'co_id': str(co_id),
            'year': str(year_roc), 'month': f'{month:02d}',
        }, timeout=TIMEOUT, verify=False)
        r.encoding = 'utf-8'
    except Exception:
        return []
    soup = BeautifulSoup(r.text, 'html.parser')
    items = []
    for tr in soup.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) < 5: continue
        title = tds[4].get_text(' ', strip=True)
        title = re.sub(r'\s+', ' ', title)
        # 從 onclick 解析 detail 觸發參數
        onclick = ''
        for tag in tr.find_all(onclick=True):
            onclick = tag.get('onclick') or ''
            if 'seq_no' in onclick: break
        if not onclick: continue
        params = {}
        for m in re.finditer(r"(\w+)\.value\s*=\s*'([^']*)'", onclick):
            params[m.group(1)] = m.group(2)
        if 'seq_no' not in params: continue
        date = tds[2].get_text(' ', strip=True)
        items.append({
            'date': date, 'title': title,
            'co_id': params.get('co_id', co_id),
            'TYPEK': params.get('TYPEK', 'sii'),
            'seq_no': params.get('seq_no'),
            'spoke_date': params.get('spoke_date'),
            'spoke_time': params.get('spoke_time'),
        })
    return items


def fetch_mops_detail(session, item: dict) -> str:
    """抓某筆重大訊息詳細內文 (step=2),回傳純文字"""
    try:
        r = session.post(MOPS_URL, data={
            'firstin':'true','b_date':'','e_date':'',
            'TYPEK': item.get('TYPEK','sii'),
            'year':'', 'month':'', 'type':'',
            'co_id': item['co_id'],
            'spoke_date': item['spoke_date'],
            'spoke_time': item['spoke_time'],
            'seq_no': item['seq_no'],
            'MEETING_STEP':'','MODEL':'','ITEM':'',
            'step':'2','off':'1',
        }, timeout=TIMEOUT, verify=False)
        r.encoding = 'utf-8'
        return BeautifulSoup(r.text, 'html.parser').get_text(' ', strip=True)
    except Exception:
        return ''


def parse_body_conv_price(body: str) -> tuple[float | None, str]:
    for pat, hint in BODY_PATTERNS:
        m = pat.search(body)
        if m:
            try:
                v = float(m.group(1).replace(',', ''))
                if 0.01 < v < 100000:
                    return v, hint
            except ValueError:
                pass
    return None, 'not-found'


# 「定價時間」parse — 抓兩個關鍵日期
PAT_SET_DATE_FACT = re.compile(r'事實發生日\s*[::]\s*([0-9]+)[/-]([0-9]+)[/-]([0-9]+)')
PAT_SET_DATE_ANCHOR = re.compile(r'以民國\s*([0-9]+)\s*年\s*([0-9]+)\s*月\s*([0-9]+)\s*日為.{0,15}訂定基準日')


def roc_to_ad(yr: str, mo: str, day: str) -> str | None:
    """民國 YY/MM/DD → 西元 YYYY-MM-DD"""
    try:
        ad = int(yr) + 1911
        return f'{ad:04d}-{int(mo):02d}-{int(day):02d}'
    except (ValueError, TypeError):
        return None


def parse_body_pricing_dates(body: str) -> tuple[str | None, str | None]:
    """從公告 body 抓兩個定價日期:
       - set_date    = 事實發生日 (MOPS 公告當天,正式對外公告)
       - anchor_date = 訂定基準日 (個股 close 樣本截止日,定價計算用)
    """
    set_date = None
    m = PAT_SET_DATE_FACT.search(body)
    if m:
        set_date = roc_to_ad(*m.groups())
    anchor_date = None
    m2 = PAT_SET_DATE_ANCHOR.search(body)
    if m2:
        anchor_date = roc_to_ad(*m2.groups())
    return set_date, anchor_date


def get_targets(conn, force=False, cb_codes=None, also_missing_dates=False, months_window=6):
    cur = conn.cursor()
    if cb_codes:
        out = []
        for cb in cb_codes:
            r = cur.execute('SELECT cb_code, stock_code, conv_price, eff_date FROM issued WHERE cb_code=?', (cb,)).fetchone()
            if r: out.append(r)
        return out

    cur.execute('''SELECT u.cb_code, u.stock_code, i.conv_price, i.eff_date
                   FROM upcoming_auctions u LEFT JOIN issued i ON i.cb_code = u.cb_code
                   WHERE u.is_cancelled = 0''')
    rows = list(cur.fetchall())
    six_mo_ago = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
    cur.execute('''SELECT cb_code, stock_code, conv_price, eff_date FROM issued
                   WHERE eff_date IS NOT NULL AND substr(eff_date,1,10) >= ?
                   AND (conv_price IS NULL OR conv_price = 0)''', (six_mo_ago,))
    rows.extend(cur.fetchall())
    # 也補「定價日期」缺的 (conv_price 已有但 fm_conv_price_set_date 沒抓的)
    # 用 months_window (跟 --months 一致) 而非寫死 6 個月,讓 --months 24 真的能搜近 24 月生效的案
    if also_missing_dates:
        win_ago = (datetime.now() - timedelta(days=30*months_window)).strftime('%Y-%m-%d')
        cur.execute('''SELECT cb_code, stock_code, conv_price, eff_date FROM issued
                       WHERE eff_date IS NOT NULL AND substr(eff_date,1,10) >= ?
                         AND conv_price IS NOT NULL AND conv_price > 0
                         AND (fm_conv_price_set_date IS NULL OR fm_conv_price_set_date = '')''',
                    (win_ago,))
        rows.extend(cur.fetchall())
    seen = set(); out = []
    for cb, sc, cp, ef in rows:
        if cb in seen: continue
        seen.add(cb)
        if not force and cp and cp > 0 and not also_missing_dates: continue
        out.append((cb, sc, cp, ef))
    return out


def update_db(conn, cb_code, conv_price, source, mops_date, set_date=None, anchor_date=None):
    cur = conn.cursor()
    # 不一定要更新 conv_price (若僅補日期欄位也呼叫此函數)
    if conv_price is not None:
        cur.execute('''UPDATE issued SET conv_price=?,
                       fm_conv_price_set_date=COALESCE(?, fm_conv_price_set_date),
                       fm_conv_price_anchor_date=COALESCE(?, fm_conv_price_anchor_date),
                       note=COALESCE(note,'')||?, updated_at=?
                       WHERE cb_code=?''',
                    (conv_price, set_date, anchor_date,
                     f' [conv_price auto-filled from MOPS {mops_date} ({source}) @ {datetime.now():%Y-%m-%d %H:%M}]',
                     datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                     cb_code))
    else:
        cur.execute('''UPDATE issued SET
                       fm_conv_price_set_date=COALESCE(?, fm_conv_price_set_date),
                       fm_conv_price_anchor_date=COALESCE(?, fm_conv_price_anchor_date),
                       updated_at=?
                       WHERE cb_code=?''',
                    (set_date, anchor_date,
                     datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                     cb_code))
    conn.commit()
    return cur.rowcount


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('cb_codes', nargs='*')
    ap.add_argument('--force', action='store_true')
    ap.add_argument('--months', type=int, default=4, help='查最近 N 個月 MOPS (default 4)')
    ap.add_argument('--also-dates', action='store_true', help='也補 conv_price 有但定價日期沒抓的案件')
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    targets = get_targets(conn, force=args.force, cb_codes=args.cb_codes or None,
                          also_missing_dates=args.also_dates,
                          months_window=args.months)
    print(f'處理 {len(targets)} 檔 CB')

    session = requests.Session()
    session.headers['User-Agent'] = 'Mozilla/5.0'

    today = datetime.now()
    ok, miss = 0, 0
    for row in targets:
        cb_code, stock_code, current_cp, eff_date = row[0], row[1], row[2], (row[3] or '')[:10]
        print(f'\n=== {cb_code} (股 {stock_code}) — current={current_cp} eff={eff_date} ===')
        if not stock_code: print('  跳過: 缺 stock_code'); continue

        target_seq = cb_code_seq(cb_code)

        # 掃近 N 個月 MOPS
        found_cp = None; found_source = None; found_date = None
        found_set_date = None; found_anchor_date = None
        for mo_off in range(args.months):
            dt = (today.replace(day=15) - timedelta(days=30*mo_off))
            yr_roc, mo = dt.year - 1911, dt.month
            items = query_mops_list(session, stock_code, yr_roc, mo)
            time.sleep(0.4)
            for it in items:
                title = it['title']
                # 條件: 標題符合「轉換價格...溢價率」(訂定) 或 「董事會...決議發行...轉換公司債」(暫定)
                is_definite = bool(PAT_CONV_PRICE_TITLE.search(title))
                is_board = bool(PAT_BOARD_DECISION.search(title))
                if not (is_definite or is_board): continue
                # 「第 N 次」校驗 — 若 title 有序號要跟 cb_code 末碼對得上
                seqs = parse_cb_seqs(title)
                if target_seq and seqs and target_seq not in seqs: continue
                # 抓 detail body parse conv_price
                body = fetch_mops_detail(session, it)
                time.sleep(0.3)
                cp, hint = parse_body_conv_price(body)
                if cp is None: continue
                # 順便抓「定價時間」(只對 definite 公告有意義 — 暫定公告通常還沒定基準日)
                set_d, anchor_d = (None, None)
                if 'definite' in hint:
                    set_d, anchor_d = parse_body_pricing_dates(body)
                # 「訂定」優於「暫定」: 訂定就 break,暫定先暫存繼續找看有沒有更新的訂定
                if 'definite' in hint:
                    found_cp, found_source, found_date = cp, hint, it['date']
                    found_set_date, found_anchor_date = set_d, anchor_d
                    break
                elif not found_cp:
                    found_cp, found_source, found_date = cp, hint, it['date']
            if found_cp and 'definite' in (found_source or ''): break

        if found_cp is None:
            print('  ❌ MOPS 找不到 conv_price 公告')
            miss += 1; continue

        date_info = ''
        if found_set_date:
            date_info += f' 公告日={found_set_date}'
        if found_anchor_date:
            date_info += f' 基準日={found_anchor_date}'
        print(f'  ✓ MOPS {found_date}: {found_cp} 元 ({found_source}){date_info}')
        if current_cp and abs(current_cp - found_cp) > 0.01:
            print(f'  [WARN] DB={current_cp} vs MOPS={found_cp} — {"覆寫" if args.force else "保留 DB"}')
            if not args.force:
                # 仍 update 日期 (即使保留 conv_price)
                if found_set_date or found_anchor_date:
                    update_db(conn, cb_code, None, None, None, found_set_date, found_anchor_date)
                    print(f'  → 仍更新定價日期欄位')
                continue
        n = update_db(conn, cb_code, found_cp, found_source, found_date, found_set_date, found_anchor_date)
        if n: ok += 1

    print(f'\n=== DONE: ✓{ok} / ❌{miss} ===')
    print('→ 跑 build_html.py + publish_cb.py 上線')


if __name__ == '__main__':
    main()
