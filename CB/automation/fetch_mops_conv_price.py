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
  - 三來源 fallback 鏈 (便宜快的先): MOPS 重大訊息 → FinMind DailyOverview → B05 生效 PDF
    a) FinMind: 已掛牌交易的 CB,首筆 = 掛牌轉換價
    b) B05 公司債生效 (發行價格確認版) PDF: 生效定價後、掛牌前的權威來源,
       抓「本轉換公司債發行時之轉換價格為每股新臺幣 X 元」(全文僅一處,需 NFKC 正規化)

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
FINMIND_URL = 'https://api.finmindtrade.com/api/v4/data'
FINMIND_TOKEN_PATH = HERE / 'finmind_token.txt'
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


def load_finmind_token():
    # 雲端 (GitHub Actions) 用 FINMIND_TOKEN env var;本機用 finmind_token.txt
    import os
    tok = os.environ.get('FINMIND_TOKEN', '').strip()
    if tok:
        return tok
    try:
        return FINMIND_TOKEN_PATH.read_text(encoding='utf-8').strip()
    except OSError:
        return ''



def fetch_finmind_conv_price(session, cb_code, eff_date, token):
    """已掛牌交易的 CB,從 FinMind DailyOverview 取掛牌初始轉換價 (元/股)。
       用於 MOPS 沒有對應「訂定轉換價格」公告格式的案 (如部分有擔保競拍/海外 E 型)。
       eff_date 當 start_date 排除「代號重用」的舊債;eff_date 缺時拉寬到 2005,
       再以 >365 天斷層切出最近一段連續交易 (單一檔 CB 不可能有逾年斷層),
       取該段首筆 = 本檔初始轉換價。回傳 (conv_price, date) 或 (None, None)。"""
    if not token:
        return None, None
    start = (eff_date or '')[:10]
    if not re.match(r'\d{4}-\d{2}-\d{2}$', start):
        start = '2005-01-01'
    try:
        r = session.get(FINMIND_URL, params={
            'dataset': 'TaiwanStockConvertibleBondDailyOverview',
            'data_id': cb_code, 'start_date': start, 'token': token,
        }, timeout=TIMEOUT, verify=False)
        data = r.json().get('data') or []
    except Exception:
        return None, None
    if not data:
        return None, None
    data.sort(key=lambda x: x.get('date') or '')
    block_start = 0
    for i in range(len(data) - 1, 0, -1):
        try:
            gap = (datetime.strptime(data[i]['date'], '%Y-%m-%d')
                   - datetime.strptime(data[i-1]['date'], '%Y-%m-%d')).days
        except (ValueError, KeyError, TypeError):
            gap = 0
        if gap > 365:
            block_start = i
            break
    first = data[block_start]
    try:
        cp = float(first.get('ConversionPrice'))
    except (TypeError, ValueError):
        return None, None
    return (cp, first.get('date')) if 0.01 < cp < 100000 else (None, None)


# ── B05 公司債生效 PDF (發行價格確認版) ──────────────────────────────
# 權威定價版,生效後上傳;掛牌前 (FinMind 尚無資料) 即可取得。
# 「本轉換公司債(發行時)之轉換價格為每股新臺幣 X 元」— 「本轉換公司債」主詞鎖定本檔,
# 排除前次/同業比較與試算列 (那些用「訂定/計算得出/前次」等不同措辭)。「發行時」可有可無。
B05_CONV_PRICE_PAT = re.compile(rf'本轉換公司債(?:發行時)?之轉換價格為每股{NTD}\s*([\d,]+(?:\.\d+)?)\s*元')
B05_BASIS_DATE_PAT = re.compile(r'民國\s*([0-9]+)\s*年\s*([0-9]+)\s*月\s*([0-9]+)\s*日為[^。]{0,20}基準日')


def _ym_month_dist(fn_ym, eff_iso):
    """B05 檔名 yyyymm 與 eff_date 的月份距離 — 同股多檔 CB 時對到正確那檔"""
    try:
        y1, m1 = int(fn_ym[:4]), int(fn_ym[4:6])
        y2, m2 = int(eff_iso[:4]), int(eff_iso[5:7])
        return abs((y1 * 12 + m1) - (y2 * 12 + m2))
    except (ValueError, TypeError):
        return 999


def parse_b05_pdf_conv_price(pdf_path):
    """逐頁掃 B05 生效 PDF,命中定價句即停 (不解析剩餘數百頁)。
       PDF 常在中文字間斷行,先壓掉空白再比對 (否則「本轉換公司債發行\\n時之…」會漏)。
       只認帶「本轉換公司債」主詞 + 「為」(非「訂為」) 的句,避開前次/同業比較的他檔價。
       回傳 (conv_price, 基準日 ISO) 或 (None, None)。"""
    try:
        import pdfplumber
        import unicodedata
        with pdfplumber.open(str(pdf_path)) as pdf:
            for pg in pdf.pages:
                # NFKC 正規化: TWSE PDF 字型常用 CJK 相容字 (如 行=U+FA08),
                # 不正規化會比對不到標準字 (行=U+884C)
                raw = unicodedata.normalize('NFKC', pg.extract_text() or '')
                tc = re.sub(r'\s+', '', raw)
                if '之轉換價格為每股' not in tc:
                    continue
                m = B05_CONV_PRICE_PAT.search(tc)
                if not m:
                    continue
                try:
                    cp = float(m.group(1).replace(',', ''))
                except ValueError:
                    continue
                if not (0.01 < cp < 100000):
                    continue
                bm = B05_BASIS_DATE_PAT.search(tc)
                return cp, (roc_to_ad(*bm.groups()) if bm else None)
    except Exception:
        return None, None
    return None, None


def fetch_b05_conv_price(stock_code, eff_date):
    """抓 stock 的 B05 公司債生效 PDF (發行價格確認版) 解析轉換價。
       同股多檔 CB → 依檔名 yyyymm 對 eff_date 最近者挑選 (差 >6 月不認)。
       30 天內抓過的 B05 不重抓。回傳 (conv_price, 基準日) 或 (None, None)。"""
    if not stock_code:
        return None, None
    try:
        import fetch_prospectus_pdf as fp
        items = fp.list_prospectuses(stock_code, fp._new_session())
    except Exception:
        return None, None
    b05 = [x for x in items if x.get('type_code') == 'B05' and x.get('status') == '生效']
    if not b05:
        return None, None
    eff = (eff_date or '')[:10]
    if re.match(r'\d{4}-\d{2}-\d{2}', eff):
        b05 = [x for x in b05 if _ym_month_dist(x['filename'][:6], eff) <= 6]
        if not b05:
            return None, None
        pick = min(b05, key=lambda x: _ym_month_dist(x['filename'][:6], eff))
    else:
        pick = max(b05, key=lambda x: x['filename'][:6])
    out_dir = Path(fp.DEFAULT_OUT_DIR)
    cached = sorted(out_dir.glob(f"{pick['filename'][:6]}_{stock_code}_B05*.pdf"),
                    key=lambda p: p.stat().st_mtime, reverse=True)
    if cached and (datetime.now().timestamp() - cached[0].stat().st_mtime) < 86400 * 30:
        return parse_b05_pdf_conv_price(cached[0])
    try:
        path = fp.fetch_latest_prospectus(stock_code, kind='B05', only_status='生效',
                                          filename=pick['filename'], out_dir=str(out_dir))
    except Exception:
        return None, None
    return parse_b05_pdf_conv_price(path)


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
    # 也補「進行中但 eff_date 空/舊」的案: 近 6 月才公告董事會、conv_price 空、非 legacy
    # (修補: 原本只用 eff_date 篩,漏掉 eff_date='' 的進行中案,如 52892 宜鼎二 — eff 沒填就永遠不被 targeting)
    cur.execute('''SELECT cb_code, stock_code, conv_price, eff_date FROM issued
                   WHERE (conv_price IS NULL OR conv_price = 0)
                     AND (is_legacy IS NULL OR is_legacy != 1)
                     AND fm_board_decision_date >= ?
                     AND (eff_date IS NULL OR eff_date = '' OR substr(eff_date,1,10) < ?)''',
                (six_mo_ago, six_mo_ago))
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
        now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # 順便標記「狀態更新」→ HTML 已發行列表把近期填到轉換價的浮頂 + 🆕 badge
        cur.execute('''UPDATE issued SET conv_price=?,
                       fm_conv_price_set_date=COALESCE(?, fm_conv_price_set_date),
                       fm_conv_price_anchor_date=COALESCE(?, fm_conv_price_anchor_date),
                       last_status_update=?, last_status_note=?,
                       note=COALESCE(note,'')||?, updated_at=?
                       WHERE cb_code=?''',
                    (conv_price, set_date, anchor_date,
                     now_ts, f'轉換價 {conv_price}',
                     f' [conv_price auto-filled from {source} {mops_date} @ {datetime.now():%Y-%m-%d %H:%M}]',
                     now_ts,
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
    fm_token = load_finmind_token()

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

        # FinMind fallback: MOPS 無對應公告但 CB 已掛牌交易 → DailyOverview 有掛牌轉換價
        if found_cp is None:
            fm_cp, fm_date = fetch_finmind_conv_price(session, cb_code, eff_date, fm_token)
            if fm_cp is not None:
                found_cp, found_source, found_date = fm_cp, 'finmind-overview', fm_date
                print(f'  ↪ MOPS 無公告,改用 FinMind 掛牌轉換價 (首日 {fm_date})')

        # B05 公司債生效 PDF fallback: 生效定價後、掛牌前 (MOPS/FinMind 都還沒有時) 的權威來源
        if found_cp is None:
            b05_cp, b05_basis = fetch_b05_conv_price(stock_code, eff_date)
            if b05_cp is not None:
                found_cp, found_source, found_date = b05_cp, 'b05-pdf', (b05_basis or '')
                found_anchor_date = b05_basis or found_anchor_date
                print(f'  ↪ MOPS/FinMind 無,改用 B05 生效 PDF 定價 (基準日 {b05_basis})')

        if found_cp is None:
            print('  ❌ MOPS/FinMind/B05 都找不到 conv_price (尚未生效定價)')
            miss += 1; continue

        date_info = ''
        if found_set_date:
            date_info += f' 公告日={found_set_date}'
        if found_anchor_date:
            date_info += f' 基準日={found_anchor_date}'
        print(f'  ✓ {found_date}: {found_cp} 元 ({found_source}){date_info}')
        if current_cp and abs(current_cp - found_cp) > 0.01:
            print(f'  [WARN] DB={current_cp} vs {found_source}={found_cp} — {"覆寫" if args.force else "保留 DB"}')
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
