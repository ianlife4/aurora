#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""抓「個股價差 + CB 首日溢價」分析資料 → UPDATE issued

對 issued 表中所有有 listing_date 的案件：
  1. stock_code fallback：issued.stock_code 為空時用 cb_code 前 4 碼
  2. 解析 bid_period 文字 → fm_bid_start_date, fm_bid_end_date
  3. 抓 stock OHLC 全期間 (eff - 60 ~ listing + 5)
  4. fm_eff_close = eff_date 對應交易日 close
  5. fm_listing_stk_close = listing_date 對應交易日 close
  6. fm_stock_rally_pct = (listing - eff) / eff × 100
  7. fm_pre_bid_rally_pct = (bid_start - 30d 到 bid_start) 個股漲幅
  8. 抓 CB Daily listing_date 後 30 天的第一筆 close → fm_cb_first_close, fm_cb_first_date
  9. fm_cb_vs100 = fm_cb_first_close - 100
  10. UPDATE issued

執行：py -3.12 fetch_premium_rally.py [--all] [--limit N]
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

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / 'cb_data.db'
TOKEN_PATH = BASE_DIR / 'finmind_token.txt'

API_URL = 'https://api.finmindtrade.com/api/v4/data'
MAX_WORKERS = 8
REQUEST_TIMEOUT = 25


def load_token() -> str:
    # 雲端 (GitHub Actions) 用 FINMIND_TOKEN env var;本機用 finmind_token.txt
    import os
    tok = os.environ.get('FINMIND_TOKEN', '').strip()
    if tok:
        return tok
    return TOKEN_PATH.read_text(encoding='utf-8').strip()


def normalize_date(s) -> str | None:
    """各種格式 → 'YYYY-MM-DD'。"""
    if not s:
        return None
    raw = str(s).strip().split(' ')[0].split('T')[0].replace('/', '-')
    parts = raw.split('-')
    if len(parts) != 3:
        return None
    try:
        y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
        if y < 1900 or y > 2100:
            return None
        return f'{y:04d}-{m:02d}-{d:02d}'
    except ValueError:
        return None


# ── bid_period 文字解析 ──────────────────────────────────────────────────────
# 格式樣本：
#   "4/14~4/15"           — 標準
#   "3/27-3/31 競拍"      — 帶後綴
#   "8/16-8/17"           — - 連字
#   "8/9-8-11"            — 髒：M/D-M-D (第二個 - 是日期分隔)
#   "10/29-10-30"         — 髒：同上
#   "11/14-11-16"         — 髒：同上
#   "11/12/11/13"         — / 串接（異常但常見）
#   "12/2/12/3"           — / 串接
#   "競拍" / "詢圈"       — 沒日期
#   "延長募集..."          — 狀態文字
PAT_DASH    = re.compile(r'(\d{1,2})/(\d{1,2})\s*[~\-]\s*(\d{1,2})/(\d{1,2})')  # M/D~M/D 或 M/D-M/D
PAT_DASHED  = re.compile(r'(\d{1,2})/(\d{1,2})\s*-\s*(\d{1,2})\s*-\s*(\d{1,2})') # M/D-M-D (mail 髒格式)
PAT_SHORT   = re.compile(r'(\d{1,2})/(\d{1,2})\s*[~\-]\s*(\d{1,2})(?!\d|/|-)')   # M/D~D
PAT_4SLASH  = re.compile(r'(\d{1,2})/(\d{1,2})/(\d{1,2})/(\d{1,2})')             # M/D/M/D


def parse_bid_period(bid_period: str, listing_date_str: str | None) -> tuple[str | None, str | None]:
    """解析 bid_period 文字 → (start_date, end_date) 'YYYY-MM-DD' 或 (None, None)。

    用 listing_date 推年份：bid_period 必定在 listing_date 之前 ≤ 4 個月（一般 1-2 個月）。
    先試 listing_date 同年，若解析出的 bid_start > listing_date，則年份 -1。

    Sanity: 解析出的 end - start 必須在 0 ~ 10 天間，否則視為解析失敗。"""
    if not bid_period or not listing_date_str:
        return (None, None)
    txt = str(bid_period).strip()
    # 修正：把連續斜線壓成單斜線（"2//22-2/26" → "2/22-2/26"）
    txt = re.sub(r'/{2,}', '/', txt)

    # 優先嘗試髒格式 "M/D-M-D" → 視為 M/D~M/D
    m = PAT_DASHED.search(txt)
    if m:
        m1, d1, m2, d2 = map(int, m.groups())
    else:
        m = PAT_DASH.search(txt) or PAT_4SLASH.search(txt)
        if m:
            m1, d1, m2, d2 = map(int, m.groups())
        else:
            m = PAT_SHORT.search(txt)
            if m:
                m1, d1, d2 = int(m.group(1)), int(m.group(2)), int(m.group(3))
                m2 = m1
            else:
                return (None, None)

    if not (1 <= m1 <= 12 and 1 <= d1 <= 31 and 1 <= m2 <= 12 and 1 <= d2 <= 31):
        return (None, None)

    listing_year = int(listing_date_str.split('-')[0])
    # 用 listing_year 試
    for try_year in (listing_year, listing_year - 1):
        try:
            start_dt = datetime(try_year, m1, d1)
            end_year = try_year if m2 >= m1 else try_year + 1  # 跨年（如 12/29~1/3）
            end_dt   = datetime(end_year, m2, d2)
        except ValueError:
            continue
        # Sanity: end >= start, 且 end - start ≤ 10 天
        if end_dt < start_dt:
            continue
        if (end_dt - start_dt).days > 10:
            continue
        start = start_dt.strftime('%Y-%m-%d')
        end   = end_dt.strftime('%Y-%m-%d')
        # bid_start 必須 ≤ listing_date
        if start <= listing_date_str:
            return (start, end)
    return (None, None)


# ── stock_code fallback ──────────────────────────────────────────────────────

def derive_stock_code(cb_code: str, issued_stock: str) -> str:
    """issued.stock_code 為空時用 cb_code 前 4 碼。"""
    s = (issued_stock or '').strip()
    if s:
        return s
    cb = (cb_code or '').strip()
    if len(cb) >= 4 and cb[:4].isdigit():
        return cb[:4]
    return ''


# ── FinMind API ──────────────────────────────────────────────────────────────

def fetch_stock(session, stock_code: str, start: str, end: str, token: str) -> list[dict]:
    try:
        r = session.get(API_URL, params={
            'dataset': 'TaiwanStockPrice', 'data_id': stock_code,
            'start_date': start, 'end_date': end,
        }, headers={'Authorization': f'Bearer {token}'}, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        j = r.json()
    except Exception as e:
        return []
    if j.get('status') != 200:
        return []
    return sorted([x for x in (j.get('data') or [])
                   if x.get('close') is not None and x['close'] > 0],
                  key=lambda x: x['date'])


def fetch_cb_first(session, cb_code: str, listing_date: str, token: str) -> dict | None:
    try:
        # end_date 用「listing+45 天」或「今天」中較晚者 — 涵蓋「listing 是 eff 但實際 listing 拖很久」的 case
        listing_dt = datetime.strptime(listing_date, '%Y-%m-%d')
        end_dt = max(listing_dt + timedelta(days=45), datetime.now())
        end = end_dt.strftime('%Y-%m-%d')
        r = session.get(API_URL, params={
            'dataset': 'TaiwanStockConvertibleBondDaily', 'data_id': cb_code,
            'start_date': listing_date, 'end_date': end,
        }, headers={'Authorization': f'Bearer {token}'}, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        j = r.json()
    except Exception:
        return None
    if j.get('status') != 200:
        return None
    rows = sorted([x for x in (j.get('data') or [])
                   if x.get('close') and x['close'] > 0],
                  key=lambda x: x['date'])
    return rows[0] if rows else None


def close_at_or_before(rows: list[dict], target_date: str, max_offset: int = 8) -> tuple[float, str] | None:
    """找 target_date 當天，若是假日則往前找 max_offset 天內最近的交易日。
    回傳 (close, actual_date)。"""
    target = datetime.strptime(target_date, '%Y-%m-%d')
    for offset in range(0, max_offset + 1):
        d = (target - timedelta(days=offset)).strftime('%Y-%m-%d')
        for x in rows:
            if x['date'] == d:
                return (x['close'], d)
    return None


# ── 主流程 ────────────────────────────────────────────────────────────────────

def get_targets(force_all: bool, limit: int | None, in_progress: bool = False) -> list[dict]:
    today = datetime.now().strftime('%Y-%m-%d')
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    # --in-progress：每日刷新用,只抓「走勢還在動」的小集合 (待上市 + 近 45 天剛上市 + 已公告未掛牌),
    # 不重抓 ~900 檔已掛牌歷史 (它們 CB 首日已定、走勢過時也不影響決策)
    if in_progress:
        days45 = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
        d180 = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
        rows = conn.execute('''
            SELECT cb_code, company, stock_code, eff_date, bid_period, listing_date, method,
                   fm_premium_updated_at, fm_cb_first_close, fm_board_decision_date,
                   fm_stock_chart_json
            FROM issued
            WHERE stock_code IS NOT NULL AND stock_code != ''
              AND (is_legacy IS NULL OR is_legacy != 1)
              AND (
                (substr(listing_date,1,10) > ?
                 AND (fm_board_decision_date IS NOT NULL OR (eff_date IS NOT NULL AND eff_date != '')))
                OR
                (listing_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*'
                 AND substr(listing_date,1,10) BETWEEN ? AND ?)
                OR
                ((listing_date IS NULL OR listing_date = '' OR listing_date = '未定')
                 AND fm_cb_first_close IS NULL AND fm_board_decision_date >= ?)
              )
            ORDER BY listing_date DESC
        ''', (today, days45, today, d180)).fetchall()
        conn.close()
        targets = [dict(r) for r in rows]
        return targets[:limit] if limit else targets
    # --all：強制抓全部 (含已掛牌、未掛牌、in-progress)
    if force_all:
        rows = conn.execute('''
            SELECT cb_code, company, stock_code, eff_date, bid_period, listing_date, method,
                   fm_premium_updated_at, fm_cb_first_close, fm_board_decision_date,
                   fm_stock_chart_json
            FROM issued
            WHERE stock_code IS NOT NULL AND stock_code != ''
            ORDER BY listing_date DESC
        ''').fetchall()
    else:
        # 否則用條件過濾，三種 case：
        #   (a) listing_date 在過去（已知上市）
        #   (b) listing_date 空 / '未定' 但缺 fm_cb_first_close → 可能 mail 漏日期，探測 FinMind
        #   (c) 未掛牌進行中 (有 board 或 eff 且缺 chart) → 抓個股走勢
        #   (d) listing_date 在「未來」(進行中) → 重新抓 (走勢可能延伸到 today)
        rows = conn.execute('''
            SELECT cb_code, company, stock_code, eff_date, bid_period, listing_date, method,
                   fm_premium_updated_at, fm_cb_first_close, fm_board_decision_date,
                   fm_stock_chart_json
            FROM issued
            WHERE stock_code IS NOT NULL AND stock_code != ''
              AND (is_legacy IS NULL OR is_legacy != 1)  -- 跳過手動補建的歷史 CB (保留 chart 不被覆寫)
              AND (
                (listing_date IS NOT NULL AND listing_date != '' AND listing_date != '未定'
                 AND substr(listing_date, 1, 10) <= ?)
                OR
                ((listing_date IS NULL OR listing_date = '' OR listing_date = '未定')
                 AND fm_cb_first_close IS NULL)
                OR
                (substr(listing_date,1,10) > ?
                 AND (fm_board_decision_date IS NOT NULL
                      OR (eff_date IS NOT NULL AND eff_date != '')))
              )
            ORDER BY listing_date DESC
        ''', (today, today)).fetchall()
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


def fetch_upcoming_dates(cb_code: str) -> dict:
    """從 upcoming_auctions 表撈 TWSE 官方公告日期 (bid_start / bid_end / listing_date / auction_date)。
    Mail 沒解析到日期 (如 bid_period='競拍' 或 listing='未定') 時的權威 fallback。"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        r = conn.execute(
            'SELECT bid_start, bid_end, listing_date, auction_date FROM upcoming_auctions WHERE cb_code=? AND (is_cancelled IS NULL OR is_cancelled=0)',
            (cb_code,)
        ).fetchone()
        conn.close()
        if not r:
            return {}
        return {
            'bid_start':    r['bid_start'],
            'bid_end':      r['bid_end'],
            'listing_date': r['listing_date'],
            'auction_date': r['auction_date'],
        }
    except Exception:
        return {}


def process_one(t: dict, token: str) -> tuple[str, str, dict]:
    cb = t['cb_code']
    listing = normalize_date(t.get('listing_date'))
    eff = normalize_date(t.get('eff_date'))
    board = normalize_date(t.get('fm_board_decision_date'))
    stock_code = derive_stock_code(cb, t.get('stock_code'))
    fields: dict = {'fm_premium_updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

    # ── TWSE upcoming_auctions fallback ────────────────────────────────────
    # Mail 沒解析出 bid_period/listing_date 時 (如 bid_period='競拍'、listing='未定'),
    # 用 TWSE 官方公告補上 → 30 天拉抬 (fm_pre_bid_rally_pct) 才能計算
    twse = fetch_upcoming_dates(cb)
    twse_bid_start = twse.get('bid_start')
    twse_bid_end = twse.get('bid_end')
    if not listing and twse.get('listing_date'):
        listing = twse['listing_date']  # 注意:這檔還沒真正上市,但用 TWSE 預定日繼續走主流程

    # 沒 listing_date → 用 eff_date 當 FinMind 搜尋起點（元富 mail 沒給 listing 但 CB 可能已上市）
    # 也沒 eff → 用 1 年前
    fallback_listing = listing
    if not fallback_listing:
        if eff:
            fallback_listing = eff
        else:
            fallback_listing = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

    if not stock_code:
        # 至少抓 CB 首日 close
        sess = requests.Session()
        cb_first = fetch_cb_first(sess, cb, fallback_listing, token)
        if cb_first:
            fields['fm_cb_first_close'] = cb_first['close']
            fields['fm_cb_first_date']  = cb_first['date']
            fields['fm_cb_vs100']       = round(cb_first['close'] - 100, 2)
        return (cb, 'cb_only', fields)
    # 真正主邏輯仍需要 listing — 沒原始 listing 但有 FinMind 資料時，用 fm_cb_first_date 當 listing 替代
    if not listing:
        sess = requests.Session()
        cb_first = fetch_cb_first(sess, cb, fallback_listing, token)
        if cb_first:
            fields['fm_cb_first_close'] = cb_first['close']
            fields['fm_cb_first_date']  = cb_first['date']
            fields['fm_cb_vs100']       = round(cb_first['close'] - 100, 2)
            listing = cb_first['date']  # 用 CB 首日當 listing 繼續走後面 stock OHLC 邏輯
        else:
            # 進行中案件 (還沒掛牌)：若有 board 或 eff，仍抓個股走勢到「今天 + 5 天」
            # 用 board 或 eff 較早的當 chart 起點，這樣完整看到從決議到目前的走勢
            anchors = [d for d in [board, eff] if d]
            anchor = min(anchors) if anchors else None
            if anchor:
                anchor_dt = datetime.strptime(anchor, '%Y-%m-%d')
                # 從 anchor 前 30 天到「今天 + 5 天」
                range_start = (anchor_dt - timedelta(days=30)).strftime('%Y-%m-%d')
                range_end = (datetime.now() + timedelta(days=5)).strftime('%Y-%m-%d')
                stocks_draft = fetch_stock(sess, stock_code, range_start, range_end, token)
                if stocks_draft:
                    # eff_close (若有 eff)
                    if eff:
                        r = close_at_or_before(stocks_draft, eff)
                        if r:
                            fields['fm_eff_close'], fields['fm_eff_close_date'] = r
                    # 個股走勢 JSON [anchor-7d, today+5]（anchor=最早的 milestone）
                    try:
                        chart_start_dt = anchor_dt - timedelta(days=7)
                        chart_end_dt = datetime.now() + timedelta(days=5)
                        chart_rows = [s for s in stocks_draft
                                      if chart_start_dt <= datetime.strptime(s['date'], '%Y-%m-%d') <= chart_end_dt]
                        if chart_rows:
                            import json as _json
                            compact = [{'d': s['date'], 'c': s['close'], 'h': s.get('max'), 'l': s.get('min')}
                                       for s in chart_rows]
                            fields['fm_stock_chart_json'] = _json.dumps(compact, separators=(',', ':'))
                    except Exception:
                        pass
                return (cb, 'draft_chart', fields)
            return (cb, 'no_listing', fields)

    # 解析 bid_period（用 listing_date 推年份，更穩）
    bid_start, bid_end = parse_bid_period(t.get('bid_period') or '', listing)
    # TWSE 公告 優先 (mail 解析不到日期時的權威來源)
    if twse_bid_start: bid_start = twse_bid_start
    if twse_bid_end:   bid_end   = twse_bid_end
    if bid_start: fields['fm_bid_start_date'] = bid_start
    if bid_end:   fields['fm_bid_end_date']   = bid_end

    # 抓股票 OHLC（從「最早的 milestone」前 30 天 → listing 後 N 天）
    # 最早通常是 board (董事會決議)，比 eff 早 ~30 天
    earliest_anchors = [d for d in [board, bid_start, eff, listing] if d]
    range_start_ref = min(earliest_anchors) if earliest_anchors else listing
    range_start = (datetime.strptime(range_start_ref, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
    # range_end：
    #   - 近 6 個月內掛牌：抓到「今天 + 5 天」(走勢還新)
    #   - 較舊：抓到「掛牌 + 30 天」(看上市後 1 個月走勢)
    listing_dt = datetime.strptime(listing, '%Y-%m-%d')
    days_since_listing = (datetime.now() - listing_dt).days
    if days_since_listing <= 180 and days_since_listing >= 0:
        range_end = (datetime.now() + timedelta(days=5)).strftime('%Y-%m-%d')
    else:
        range_end = (listing_dt + timedelta(days=30)).strftime('%Y-%m-%d')

    sess = requests.Session()
    stocks = fetch_stock(sess, stock_code, range_start, range_end, token)

    if stocks:
        # eff_close
        if eff:
            r = close_at_or_before(stocks, eff)
            if r:
                fields['fm_eff_close'], fields['fm_eff_close_date'] = r
        # listing_stk_close
        r = close_at_or_before(stocks, listing)
        if r:
            fields['fm_listing_stk_close'], fields['fm_listing_stk_date'] = r
        # stock_rally
        if 'fm_eff_close' in fields and 'fm_listing_stk_close' in fields and fields['fm_eff_close']:
            fields['fm_stock_rally_pct'] = round(
                (fields['fm_listing_stk_close'] / fields['fm_eff_close'] - 1) * 100, 2)
        # pre_bid_rally
        if bid_start:
            r_start = close_at_or_before(stocks, bid_start)
            ref_30d = (datetime.strptime(bid_start, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
            r_30d = close_at_or_before(stocks, ref_30d)
            if r_start and r_30d and r_30d[0]:
                fields['fm_pre_bid_rally_pct'] = round(
                    (r_start[0] / r_30d[0] - 1) * 100, 2)
        # 個股走勢 JSON [最早 anchor - 7d, listing+30d 或 today+5]
        # - 近 6 個月內掛牌：延伸到 today (走勢還新)
        # - 較舊：延伸到 listing+30d (看上市後 1 個月走勢)
        try:
            chart_anchors = [d for d in [board, eff, fields.get('fm_eff_close_date'), listing] if d]
            chart_anchor = min(chart_anchors) if chart_anchors else listing
            chart_start_dt = datetime.strptime(chart_anchor, '%Y-%m-%d') - timedelta(days=7)
            if days_since_listing <= 180 and days_since_listing >= 0:
                chart_end_dt = datetime.now() + timedelta(days=5)
            else:
                chart_end_dt = datetime.strptime(listing, '%Y-%m-%d') + timedelta(days=30)
            chart_rows = [s for s in stocks
                          if chart_start_dt <= datetime.strptime(s['date'], '%Y-%m-%d') <= chart_end_dt]
            if chart_rows:
                import json as _json
                compact = [{'d': s['date'], 'c': s['close'], 'h': s.get('max'), 'l': s.get('min')}
                           for s in chart_rows]
                fields['fm_stock_chart_json'] = _json.dumps(compact, separators=(',', ':'))
        except Exception:
            pass

    # 抓 CB 首日
    cb_first = fetch_cb_first(sess, cb, listing, token)
    if cb_first:
        fields['fm_cb_first_close'] = cb_first['close']
        fields['fm_cb_first_date']  = cb_first['date']
        fields['fm_cb_vs100']       = round(cb_first['close'] - 100, 2)

    return (cb, 'ok', fields)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--all', action='store_true', help='強制重抓全部')
    parser.add_argument('--in-progress', dest='in_progress', action='store_true',
                        help='每日刷新用: 只抓待上市+近45天剛上市+已公告未掛牌的小集合')
    parser.add_argument('--limit', type=int, help='只抓前 N 筆 (debug)')
    args = parser.parse_args()

    token = load_token()
    targets = get_targets(args.all, args.limit, in_progress=args.in_progress)
    mode = 'in-progress (走勢還在動)' if args.in_progress else ('全部' if args.all else '已上市+進行中')
    print(f'準備抓 {len(targets)} 筆 issued [{mode}]')

    t0 = time.time()
    counters = {'ok': 0, 'cb_only': 0, 'no_listing': 0, 'draft_chart': 0, 'updated': 0}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process_one, t, token): t for t in targets}
        for i, fut in enumerate(as_completed(futures), 1):
            try:
                cb, status, fields = fut.result()
            except Exception as e:
                print(f'  [ERR] {e}')
                continue
            counters[status] = counters.get(status, 0) + 1
            if update_db(cb, fields):
                counters['updated'] += 1
            if i % 80 == 0:
                print(f'  進度 {i}/{len(targets)}  ok={counters["ok"]} cb_only={counters["cb_only"]}')

    elapsed = time.time() - t0
    print()
    print('=== 結果 ===')
    print(f'  完整成功 (ok)       : {counters["ok"]}')
    print(f'  只 CB 首日 (cb_only): {counters["cb_only"]}')
    print(f'  進行中含走勢 (draft): {counters["draft_chart"]}')
    print(f'  無 listing_date     : {counters["no_listing"]}')
    print(f'  DB updated          : {counters["updated"]}')
    print(f'  耗時                : {elapsed:.1f}s')


if __name__ == '__main__':
    main()
