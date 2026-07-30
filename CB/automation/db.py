#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite 資料庫操作
Tables: issued（已發行1）, auctions（CB競拍結果）, stocks（個股）
"""
import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / 'cb_data.db'


def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.executescript('''
    CREATE TABLE IF NOT EXISTS issued (
        cb_code      TEXT PRIMARY KEY,
        company      TEXT,
        tcri         TEXT,
        method       TEXT,
        amount       REAL,
        term         TEXT,
        underwriter  TEXT,
        issue_price  TEXT,
        conv_price   REAL,
        conv_value   TEXT,
        put_cond     TEXT,
        receipt_date TEXT,
        eff_date     TEXT,
        bid_period   TEXT,
        listing_date TEXT,
        capital      REAL,
        note         TEXT,
        stock_code   TEXT,
        updated_at   TEXT
    );

    CREATE TABLE IF NOT EXISTS auctions (
        cb_code         TEXT PRIMARY KEY,
        serial          INTEGER,
        auction_date    TEXT,
        company         TEXT,
        stock_code      TEXT,
        issue_amount    REAL,
        auction_amount  REAL,
        term            TEXT,
        guarantee       TEXT,
        tcri            TEXT,
        auction_lots    REAL,
        min_bid_pct     REAL,
        bid_date        TEXT,
        weighted_avg    REAL,
        min_award_pct   REAL,
        max_award_pct   REAL,
        avg_award_pct   REAL,
        listing_date    TEXT,
        lead_mgr        TEXT,
        total_award_amt REAL,
        total_valid     REAL,
        valid_lots      REAL,
        actual_price    REAL,
        close_price     REAL,
        theory_price    REAL,
        post_high       REAL,
        post_low        REAL,
        low_premium     REAL,
        high_premium    REAL,
        avg_premium     REAL,
        updated_at      TEXT
    );

    CREATE TABLE IF NOT EXISTS stocks (
        stock_code   TEXT PRIMARY KEY,
        company      TEXT,
        industry     TEXT,
        sub_industry TEXT,
        biz_desc     TEXT,
        capital      REAL,
        related      TEXT,
        stock_type   TEXT,
        ky           TEXT,
        updated_at   TEXT
    );

    CREATE TABLE IF NOT EXISTS meta (
        key   TEXT PRIMARY KEY,
        value TEXT
    );
    ''')

    # Migration: add new columns to auctions if they don't exist (for existing DBs)
    existing_cols = {row[1] for row in c.execute('PRAGMA table_info(auctions)').fetchall()}
    new_cols = [
        ('close_price',  'REAL'),
        ('theory_price', 'REAL'),
        ('post_high',    'REAL'),
        ('post_low',     'REAL'),
        ('low_premium',  'REAL'),
        ('high_premium', 'REAL'),
        ('avg_premium',  'REAL'),
        # 撥券上市那天的 CB 收盤價 + 與最低拍價的差距（潛在最低拍獲利率）
        ('listing_close',       'REAL'),
        ('listing_low_premium', 'REAL'),
        ('listing_market',      'TEXT'),  # 'TPEx' / 'TWSE' / '' (來源)
        # FinMind 二級市場資料 (TaiwanStockConvertibleBondDaily)
        ('fm_latest_date',     'TEXT'),     # 最新一筆有效成交日期 'YYYY-MM-DD'
        ('fm_latest_close',    'REAL'),     # 最新 CB 收盤價
        ('fm_latest_change',   'REAL'),     # 最新漲跌
        ('fm_latest_volume',   'REAL'),     # 最新成交張數
        ('fm_latest_turnover', 'REAL'),     # 最新成交金額
        ('fm_all_high',        'REAL'),     # 上市以來最高 close
        ('fm_all_low',         'REAL'),     # 上市以來最低 close
        ('fm_30d_high',        'REAL'),     # 近 30 個交易日最高
        ('fm_30d_low',         'REAL'),     # 近 30 個交易日最低
        ('fm_trade_days',      'INTEGER'),  # 上市以來有成交日數
        ('fm_updated_at',      'TEXT'),     # 抓取時間戳
    ]
    for col, typ in new_cols:
        if col not in existing_cols:
            c.execute(f'ALTER TABLE auctions ADD COLUMN {col} {typ}')

    # Migration: issued 表加「撤回/廢止」標記
    issued_cols_check = {row[1] for row in c.execute('PRAGMA table_info(issued)').fetchall()}
    for col, typ in [('is_withdrawn', 'INTEGER'), ('withdrawn_note', 'TEXT')]:
        if col not in issued_cols_check:
            c.execute(f'ALTER TABLE issued ADD COLUMN {col} {typ}')

    # Migration: issued 表加「個股漲幅 + CB 首日溢價」分析欄位
    issued_cols = {row[1] for row in c.execute('PRAGMA table_info(issued)').fetchall()}
    issued_new_cols = [
        ('fm_eff_close',          'REAL'),     # eff_date 那天標的股 close
        ('fm_eff_close_date',     'TEXT'),     # 實際對應的交易日（eff_date 可能是假日）
        ('fm_listing_stk_close',  'REAL'),     # listing_date 那天標的股 close
        ('fm_listing_stk_date',   'TEXT'),     # 實際對應的交易日
        ('fm_stock_rally_pct',    'REAL'),     # (listing - eff) / eff × 100  —— 個股價差%
        # 從 bid_period 文字解析的詢圈/競拍實際時間 + 拉抬指標
        ('fm_bid_start_date',     'TEXT'),     # 詢圈/競拍期間開始 (從 bid_period 解析)
        ('fm_bid_end_date',       'TEXT'),     # 詢圈/競拍期間結束
        ('fm_pre_bid_rally_pct',  'REAL'),     # bid_start 前 30 天到 bid_start 個股漲幅 (拉抬指標)
        ('fm_cb_first_close',     'REAL'),     # CB 上市後第一筆有成交的 close
        ('fm_cb_first_date',      'TEXT'),     # CB 第一筆成交日（≠ listing_date 因為可能掛牌當天無成交）
        ('fm_cb_vs100',           'REAL'),     # fm_cb_first_close - 100
        ('fm_premium_updated_at', 'TEXT'),     # 抓取時間戳
        ('fm_stock_chart_json',   'TEXT'),     # 個股走勢 JSON: [{d:'YYYY-MM-DD', c:close, h:high, l:low}]
                                                # 範圍 [eff_date - 7d, listing_date + 7d]，給 sparkline 用
        # MOPS 重大訊息抓取的里程碑日期
        ('fm_board_decision_date', 'TEXT'),    # 董事會決議發行 CB 公告日 (從 MOPS)
        ('fm_account_setup_date',  'TEXT'),    # 簽訂存儲專戶/代收價款專戶 公告日 (從 MOPS)
        ('fm_mops_updated_at',     'TEXT'),    # MOPS 抓取時間
    ]
    for col, typ in issued_new_cols:
        if col not in issued_cols:
            c.execute(f'ALTER TABLE issued ADD COLUMN {col} {typ}')

    # Migration: issued 表加「公開說明書分析」欄位 (markdown 全文 + 時間戳)
    issued_cols = {row[1] for row in c.execute('PRAGMA table_info(issued)').fetchall()}
    for col, typ in [('analysis_md', 'TEXT'), ('analysis_updated_at', 'TEXT')]:
        if col not in issued_cols:
            c.execute(f'ALTER TABLE issued ADD COLUMN {col} {typ}')

    # Migration (2026-05-11): is_legacy 標記 → 保護手動補建的歷史 CB 不被
    # fetch_premium_rally.py 預設覆寫 (例如 80961 擎亞一 2006-2009 完整走勢)
    issued_cols = {row[1] for row in c.execute('PRAGMA table_info(issued)').fetchall()}
    if 'is_legacy' not in issued_cols:
        c.execute('ALTER TABLE issued ADD COLUMN is_legacy INTEGER DEFAULT 0')

    # Migration (2026-05-11): prospectus_filename → 指定 B021 公開說明書檔名
    # (一家公司可能有多版 B021,例如擎亞 8096 有 200606/201808/201809,需精確配對 cb_code)
    issued_cols = {row[1] for row in c.execute('PRAGMA table_info(issued)').fetchall()}
    if 'prospectus_filename' not in issued_cols:
        c.execute('ALTER TABLE issued ADD COLUMN prospectus_filename TEXT')

    # Migration (2026-05-20): 狀態更新追蹤 → scan_cb_disclosures 偵測到新里程碑/轉換價時設,
    # HTML「已發行」列表把近 7 天有更新的浮到頂端 + 🆕 badge 顯示 note
    issued_cols = {row[1] for row in c.execute('PRAGMA table_info(issued)').fetchall()}
    for col, typ in [('last_status_update', 'TEXT'), ('last_status_note', 'TEXT')]:
        if col not in issued_cols:
            c.execute(f'ALTER TABLE issued ADD COLUMN {col} {typ}')

    # Migration (2026-07-30): 發債動機分類 → 清單篩選「只看純擴產型」+ 未來做分類績效統計
    #   motive_type   純擴產備料型 / 純還債型 / 財務體質修復型 / 併購型 / 混合型 / PDF未揭露用途
    #   repay_pct     資金用途中「償還借款」占比 (%)
    #   expand_pct    資金用途中「購料/營運/設備」占比 (%)
    #   repay_rate_lo 擬償還借款的最低利率 (%) — 🔴 關鍵:同樣是「純還債」,還 4.3% 高息是真省錢,
    #                 還 0.93% 土銀貸款省不了多少,真正目的多半是美化負債比/騰銀行額度
    #   motive_note   判定理由一句話
    issued_cols = {row[1] for row in c.execute('PRAGMA table_info(issued)').fetchall()}
    for col, typ in [('motive_type', 'TEXT'), ('repay_pct', 'REAL'), ('expand_pct', 'REAL'),
                     ('repay_rate_lo', 'REAL'), ('motive_note', 'TEXT')]:
        if col not in issued_cols:
            c.execute(f'ALTER TABLE issued ADD COLUMN {col} {typ}')

    conn.commit()
    conn.close()


# ── issued ────────────────────────────────────────────────────────────────────

_VALID_TERMS = {'1Y','2Y','3Y','4Y','5Y','6Y','7Y','2.5Y','3.5Y'}
_VALID_METHODS = {'詢圈','競拍'}

def _sanitize_value(db_col: str, v):
    """回傳清理後的值，若 v 對該欄位無效則回傳 None (不寫)。"""
    import re as _re
    if v is None:
        return None
    s = str(v).strip()
    if not s or s in ('nan','NaT','None','-','—','未公告','未定','待定'):
        return None
    if db_col == 'tcri':
        # 'TCRI5' / 'twAA+' OK; '5'/'6' → 'TCRI5'/'TCRI6'; 其他全部拒絕
        if _re.match(r'^TCRI[1-9]$', s) or _re.match(r'^tw[A-Za-z+\-]+$', s):
            return s
        if _re.match(r'^[1-9]$', s):
            return f'TCRI{s}'
        return None  # 拒絕「第一銀行」、「4Y」、日期等
    if db_col == 'method':
        if s in _VALID_METHODS:
            return s
        if '競拍' in s: return '競拍'
        if '詢圈' in s or '詢價' in s: return '詢圈'
        return None
    if db_col == 'term':
        if s in _VALID_TERMS:
            return s
        # 嘗試標準化：'3' → '3Y'
        if _re.match(r'^[1-7]$', s):
            return f'{s}Y'
        if _re.match(r'^\d+\.?\d*$', s):
            try:
                f = float(s)
                if 1 <= f <= 7:
                    return f'{int(f)}Y' if f == int(f) else f'{f}Y'
            except ValueError: pass
        return None  # 拒絕 '133Y', '1145Y', '未定' 等
    if db_col in ('eff_date', 'receipt_date', 'listing_date'):
        # 必須是有效日期格式 YYYY-MM-DD 或 'YYYY-MM-DD HH:MM:SS' 或包含「未定」
        # 自動把 / 轉成 - (避免字串比較 bug: '2024/07/12' > '2024-08-16')
        if s == '未定':
            return s
        if _re.match(r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}', s):
            return s.replace('/', '-')  # 標準化為 ISO
        return None  # 拒絕 '-', '—' 等
    return v


def upsert_issued(records: list[dict]) -> int:
    """新增或更新已發行 CB，回傳新增筆數。
    對現有列：只填空欄位（如後來 conv_price 才公告），不覆寫已填的內容。
    新增 sanity filter：不寫入髒資料 (異常 tcri/method/term/eff_date)。"""
    conn = get_conn()
    c = conn.cursor()
    added = 0
    updated = 0
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 名稱對映：dict key → DB column
    field_map = {
        'company':      'company',
        'tcri':         'tcri',
        'method':       'method',
        'amount':       'amount',
        'term':         'term',
        'underwriter':  'underwriter',
        'issue_price_raw': 'issue_price',
        'issue_price':  'issue_price',
        'conv_price':   'conv_price',
        'conv_value':   'conv_value',
        'premium_rate': 'conv_value',
        'put_cond':     'put_cond',
        'receipt_date': 'receipt_date',
        'eff_date':     'eff_date',
        'bid_period':   'bid_period',
        'listing_date': 'listing_date',
        'capital':      'capital',
        'note':         'note',
        'stock_code':   'stock_code',
        'is_withdrawn': 'is_withdrawn',     # 撤回標記（一旦標就跟著保留）
        'withdrawn_note': 'withdrawn_note', # 撤回原因
    }

    for r in records:
        code = str(r.get('cb_code', '')).strip()
        if not code:
            continue
        existing = c.execute('SELECT * FROM issued WHERE cb_code=?', (code,)).fetchone()
        if existing:
            # merge: 對每個欄位，若 DB 為 NULL/空字串/0 而新資料有值，就填進去
            ex = dict(existing)
            sets, vals = [], []
            for src_key, db_col in field_map.items():
                if src_key not in r:
                    continue
                new_v = r[src_key]
                if new_v is None or (isinstance(new_v, str) and not new_v.strip()):
                    continue
                # Sanity filter：拒絕髒資料寫入 DB
                clean_v = _sanitize_value(db_col, new_v)
                if clean_v is None:
                    continue
                new_v = clean_v
                old_v = ex.get(db_col)
                old_str = str(old_v).strip() if old_v is not None else ''
                # 只當舊值是空才覆蓋（含「未定/未公告/-/待定」這類佔位字串）
                is_empty = (old_v is None
                            or (isinstance(old_v, str) and not old_v.strip())
                            or old_str in ('未定', '未公告', '-', '待定', 'nan', 'NaT', 'None')
                            or (isinstance(old_v, (int, float)) and old_v == 0 and db_col in ('conv_price','amount','capital')))
                if is_empty:
                    sets.append(f'{db_col}=?')
                    vals.append(new_v)
            if sets:
                sets.append('updated_at=?')
                vals.append(now)
                vals.append(code)
                c.execute(f'UPDATE issued SET {", ".join(sets)} WHERE cb_code=?', vals)
                updated += 1
            continue
        # INSERT 時也用 sanitize (拒絕髒資料)
        def _v(src_key, db_col):
            raw = r.get(src_key)
            if raw is None or (isinstance(raw, str) and not raw.strip()):
                return ''
            cleaned = _sanitize_value(db_col, raw)
            return cleaned if cleaned is not None else ''
        c.execute('''
            INSERT INTO issued
              (cb_code, company, tcri, method, amount, term, underwriter,
               issue_price, conv_price, conv_value, put_cond,
               receipt_date, eff_date, bid_period, listing_date,
               capital, note, stock_code, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', (
            code,
            r.get('company', ''),
            _v('tcri', 'tcri'),
            _v('method', 'method'),
            r.get('amount'),
            _v('term', 'term'),
            r.get('underwriter', ''),
            r.get('issue_price_raw') or r.get('issue_price', ''),
            r.get('conv_price'),
            r.get('conv_value') or r.get('premium_rate', ''),
            r.get('put_cond', ''),
            _v('receipt_date', 'receipt_date'),
            _v('eff_date', 'eff_date'),
            r.get('bid_period', ''),
            _v('listing_date', 'listing_date'),
            r.get('capital'),
            r.get('note', ''),
            r.get('stock_code', ''),
            now,
        ))
        added += 1

    conn.commit()
    # 跑 logical sanity sweep: mail 帶錯的 eff_date 自動清掉
    cleaned = _cleanup_invalid_dates(c)
    conn.commit()
    conn.close()
    if updated:
        print(f'  upsert_issued: 補齊 {updated} 筆既有列的空欄位')
    if cleaned:
        print(f'  upsert_issued: 邏輯清理 {cleaned} 筆 (eff > listing 等)')
    return added


def _cleanup_invalid_dates(c) -> int:
    """清掉 mail 帶錯的 eff_date (eff > listing / eff > bid_start / eff = listing)。
    這些是 mail 年份打錯或填重複資料造成，自動清比讓 timeline 顯示錯誤好。"""
    total = 0
    # eff > listing → 邏輯不可能 (listing 必定在 eff 之後)
    c.execute("""UPDATE issued
        SET eff_date = '', fm_eff_close = NULL, fm_eff_close_date = NULL
        WHERE eff_date != '' AND eff_date IS NOT NULL
          AND listing_date NOT IN ('','未定') AND listing_date IS NOT NULL
          AND substr(eff_date,1,10) > substr(listing_date,1,10)""")
    total += c.rowcount
    # eff > bid_start → 邏輯不可能 (bid 必定在 eff 之後)
    c.execute("""UPDATE issued
        SET eff_date = '', fm_eff_close = NULL, fm_eff_close_date = NULL
        WHERE eff_date != '' AND eff_date IS NOT NULL
          AND fm_bid_start_date IS NOT NULL
          AND substr(eff_date,1,10) > fm_bid_start_date""")
    total += c.rowcount
    # eff = listing 同一天 → 邏輯不可能 (中間至少要有 ~28d 詢圈/競拍/繳款)
    c.execute("""UPDATE issued
        SET eff_date = '', fm_eff_close = NULL, fm_eff_close_date = NULL
        WHERE eff_date != '' AND eff_date IS NOT NULL
          AND substr(eff_date,1,10) = substr(listing_date,1,10)""")
    total += c.rowcount
    # board > eff → MOPS 配對到別次的 board，清空
    c.execute("""UPDATE issued
        SET fm_board_decision_date = NULL
        WHERE fm_board_decision_date IS NOT NULL
          AND eff_date != '' AND eff_date IS NOT NULL
          AND fm_board_decision_date > substr(eff_date,1,10)""")
    total += c.rowcount
    # account > listing → MOPS 配對到別次，清空
    c.execute("""UPDATE issued
        SET fm_account_setup_date = NULL
        WHERE fm_account_setup_date IS NOT NULL
          AND listing_date NOT IN ('','未定') AND listing_date IS NOT NULL
          AND fm_account_setup_date > substr(listing_date,1,10)""")
    total += c.rowcount
    return total


def get_issued(search: str = '', method: str = '', tcri: str = '') -> list[dict]:
    conn = get_conn()
    c = conn.cursor()
    sql = 'SELECT * FROM issued WHERE 1=1'
    params = []
    if search:
        sql += ' AND (cb_code LIKE ? OR company LIKE ? OR underwriter LIKE ? OR stock_code LIKE ?)'
        p = f'%{search}%'
        params += [p, p, p, p]
    if method:
        sql += ' AND method=?'
        params.append(method)
    if tcri:
        sql += ' AND tcri LIKE ?'
        params.append(f'%{tcri}%')
    sql += ' ORDER BY cb_code DESC'
    rows = c.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── auctions ──────────────────────────────────────────────────────────────────

def upsert_auction(records: list[dict]) -> int:
    conn = get_conn()
    c = conn.cursor()
    added = 0
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for r in records:
        code = str(r.get('cb_code', '')).strip()
        if not code:
            continue
        existing = c.execute('SELECT cb_code FROM auctions WHERE cb_code=?', (code,)).fetchone()
        if existing:
            continue
        c.execute('''
            INSERT INTO auctions
              (cb_code, serial, auction_date, company, stock_code,
               issue_amount, auction_amount, term, guarantee, tcri,
               auction_lots, min_bid_pct, bid_date,
               weighted_avg, min_award_pct, max_award_pct, avg_award_pct,
               listing_date, lead_mgr, total_award_amt, total_valid,
               valid_lots, actual_price, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', (
            code,
            r.get('serial'),
            r.get('open_date', '') or r.get('auction_date', ''),
            r.get('sec_name', '') or r.get('company', ''),
            r.get('stock_code', ''),
            r.get('auction_amount_yi'),
            r.get('auction_amount_yi'),
            r.get('term', ''),
            r.get('guarantee', ''),
            r.get('tcri', ''),
            r.get('auction_lots'),
            r.get('min_bid_price'),
            r.get('bid_start', ''),
            r.get('weighted_avg'),
            r.get('min_award'),
            r.get('max_award'),
            r.get('weighted_avg'),
            r.get('transfer_date', ''),
            r.get('lead_mgr', ''),
            r.get('total_award'),
            r.get('total_valid'),
            r.get('valid_lots'),
            r.get('actual_price'),
            now,
        ))
        added += 1

    conn.commit()
    conn.close()
    return added


def update_auction_prices(cb_code: str, fields: dict) -> bool:
    """Update price-related fields on an existing auction by cb_code.
    Only sets columns where the value is not None. Returns True if a row was updated."""
    if not cb_code or not fields:
        return False
    allowed = {'close_price','theory_price','post_high','post_low',
               'low_premium','high_premium','avg_premium','conv_price',
               'listing_close','listing_low_premium','listing_market',
               # FinMind 二級市場欄位
               'fm_latest_date','fm_latest_close','fm_latest_change',
               'fm_latest_volume','fm_latest_turnover',
               'fm_all_high','fm_all_low','fm_30d_high','fm_30d_low',
               'fm_trade_days','fm_updated_at'}
    cols, vals = [], []
    for k, v in fields.items():
        if k in allowed and v is not None:
            cols.append(f'{k}=?')
            vals.append(v)
    if not cols:
        return False
    conn = get_conn()
    c = conn.cursor()
    existing = c.execute('SELECT cb_code FROM auctions WHERE cb_code=?', (cb_code,)).fetchone()
    if not existing:
        conn.close()
        return False
    vals.append(cb_code)
    c.execute(f'UPDATE auctions SET {", ".join(cols)} WHERE cb_code=?', vals)
    conn.commit()
    conn.close()
    return True


def get_auctions_missing_listing_close() -> list[dict]:
    """有 listing_date 跟 lowWinPrice 但還沒抓到 listing_close 的 auctions。"""
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute('''
        SELECT cb_code, stock_code, listing_date, min_award_pct AS low_win_price
        FROM auctions
        WHERE listing_close IS NULL
          AND listing_date IS NOT NULL AND listing_date != '' AND listing_date != '未定'
          AND min_award_pct IS NOT NULL AND min_award_pct > 0
        ORDER BY listing_date DESC
    ''').fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_auctions_missing_close() -> list[dict]:
    """Auctions with bid_date set but close_price still NULL — candidates for TWSE backfill."""
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute('''
        SELECT cb_code, stock_code, bid_date, weighted_avg AS conv_price
        FROM auctions
        WHERE close_price IS NULL
          AND stock_code IS NOT NULL AND stock_code != ''
          AND bid_date IS NOT NULL AND bid_date != ''
    ''').fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_auctions(search: str = '', guarantee: str = '') -> list[dict]:
    conn = get_conn()
    c = conn.cursor()
    sql = 'SELECT * FROM auctions WHERE 1=1'
    params = []
    if search:
        sql += ' AND (cb_code LIKE ? OR company LIKE ? OR stock_code LIKE ? OR lead_mgr LIKE ?)'
        p = f'%{search}%'
        params += [p, p, p, p]
    if guarantee:
        sql += ' AND guarantee=?'
        params.append(guarantee)
    sql += ' ORDER BY auction_date DESC'
    rows = c.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── stocks ────────────────────────────────────────────────────────────────────

def upsert_stock(records: list[dict]) -> int:
    conn = get_conn()
    c = conn.cursor()
    added = 0
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for r in records:
        code = str(r.get('stock_code', '')).strip()
        if not code:
            continue
        existing = c.execute('SELECT stock_code FROM stocks WHERE stock_code=?', (code,)).fetchone()
        if existing:
            c.execute('''UPDATE stocks SET company=?, industry=?, sub_industry=?,
                         biz_desc=?, capital=?, related=?, stock_type=?, ky=?, updated_at=?
                         WHERE stock_code=?''',
                      (r.get('company',''), r.get('industry',''), r.get('sub_industry',''),
                       r.get('biz_desc',''), r.get('capital'), r.get('related',''),
                       r.get('stock_type',''), r.get('ky',''), now, code))
        else:
            c.execute('''INSERT INTO stocks
                         (stock_code, company, industry, sub_industry, biz_desc,
                          capital, related, stock_type, ky, updated_at)
                         VALUES (?,?,?,?,?,?,?,?,?,?)''',
                      (code, r.get('company',''), r.get('industry',''), r.get('sub_industry',''),
                       r.get('biz_desc',''), r.get('capital'), r.get('related',''),
                       r.get('stock_type',''), r.get('ky',''), now))
            added += 1

    conn.commit()
    conn.close()
    return added


def get_stocks(search: str = '', industry: str = '') -> list[dict]:
    conn = get_conn()
    c = conn.cursor()
    sql = 'SELECT * FROM stocks WHERE 1=1'
    params = []
    if search:
        sql += ' AND (stock_code LIKE ? OR company LIKE ? OR industry LIKE ? OR sub_industry LIKE ?)'
        p = f'%{search}%'
        params += [p, p, p, p]
    if industry:
        sql += ' AND industry LIKE ?'
        params.append(f'%{industry}%')
    sql += ' ORDER BY stock_code'
    rows = c.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── meta ──────────────────────────────────────────────────────────────────────

def set_meta(key: str, value: str):
    conn = get_conn()
    conn.execute('INSERT OR REPLACE INTO meta (key, value) VALUES (?,?)', (key, value))
    conn.commit()
    conn.close()


def get_meta(key: str) -> str:
    conn = get_conn()
    row = conn.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
    conn.close()
    return row['value'] if row else ''


def get_counts() -> dict:
    conn = get_conn()
    c = conn.cursor()
    issued   = c.execute('SELECT COUNT(*) FROM issued').fetchone()[0]
    auctions = c.execute('SELECT COUNT(*) FROM auctions').fetchone()[0]
    stocks   = c.execute('SELECT COUNT(*) FROM stocks').fetchone()[0]
    conn.close()
    return {'issued': issued, 'auctions': auctions, 'stocks': stocks}


if __name__ == '__main__':
    init_db()
    print(f'DB initialized at {DB_PATH}')
