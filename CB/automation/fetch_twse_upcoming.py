#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""從 TWSE 官方公告抓「即將開標」CB 競拍清單。

來源: https://www.twse.com.tw/rwd/zh/announcement/auction?response=json&yy=YYYY
判斷「即將開標」: col[23] (得標加權平均價) = 0/'-' (尚未開標)

寫入 SQLite 表 `upcoming_auctions`:
  cb_code, company, stock_code, auction_date, bid_start, bid_end,
  amount_lots, min_bid_price, lead_mgr, listing_date, market, etc.

issued 表同步 (2026-10-26 改):TWSE 是競拍日程的【權威來源】,在途案的投標起迄/撥券日
  以前只填空值 → 颱風順延、公會改期後 DB 永遠留第一次看到的舊日期 (安國一 開標 07-10→07-13、
  撥券 07-21→07-22;生合一 撥券 10-29→10-30),掛牌 watch 叫人在錯的那天掛買。
  現在:在途案 (還沒有 CB 首日成交) 的日期跟 TWSE 不同就覆寫,寫 last_status_note「撥券 10-29→10-30 (TWSE 改期)」。
  已開標的案也同步撥券日 (以前只同步得標價,開標後才改的撥券日進不來)。
  已掛牌的舊案不動。

執行: py -3.12 fetch_twse_upcoming.py
"""
import io
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests

import db as _dbm                  # adopt_official_code:正式代號對正 (2026-10-05 光譜 53814)

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / 'cb_data.db'
URL = 'https://www.twse.com.tw/rwd/zh/announcement/auction'


def init_table(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS upcoming_auctions (
        cb_code        TEXT PRIMARY KEY,
        company        TEXT,
        stock_code     TEXT,
        auction_date   TEXT,    -- 開標日期 YYYY-MM-DD
        bid_start      TEXT,    -- 投標開始日 YYYY-MM-DD
        bid_end        TEXT,    -- 投標結束日 YYYY-MM-DD
        amount_lots    INTEGER, -- 競拍張數
        min_bid_price  REAL,    -- 最低投標價
        margin_pct     REAL,    -- 保證金成數%
        lead_mgr       TEXT,    -- 主辦券商
        listing_date   TEXT,    -- 撥券/上市日 YYYY-MM-DD
        market         TEXT,    -- 發行市場 (上市/上櫃/興櫃/公發)
        nature         TEXT,    -- 發行性質
        method         TEXT,    -- 競拍方式
        is_cancelled   INTEGER, -- 取消旗標 1=流標/取消, 0=正常
        updated_at     TEXT
    )''')
    conn.commit()


def roc_to_iso(s: str) -> str:
    """民國日期 'YYYY/MM/DD' → ISO 'YYYY-MM-DD' (這個 API 已是西元，直接 / → -)"""
    if not s or s in ('-', '—', ''):
        return ''
    return str(s).replace('/', '-')


def to_int(s) -> int | None:
    if s in (None, '', '-', '—'): return None
    try:
        return int(str(s).replace(',', ''))
    except: return None


def to_float(s) -> float | None:
    if s in (None, '', '-', '—'): return None
    try:
        return float(str(s).replace(',', ''))
    except: return None


def fetch_year_all(yy: int) -> tuple[list[dict], list[dict]]:
    """抓某年 (西元) TWSE 競拍公告。回傳 (upcoming, opened) 兩 list。
    upcoming: 還沒開標 (加權平均=0) → 寫 upcoming_auctions
    opened: 已開標 (加權平均>0) → 同步回 auctions 表（修正 stale 加權）+ 在途案撥券日"""
    try:
        r = requests.get(URL, params={'response':'json','yy':str(yy)}, timeout=30)
        r.raise_for_status()
        d = r.json()
    except Exception as e:
        print(f'  [ERR] yy={yy}: {e}')
        return [], []
    data = d.get('data', [])
    upcoming, opened = [], []
    for row in data:
        nature = str(row[5] if len(row) > 5 else '').strip()
        if '轉換公司債' not in nature:
            continue
        avg_price_raw = row[23] if len(row) > 23 else ''
        is_upcoming = avg_price_raw in ('0', '0.00', '0.0', 0, '-', '—', '', None)
        cancelled_flag = row[25] if len(row) > 25 else ''
        is_cancelled = 1 if cancelled_flag and cancelled_flag not in ('-', '—', '') else 0
        if not is_upcoming:
            # 已開標：收集到 opened (所有 TWSE 欄位)
            opened.append({
                'cb_code':         str(row[3]).strip(),
                'company':         str(row[2]).strip(),
                'auction_date':    roc_to_iso(row[1]),
                'bid_start':       roc_to_iso(row[7]),
                'bid_end':         roc_to_iso(row[8]),
                'listing_date':    roc_to_iso(row[15]),   # 撥券日 — 開標後 TWSE 改期 (颱風順延) 只會出現在這裡
                'auction_lots':    to_int(row[9]),
                'min_bid_pct':     to_float(row[10]),
                'total_award_amt': to_float(row[17]),
                'total_valid':     to_int(row[19]),
                'valid_lots':      to_int(row[20]),
                'min_award_pct':   to_float(row[21]),
                'max_award_pct':   to_float(row[22]),
                # row[23] 得標加權平均價格 → weighted_avg / avg_award_pct / actual_price 三欄同值
                # row[24] 實際承銷價格 (公開申購價) → underwriting_price
                # 舊版把 row[24] 寫進 actual_price,聯上六 等 6 檔 avgWinPrice 因此顯示 100.0 而非 100.28 (2026-10-02 修)
                'weighted_avg':    to_float(row[23]),
                'avg_award_pct':   to_float(row[23]),
                'actual_price':    to_float(row[23]),
                'underwriting_price': to_float(row[24]),
                'is_cancelled':    is_cancelled,
            })
            continue
        upcoming.append({
            'cb_code':       str(row[3]).strip(),
            'company':       str(row[2]).strip(),
            'stock_code':    str(row[3])[:4] if len(str(row[3])) >= 4 else '',
            'auction_date':  roc_to_iso(row[1]),
            'bid_start':     roc_to_iso(row[7]),
            'bid_end':       roc_to_iso(row[8]),
            'amount_lots':   to_int(row[9]),
            'min_bid_price': to_float(row[10]),
            'margin_pct':    to_float(row[13]),
            'lead_mgr':      str(row[16]).strip(),
            'listing_date':  roc_to_iso(row[15]),
            'market':        str(row[4]).strip(),
            'nature':        str(row[5]).strip(),
            'method':        str(row[6]).strip(),
            'is_cancelled':  is_cancelled,
        })
    return upcoming, opened


def _ensure_auction_cols(conn):
    """auctions.conv_price / underwriting_price 欄位 (2026-10-02 語意修正)。
    優先用 db.ensure_auction_cols;雲端 aurora 若沒帶 db.py 就 inline 補,兩邊 DB 都不會因缺欄位炸掉。"""
    try:
        sys.path.insert(0, str(BASE_DIR))
        from db import ensure_auction_cols
        return ensure_auction_cols(conn)
    except Exception:
        existing = {r[1] for r in conn.execute('PRAGMA table_info(auctions)').fetchall()}
        for col in ('conv_price', 'underwriting_price'):
            if col not in existing:
                conn.execute(f'ALTER TABLE auctions ADD COLUMN {col} REAL')
        conn.commit()


def _d10(v):
    return str(v or '').strip()[:10]


def _empty(v):
    s = _d10(v)
    return s == '' or s == '未定'


def sync_issued_dates(c, cb, bid_start, bid_end, listing, now_str, today_str, source='TWSE', cancelled=False):
    """把 TWSE 的投標起迄/撥券日同步到 issued。回 (changed, notes)。

    規則 (2026-10-26):
      - 空的一律填 (原本就這樣)。
      - 在途案 (沒有 CB 首日成交、掛牌日沒過) 的值跟 TWSE 不同 → 覆寫 + last_status_note 留痕
        (「撥券 10-29→10-30 (TWSE 改期)」),這就是颱風順延/公會改期進 DB 的唯一路徑。
      - 已掛牌的舊案 (有首日成交或掛牌日已過 3 天以上) 不動。
      - 取消/流標 (is_cancelled) 的不同步日期。
    """
    if cancelled:
        return False, []
    row = c.execute('''SELECT fm_bid_start_date, fm_bid_end_date, listing_date, bid_period,
                              fm_cb_first_close, fm_cb_first_date FROM issued WHERE cb_code=?''', (cb,)).fetchone()
    if not row:
        return False, []
    cur_bs, cur_be, cur_ld, cur_bp, first_close, first_date = row
    ld10 = _d10(cur_ld)
    cutoff = (datetime.strptime(today_str, '%Y-%m-%d') - timedelta(days=3)).strftime('%Y-%m-%d')
    in_flight = (first_close is None) and _empty(first_date) and (_empty(cur_ld) or ld10 >= cutoff)
    sets, vals, notes = [], [], []
    bs_fin, be_fin = _d10(cur_bs), _d10(cur_be)
    bid_changed = False
    for fld, new, cur, label in (('fm_bid_start_date', bid_start, cur_bs, '投標起'),
                                 ('fm_bid_end_date', bid_end, cur_be, '投標迄')):
        new = _d10(new)
        if not new:
            continue
        if _empty(cur):
            sets.append(f'{fld}=?'); vals.append(new); bid_changed = True
        elif _d10(cur) != new and in_flight:
            sets.append(f'{fld}=?'); vals.append(new); bid_changed = True
            notes.append(f'{label} {_d10(cur)[5:]}→{new[5:]} ({source} 改期)')
        else:
            continue
        if fld == 'fm_bid_start_date':
            bs_fin = new
        else:
            be_fin = new
    new_ld = _d10(listing)
    if new_ld:
        if _empty(cur_ld):
            sets.append('listing_date=?'); vals.append(new_ld + ' 00:00:00')
        elif ld10 != new_ld and in_flight:
            sets.append('listing_date=?'); vals.append(new_ld + ' 00:00:00')
            notes.append(f'撥券 {ld10[5:]}→{new_ld[5:]} ({source} 改期)')
    # bid_period 是畫面顯示的「競拍/詢圈期間」字串:日期有動就跟著換,不然卡片還是舊區間
    if bs_fin and be_fin and (bid_changed or _empty(cur_bp) or str(cur_bp).strip() in ('競拍', '詢圈')):
        bp = f'{bs_fin}~{be_fin}'
        if str(cur_bp or '').strip() != bp:
            sets.append('bid_period=?'); vals.append(bp)
    if not sets:
        return False, []
    if notes:
        sets.append('last_status_update=?'); vals.append(now_str)
        sets.append('last_status_note=?'); vals.append(' / '.join(notes))
    sets.append('updated_at=?'); vals.append(now_str)
    vals.append(cb)
    c.execute(f'UPDATE issued SET {", ".join(sets)} WHERE cb_code=?', vals)
    return True, notes


def main():
    conn = sqlite3.connect(str(DB_PATH))
    init_table(conn)
    _ensure_auction_cols(conn)
    c = conn.cursor()

    now = datetime.now()
    years = [now.year]
    if now.month >= 11:
        years.append(now.year + 1)
    # 也抓上一年 (回填可能 stale 的開標結果)
    years.append(now.year - 1)

    print(f'從 TWSE 抓 競拍公告 (西元年: {sorted(set(years))})')
    all_upcoming = []
    all_opened = []
    any_year_ok = False
    for yy in sorted(set(years)):
        try:
            upcoming, opened = fetch_year_all(yy)
            any_year_ok = True
        except Exception as e:
            print(f'  yy={yy}: FETCH FAILED ({e}) — skip year')
            continue
        print(f'  yy={yy}: {len(upcoming)} 筆即將開標 / {len(opened)} 筆已開標')
        all_upcoming.extend(upcoming)
        all_opened.extend(opened)

    # Defensive: TWSE 全失敗或回傳全 0 → 保留舊資料,不要 DELETE
    # (GHA 雲端 IP 被 TWSE 擋的常見情境;以前直接清空害頁面整段消失)
    if not any_year_ok:
        print('[ABORT] TWSE 所有年份都 fetch 失敗 → 保留現有 upcoming_auctions 不變')
        conn.close()
        return
    if not all_upcoming and not all_opened:
        existing = c.execute('SELECT COUNT(*) FROM upcoming_auctions').fetchone()[0]
        if existing > 0:
            print(f'[ABORT] TWSE 回傳 0 筆但 DB 已有 {existing} 筆 → 視為 TWSE 假空,保留現有')
            conn.close()
            return

    # 去重 (跨年 bucket 重複)
    all_upcoming = list({r['cb_code']: r for r in all_upcoming}.values())
    all_opened = list({r['cb_code']: r for r in all_opened}.values())

    # 寫入 upcoming_auctions (清空重寫)
    c.execute('DELETE FROM upcoming_auctions')
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    today_str = now.strftime('%Y-%m-%d')
    issued_synced = 0
    revised = []
    for r in all_upcoming:
        c.execute('''INSERT OR REPLACE INTO upcoming_auctions
            (cb_code, company, stock_code, auction_date, bid_start, bid_end,
             amount_lots, min_bid_price, margin_pct, lead_mgr, listing_date,
             market, nature, method, is_cancelled, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (r['cb_code'], r['company'], r['stock_code'], r['auction_date'],
             r['bid_start'], r['bid_end'], r['amount_lots'], r['min_bid_price'],
             r['margin_pct'], r['lead_mgr'], r['listing_date'],
             r['market'], r['nature'], r['method'], r['is_cancelled'], now_str))
        # 把 bid/listing 同步回 issued 表 (timeline 渲染靠這些欄位,空著就退化成預估)。
        # TWSE 即將開標給的是【正式代號】:同公司在途案掛著推導來的暫定代號 (如 53814 光譜四) 就改成正式的;
        #   正式代號若被早就掛牌的舊債占著 (櫃買真的重發舊號) → 舊債封存再接手。見 db.adopt_official_code。
        _dbm.adopt_official_code(c, r['cb_code'], r['stock_code'], r['bid_start'] or r['auction_date'],
                                 company=r.get('company') or None, source='TWSE 即將開標')
        changed, notes = sync_issued_dates(c, r['cb_code'], r['bid_start'], r['bid_end'], r['listing_date'],
                                           now_str, today_str, cancelled=bool(r['is_cancelled']))
        if changed:
            issued_synced += 1
        if notes:
            revised.append((r['cb_code'], r['company'], ' / '.join(notes)))

    # 同步 opened 到 auctions 表 (修正 stale 任何欄位)
    sync_count = 0
    date_sync = 0
    sync_fields = ['auction_lots','min_bid_pct','total_award_amt','total_valid','valid_lots',
                   'min_award_pct','max_award_pct','weighted_avg','avg_award_pct','actual_price',
                   'underwriting_price']
    for op in all_opened:
        # 2026-10-26:開標後 TWSE 改撥券日 (颱風順延) 只會出現在 opened 列 → 在途案的 issued 撥券日/auctions 日期一起同步
        if not op['is_cancelled']:
            changed, notes = sync_issued_dates(c, op['cb_code'], op['bid_start'], op['bid_end'], op['listing_date'],
                                               now_str, today_str)
            if notes:
                revised.append((op['cb_code'], op['company'], ' / '.join(notes)))
        cur = c.execute(f"SELECT {','.join(sync_fields)}, auction_date, listing_date FROM auctions WHERE cb_code=?",
                        (op['cb_code'],))
        r = cur.fetchone()
        if not r:
            continue  # 還沒進 auctions 表 (twse_scraper 會處理)
        db_vals = dict(zip(sync_fields, r[:len(sync_fields)]))
        db_auc, db_ld = r[len(sync_fields)], r[len(sync_fields) + 1]
        # auctions 的開標日/撥券日跟 TWSE 不同 (安國一 開標 07-10 颱風 → 07-13) → 以 TWSE 為準
        dsets, dvals, dnotes = [], [], []
        for fld, new, cur_v, label in (('auction_date', op['auction_date'], db_auc, '開標'),
                                       ('listing_date', op['listing_date'], db_ld, '撥券')):
            if new and _d10(cur_v) != new and not op['is_cancelled']:
                dsets.append(f'{fld}=?'); dvals.append(new); dnotes.append(f'{label} {_d10(cur_v) or "空"}→{new}')
        if dsets:
            dsets.append('updated_at=?'); dvals.append(now_str); dvals.append(op['cb_code'])
            c.execute(f"UPDATE auctions SET {', '.join(dsets)} WHERE cb_code=?", dvals)
            date_sync += 1
            if date_sync <= 10:
                print(f'  日期 {op["cb_code"]} {op["company"]}: {", ".join(dnotes)}')
        # 任一欄位有差就 update 全部 (確保一致)
        diff_fields = []
        for f in sync_fields:
            tv = op[f]
            dv = db_vals[f] or 0
            if tv is None: continue
            tol = max(0.5, abs(tv) * 0.001)  # 0.1% 容差或 0.5
            if abs(dv - tv) > tol:
                diff_fields.append(f'{f}({dv}→{tv})')
        if not diff_fields:
            continue
        sets = ', '.join(f'{f}=?' for f in sync_fields) + ', updated_at=?'
        vals = [op[f] for f in sync_fields] + [now_str, op['cb_code']]
        c.execute(f"UPDATE auctions SET {sets} WHERE cb_code=?", vals)
        sync_count += 1
        if sync_count <= 10:
            print(f'  sync {op["cb_code"]} {op["company"]}: {", ".join(diff_fields[:3])}')
    conn.commit()

    print()
    print('=== 結果 ===')
    print(f'  共寫入 {len(all_upcoming)} 筆 upcoming_auctions')
    print(f'  同步 {issued_synced} 筆到 issued 表 (bid/listing 真的有改才算)')
    print(f'  同步修正 {sync_count} 筆 auctions (TWSE 加權平均更新),{date_sync} 筆 auctions 日期')
    if revised:
        print(f'  ⚠ 官方改期已覆寫 {len(revised)} 筆 (颱風順延/公會改期):')
        for cb, co, note in revised:
            print(f'     {cb} {co}: {note}')
    # 列出所有 (按開標日排序)
    print()
    print('=== 即將開標清單 ===')
    for r in sorted(all_upcoming, key=lambda x: x['auction_date']):
        flag = ' [取消]' if r['is_cancelled'] else ''
        print(f'  {r["auction_date"]}  {r["cb_code"]:6} {r["company"]:14} 投標={r["bid_start"]}~{r["bid_end"]} 額={r["amount_lots"]:>6} 主辦={r["lead_mgr"]} 撥券={r["listing_date"]}{flag}')

    conn.close()


if __name__ == '__main__':
    main()
