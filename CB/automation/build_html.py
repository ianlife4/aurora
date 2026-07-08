#!/usr/bin/env python3
"""Build CB管理.html from cb_data.db.

讀 automation/cb_data.db → 寫 ../CB管理.html
重跑時機：Gmail/TWSE 抓到新資料、跑完 run_update.py 之後。

執行：
    py -3.12 build_html.py
"""
import sqlite3
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta


# 擔保銀行縮寫 → 標準全名（短名/異名統一化）
BANK_ALIAS = {
    # 公股
    '土銀':         '土地銀行',
    '台銀聯合':     '台灣銀行',
    '合庫銀':       '合作金庫',
    '合作金庫':     '合作金庫',
    '合庫銀行':     '合作金庫',
    '第一銀':       '第一銀行',
    '彰銀':         '彰化銀行',
    '彰化銀':       '彰化銀行',
    '華南銀':       '華南銀行',
    '兆豐銀':       '兆豐銀行',
    '台企銀':       '台灣企銀',
    '農業金庫':     '農業金庫',
    # 民營
    '台新銀':       '台新銀行',
    '永豐銀':       '永豐銀行',
    '玉山銀':       '玉山銀行',
    '板信銀':       '板信銀行',
    '安泰銀':       '安泰銀行',
    '新光銀':       '新光銀行',
    '上海銀':       '上海銀行',
    '上海商銀':     '上海銀行',
    '台中銀':       '台中銀行',
    '台中商銀':     '台中銀行',
    '陽信銀':       '陽信銀行',
    '遠東銀':       '遠東銀行',
    '中信銀':       '中國信託',
    '中國信託銀':   '中國信託',
    '中國信託':     '中國信託',
    '凱基銀':       '凱基銀行',
    '日盛銀行':     '日盛銀行',
    '台北富邦':     '富邦銀行',
    '富邦銀行':     '富邦銀行',
    '元大銀行':     '元大銀行',
    '盤谷銀行':     '盤谷銀行',
    '瑞穗銀行':     '瑞穗銀行',
}


def normalize_tcri(v):
    """統一信評/擔保銀行欄位：
    - 'TCRI5' / '5' → 'TCRI5'（範圍 1-10）
    - 銀行縮寫 → 對應全名（用 BANK_ALIAS）
    - 多家共擔「台新/永豐」→ 個別標準化後合併「台新銀行/永豐銀行」
    - 髒資料（純小數、超出 TCRI 範圍、特殊佔位字串）→ 空字串
    - 外部信評（twA+ / FitchBBB）→ 保留原樣
    """
    if v is None:
        return ''
    s = str(v).strip()
    if not s:
        return ''
    # 佔位字串
    if s in ('未定', '未公告', '-', '待定', 'nan', 'NaT', 'None', 'TCRI'):
        return ''
    # 已是 TCRIn → upper
    if re.match(r'^TCRI\d+$', s, re.IGNORECASE):
        n = int(re.sub(r'\D', '', s))
        return f'TCRI{n}' if 1 <= n <= 10 else ''
    # 純整數 → TCRIn (限合理範圍)
    if re.match(r'^\d+$', s):
        n = int(s)
        return f'TCRI{n}' if 1 <= n <= 10 else ''
    # 純小數（如 "62.5", "114.05"）→ 髒資料
    if re.match(r'^\d+\.\d+$', s):
        return ''
    # 多家共擔（含 /）— 對每段標準化，短名（如「中信」「新光」「遠東」）加「銀」再查 alias
    if '/' in s:
        parts = []
        for raw in s.split('/'):
            p = raw.strip()
            if not p:
                continue
            std = BANK_ALIAS.get(p)
            if not std:
                std = BANK_ALIAS.get(p + '銀', p)  # 「中信」→「中信銀」→「中國信託」
            parts.append(std)
        # 去重保序
        seen, uniq = set(), []
        for p in parts:
            if p not in seen:
                seen.add(p); uniq.append(p)
        return '/'.join(uniq)
    # 單一銀行名 → 查 alias 表，找不到原樣返回（如 "聯保"、"twA+"、"FitchBBB"）
    return BANK_ALIAS.get(s, s)


def normalize_term(v):
    """統一年期為 N+Y 格式（防禦性，避免 DB 還有髒資料）"""
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    # 已是 N+Y 格式：要再驗證 N 在合理範圍 (0.5 ~ 10)，否則視為髒資料
    # 例如元富 mail 解析錯把金額和年期串在一起 → "1145Y" / "133Y"
    m = re.match(r'^(\d+\.?\d*)Y$', s, re.IGNORECASE)
    if m:
        n = float(m.group(1))
        if 0.5 <= n <= 10:
            return s.upper()
        return None  # 超出範圍 → 髒資料
    if re.match(r'^\d+\.?\d*$', s):
        n = float(s)
        if 0.5 <= n <= 10:
            return f'{int(n) if n.is_integer() else n}Y'
        return None
    if '-' in s and ':' in s:
        return None
    return s

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, 'cb_data.db')
OUT_PATH = os.path.normpath(os.path.join(HERE, '..', 'CB管理.html'))


# ---- 標的名稱尾碼補齊 -----------------------------------------------------
# 早期階段 (董事會公告/預計發行) 的 xlsx 來源常常只給「青雲」「辛耘」這種公司名,
# 缺少「青雲二」「辛耘三」這種 CB 序號尾碼。這裡在 build 階段補齊 (DB 保留原樣)。
ZH_NUM = '一二三四五六七八九十'
ZH_TO_NUM = {z: i + 1 for i, z in enumerate(ZH_NUM)}


def _build_offset_map(rows):
    """掃所有 (cb_code, name) → {stock_code: 多數決 offset}.
    offset = cb_last_digit - bond_number (從名稱中第一個中文數字推).
    新光金家族 offset=+1 (28885 → 新光金四 ⇒ 5-4=1)。
    多數常見公司 offset=0 (35831 → 辛耘一 ⇒ 1-1=0)。
    """
    by_stock = defaultdict(list)
    for cb, name in rows:
        if not cb or len(cb) < 5 or not name:
            continue
        stk = cb[:4]
        m = re.search(f'[{ZH_NUM}]', name)
        if not m:
            continue
        bond_num = ZH_TO_NUM[m.group()]
        cb_last = int(cb[-1]) if cb[-1] != '0' else 10
        by_stock[stk].append(cb_last - bond_num)
    return {stk: Counter(offs).most_common(1)[0][0] for stk, offs in by_stock.items()}


def derive_bond_name(cb_code, current_name, offset_map):
    """若 current_name 沒有任何中文數字尾碼,推算並補上.
    KY 公司處理:「大略-KY」→「大略一-KY」、「特昇KY」→「特昇一KY」.
    其他:「青雲」→「青雲二」.
    """
    if not cb_code or len(cb_code) < 5 or not current_name:
        return current_name
    if any(z in current_name for z in ZH_NUM):
        return current_name  # 已有尾碼
    # 海外 CB 已有英數尾碼 (E1/E2/E1永/E2永 等) → 不再 append 中文數字
    if re.search(r'E\d+(永)?$|E\d+(永)?(-?KY)$', current_name):
        return current_name

    cb_last = int(cb_code[-1]) if cb_code[-1] != '0' else 10
    stk = cb_code[:4]
    offset = offset_map.get(stk, 0)
    bond_num = cb_last - offset
    if not (1 <= bond_num <= 10):
        bond_num = cb_last  # 越界 → 改用原始末位
        if not (1 <= bond_num <= 10):
            return current_name  # 仍越界放棄
    suffix = ZH_NUM[bond_num - 1]

    # KY 後綴處理 (插在 KY 之前)
    if current_name.endswith('-KY'):
        return current_name[:-3] + suffix + '-KY'
    if current_name.endswith('KY'):
        return current_name[:-2] + suffix + 'KY'
    return current_name + suffix
# ---------------------------------------------------------------------------


def fix_date(s):
    if not s:
        return None
    return str(s).split(' ')[0]


def try_float(v):
    if v is None or v == '':
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def compute_premium(price, theory):
    if price is None or theory is None or theory <= 0:
        return None
    return (price - theory) / theory


def load_data():
    if not os.path.exists(DB_PATH):
        sys.exit(f'ERR: db not found: {DB_PATH}')
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # ---- stocks ----
    stocks = {}
    cur.execute('SELECT * FROM stocks')
    for r in cur.fetchall():
        code = (r['stock_code'] or '').strip()
        if not code:
            continue
        stocks[code] = {
            'name': r['company'],
            'industry': r['industry'],
            'subIndustry': r['sub_industry'],
            'position': r['biz_desc'],
            'capital': r['capital'],
            'related': r['related'] or None,
            'alliance': None,
        }

    # ---- issuances (master of all CB ever issued) ----
    # 先掃一輪所有 (cb_code, company) 算 stock_code → offset map,給尾碼補齊用
    cur.execute('SELECT cb_code, company FROM issued WHERE (is_withdrawn IS NULL OR is_withdrawn=0)')
    issued_rows_for_offset = [(r['cb_code'], r['company']) for r in cur.fetchall()]
    cur.execute('SELECT cb_code, company FROM auctions')
    auction_rows_for_offset = [(r['cb_code'], r['company']) for r in cur.fetchall()]
    offset_map = _build_offset_map(issued_rows_for_offset + auction_rows_for_offset)

    # 先撈 TWSE upcoming_auctions 表 (權威來源: 競拍張數 + 擔保旗標)
    twse_lookup = {}
    try:
        cur.execute('SELECT cb_code, amount_lots, nature, min_bid_price, margin_pct, lead_mgr, bid_start, bid_end, auction_date, listing_date, is_cancelled FROM upcoming_auctions')
        for tr in cur.fetchall():
            nature = (tr['nature'] or '')
            if '有擔保' in nature:
                gflag = 'YES'
            elif '無擔保' in nature:
                gflag = 'NO'
            else:
                gflag = ''
            cancelled = bool(tr['is_cancelled'])  # 取消的競拍:保留張數/擔保 metadata,但不外洩日期 (避免被當有效開標/投標日)
            twse_lookup[tr['cb_code']] = {
                'twseLots':     tr['amount_lots'],
                'twseGuarantee': gflag,
                'twseMinBid':   tr['min_bid_price'],
                'twseMargin':   tr['margin_pct'],
                'twseLeadMgr':  tr['lead_mgr'],
                'twseBidStart': None if cancelled else tr['bid_start'],
                'twseBidEnd':   None if cancelled else tr['bid_end'],
                'twseAuctionDate': None if cancelled else tr['auction_date'],  # 開標/定價日 (公告得標)
                'twseListingDate': None if cancelled else tr['listing_date'],
            }
    except Exception as e:
        print(f'  [warn] upcoming_auctions table 讀取失敗 (跳過 TWSE 增益): {e}')

    # 從 auctions 表抓 cb_code → auction_date (歷史已開標案,給 issuance modal timeline 用)
    auction_dates_by_cb = {}
    try:
        cur.execute('SELECT cb_code, auction_date FROM auctions WHERE auction_date IS NOT NULL')
        for ar in cur.fetchall():
            d = fix_date(ar['auction_date'])
            if d:
                auction_dates_by_cb[ar['cb_code']] = d
    except Exception as e:
        print(f'  [warn] auctions.auction_date lookup 失敗: {e}')

    issuances = []
    issued_by_cb = {}  # 給 auctions 多欄位做 fallback
    cur.execute('SELECT * FROM issued')
    for r in cur.fetchall():
        cb = r['cb_code']
        if not cb:
            continue
        # 讀 fm_premium_rally 欄位（fetch_premium_rally.py 寫入）
        keys = r.keys()
        get = lambda k: (r[k] if k in keys else None)
        fm_pr = {
            'fmEffClose':       get('fm_eff_close'),
            'fmEffCloseDate':   get('fm_eff_close_date'),
            'fmListingStkClose': get('fm_listing_stk_close'),
            'fmListingStkDate': get('fm_listing_stk_date'),
            'fmStockRallyPct':  get('fm_stock_rally_pct'),
            'fmBidStartDate':   get('fm_bid_start_date'),
            'fmBidEndDate':     get('fm_bid_end_date'),
            'fmPreBidRallyPct': get('fm_pre_bid_rally_pct'),
            'fmCbFirstClose':   get('fm_cb_first_close'),
            'fmCbFirstDate':    get('fm_cb_first_date'),
            'fmCbVs100':        get('fm_cb_vs100'),
            # 個股走勢 JSON (給 sparkline + popup 走勢圖用)
            # 瘦身策略:只留「進行中 + 近 6 個月新上市」的 chart,其他全 drop
            #   - 進行中 (listing 未定 / 未來): keep (用戶要看即將上市的走勢)
            #   - listing < 6 個月: keep (新上市,投資人關注最近個股表現)
            #   - listing > 6 個月: DROP (老上市,個股走勢不再是焦點;sparkline 退化成 dash)
            # SEED size 從 ~10 MB → ~7 MB,首次載入快很多
            'fmStockChartJson': (
                lambda: (
                    None if (
                        get('listing_date')
                        and get('listing_date') != '未定'
                        and get('listing_date')[:10] < (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
                    )
                    else get('fm_stock_chart_json')
                )
            )(),
            # MOPS 里程碑
            'fmBoardDecisionDate': get('fm_board_decision_date'),
            'fmAccountSetupDate':  get('fm_account_setup_date'),
            # 轉換價定價日 (MOPS 重大訊息 第 53 款公告)
            'fmConvPriceSetDate':    get('fm_conv_price_set_date'),     # 事實發生日 (公告當天)
            'fmConvPriceAnchorDate': get('fm_conv_price_anchor_date'),  # 訂定基準日 (個股 close 樣本截止日)
        }
        issued_by_cb[cb] = {
            'conv_price': try_float(r['conv_price']),
            'tcri':       (r['tcri'] or '').strip(),
            'term':       normalize_term(r['term']),
            'amount':     try_float(r['amount']),
            'capital':    try_float(r['capital']),
            'underwriter': (r['underwriter'] or '').strip(),
            'method':     (r['method'] or '').strip(),
            'auctionPeriod': r['bid_period'],  # MAIL 原文：詢圈期間/競拍期間
            **fm_pr,  # 讓 auctions 也能 fallback 拿到
        }
        # 撤回標記
        is_withdrawn_v = r['is_withdrawn'] if 'is_withdrawn' in keys else None
        withdrawn_note_v = r['withdrawn_note'] if 'withdrawn_note' in keys else None
        # 公開說明書分析 (markdown)
        analysis_md_v = r['analysis_md'] if 'analysis_md' in keys else None
        analysis_updated_at_v = r['analysis_updated_at'] if 'analysis_updated_at' in keys else None
        # 狀態更新 (scan_cb_disclosures / conv_price 偵測到新里程碑時設) → 前端浮頂 + 🆕 badge
        last_status_update_v = r['last_status_update'] if 'last_status_update' in keys else None
        last_status_note_v = r['last_status_note'] if 'last_status_note' in keys else None
        twse_extra = twse_lookup.get(cb, {})  # 沒有就空 dict,前端會 fallback
        issuances.append({
            'cbCode': cb,
            'name': derive_bond_name(cb, r['company'], offset_map),
            'rating': normalize_tcri(r['tcri']),
            'method': r['method'],
            'amount': r['amount'],
            'capital': try_float(r['capital']),  # 股本 (issued 表已有,之前漏 export 到 SEED.issuances → 競拍建議無法算)
            'years': normalize_term(r['term']),
            'broker': r['underwriter'],
            'premium': try_float(r['issue_price']),
            'convPrice': r['conv_price'],
            'convValue': try_float(r['conv_value']),
            'putBack': r['put_cond'],
            'receiveDate': fix_date(r['receipt_date']),
            'effectiveDate': fix_date(r['eff_date']),
            'auctionPeriod': r['bid_period'],
            'auctionDate': auction_dates_by_cb.get(cb),  # 開標/定價日 (歷史 1218 筆,給 timeline 紫色節點用)
            'listDate': fix_date(r['listing_date']),
            'splitDate': None,
            'stockCode': r['stock_code'],
            'isWithdrawn': bool(is_withdrawn_v),
            'withdrawnNote': withdrawn_note_v,
            'isLegacy': bool(get('is_legacy') == 1),  # 老案 (backfill_historical_cb 進來的) — 前端 dashboard 過濾用
            'analysisMd': analysis_md_v,
            'analysisUpdatedAt': analysis_updated_at_v,
            'lastStatusUpdate': last_status_update_v,
            'lastStatusNote': last_status_note_v,
            'id': cb,
            **fm_pr,
            **twse_extra,  # twseLots / twseGuarantee / twseMinBid / twseMargin / twseLeadMgr / twseBidStart / twseBidEnd
        })

    # ---- auctions ----
    auctions = []
    cur.execute('SELECT * FROM auctions')
    for r in cur.fetchall():
        cb = r['cb_code']
        date = fix_date(r['auction_date'])
        if not cb or not date:
            continue
        stk = stocks.get((r['stock_code'] or '').strip(), {})

        # DB → JS field mapping (see comments in main()):
        low = r['min_award_pct']
        high = r['max_award_pct']
        avg = r['actual_price']  # 平均得標 (per user's curated paste)
        # 「轉換價」修 bug:auctions.weighted_avg 欄位命名誤導
        #   - 老資料:weighted_avg = 換算回的股價 (e.g. 2016.8 for 精測,個股 2280)
        #   - 新資料:weighted_avg = 加權平均得標元價 (e.g. 158.4 for 霖宏二,個股 33)
        # 若把新資料的 weighted_avg 當 conv_price → theory = 33/158.4 = 20.83 → premium 660% → 整段爆炸
        # 永遠優先用 issued.conv_price (MOPS 訂定的真實轉換股價),只有沒值才用 weighted_avg
        iss_fb = issued_by_cb.get(cb) or {}
        iss_conv = iss_fb.get('conv_price')
        if iss_conv and iss_conv > 0:
            conv = iss_conv
        else:
            conv = r['weighted_avg']  # 老資料 fallback
        # Fallback:DB actual_price / min_award_pct / max_award_pct 都 NULL,
        # 但 theory_price 和 premium% 有值 → 從 premium 反推得標元價
        # (例 65101 精測一:TWSE scraper 把 weighted_avg 抓成 2016.8 但 actual_price 沒寫)
        _tp_pre = r['theory_price'] if 'theory_price' in r.keys() else None
        _lp = r['low_premium']  if 'low_premium'  in r.keys() else None
        _hp = r['high_premium'] if 'high_premium' in r.keys() else None
        _ap = r['avg_premium']  if 'avg_premium'  in r.keys() else None
        if (avg is None or avg == 0) and _tp_pre and _ap is not None:
            avg = round(_tp_pre * (1 + _ap), 2)
        if (low is None or low == 0) and _tp_pre and _lp is not None:
            low = round(_tp_pre * (1 + _lp), 2)
        if (high is None or high == 0) and _tp_pre and _hp is not None:
            high = round(_tp_pre * (1 + _hp), 2)
        # auctions 多欄位 fallback 到 issued (同 cb_code) — 給新案 rating/term 補齊
        # conv_price 上方已優先用 iss.conv_price,不用再 fallback
        rating_v = normalize_tcri((r['tcri'] or '').strip() or iss_fb.get('tcri') or '')
        term_v = normalize_term(r['term']) or iss_fb.get('term') or None

        # 從 DB 讀新加的欄位（import_xlsx + fetch_close_prices 寫入）
        keys = r.keys()
        close_price  = r['close_price']  if 'close_price'  in keys else None
        theory_price = r['theory_price'] if 'theory_price' in keys else None
        # 若 theory_price 沒算過但有 close 跟剛 fallback 來的 conv，現場算
        if theory_price is None and close_price and conv and conv > 0:
            theory_price = round(close_price / conv * 100, 4)
        low_prem  = r['low_premium']  if 'low_premium'  in keys else None
        high_prem = r['high_premium'] if 'high_premium' in keys else None
        avg_prem  = r['avg_premium']  if 'avg_premium'  in keys else None
        post_high = r['post_high']    if 'post_high'    in keys else None
        post_low  = r['post_low']     if 'post_low'     in keys else None

        # FinMind CB 二級市場 (fetch_cb_market.py 寫入)
        fm_latest_date     = r['fm_latest_date']     if 'fm_latest_date'     in keys else None
        fm_latest_close    = r['fm_latest_close']    if 'fm_latest_close'    in keys else None
        fm_latest_change   = r['fm_latest_change']   if 'fm_latest_change'   in keys else None
        fm_latest_volume   = r['fm_latest_volume']   if 'fm_latest_volume'   in keys else None
        fm_all_high        = r['fm_all_high']        if 'fm_all_high'        in keys else None
        fm_all_low         = r['fm_all_low']         if 'fm_all_low'         in keys else None
        fm_30d_high        = r['fm_30d_high']        if 'fm_30d_high'        in keys else None
        fm_30d_low         = r['fm_30d_low']         if 'fm_30d_low'         in keys else None
        fm_trade_days      = r['fm_trade_days']      if 'fm_trade_days'      in keys else None
        # 衍生指標：以最新 CB 收盤價對應的「相對最低拍價之獲利率」(若有 low 拍價)
        # 例：最低拍價 96.5%、CB 現價 110 → 獲利 = (110 - 96.5) / 96.5 = 13.99%
        fm_latest_vs_low_win = None
        if fm_latest_close is not None and low is not None and low > 0:
            fm_latest_vs_low_win = round((fm_latest_close - low) / low * 100, 2)

        rec = {
            'auctionDate': date,
            'name': derive_bond_name(cb, r['company'], offset_map),
            'stockCode': r['stock_code'],
            'cbCode': cb,
            'capital': r['issue_amount'],
            'scale': r['issue_amount'],
            'years': term_v,
            'guarantee': r['guarantee'],
            'rating': rating_v,
            'auctionQty': r['auction_lots'],
            'minBidPrice': r['min_bid_pct'],
            'closeDate': fix_date(twse_lookup.get(cb, {}).get('twseBidEnd') or r['bid_date']),  # 截標日:優先 TWSE 投標截止(權威),無才退語意不穩的 auctions.bid_date
            'closePrice': close_price,
            'convPrice': conv,
            'theoryPrice': theory_price,
            'lowWinPrice': low,
            'lowWinPremium': low_prem,
            'highWinPrice': high,
            'highWinPremium': high_prem,
            'avgWinPrice': avg,
            'avgWinPremium': avg_prem,
            'postHigh': post_high,
            'postLow': post_low,
            'postClose': None,
            't5Close': None,
            't5Profit': None,
            'estLowWin': low,
            'maxBidQty': (r['auction_lots'] or 0) / 10 if r['auction_lots'] else None,
            'listDate': fix_date(r['listing_date']),
            'broker': r['lead_mgr'],
            'totalAmount': r['total_award_amt'],
            'totalQualified': r['total_valid'],
            'qualifiedQty': r['valid_lots'],
            'wAvgPrice': avg,
            'industry': stk.get('industry'),
            'subIndustry': stk.get('subIndustry'),
            'position': stk.get('position'),
            'related': stk.get('related'),
            'alliance': None,
            'seq': r['serial'],
            'auctionPeriod': iss_fb.get('auctionPeriod'),  # MAIL 原文 bid_period
            # FinMind CB 二級市場
            'fmDate':         fm_latest_date,
            'fmClose':        fm_latest_close,
            'fmChange':       fm_latest_change,
            'fmVolume':       fm_latest_volume,
            'fmAllHigh':      fm_all_high,
            'fmAllLow':       fm_all_low,
            'fm30dHigh':      fm_30d_high,
            'fm30dLow':       fm_30d_low,
            'fmTradeDays':    fm_trade_days,
            'fmVsLowWin':     fm_latest_vs_low_win,  # 現價對最低得標價的獲利%
            # FinMind 個股漲跌 + CB 首日（從 issued 表 fallback）
            'method':            iss_fb.get('method'),
            'fmEffClose':        iss_fb.get('fmEffClose'),
            'fmEffCloseDate':    iss_fb.get('fmEffCloseDate'),
            'fmListingStkClose': iss_fb.get('fmListingStkClose'),
            'fmListingStkDate':  iss_fb.get('fmListingStkDate'),
            'fmStockRallyPct':   iss_fb.get('fmStockRallyPct'),
            'fmBidStartDate':    iss_fb.get('fmBidStartDate'),
            'fmBidEndDate':      iss_fb.get('fmBidEndDate'),
            'fmPreBidRallyPct':  iss_fb.get('fmPreBidRallyPct'),
            'fmCbFirstClose':    iss_fb.get('fmCbFirstClose'),
            'fmCbFirstDate':     iss_fb.get('fmCbFirstDate'),
            'fmCbVs100':         iss_fb.get('fmCbVs100'),
            'fmStockChartJson':  iss_fb.get('fmStockChartJson'),
            'id': f'{cb}_{date}',
        }
        auctions.append(rec)

    # ---- upcoming_auctions (TWSE 官方即將開標) ----
    upcoming = []
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='upcoming_auctions'")
        if cur.fetchone():
            cur.execute('SELECT * FROM upcoming_auctions ORDER BY auction_date')
            for r in cur.fetchall():
                upcoming.append({
                    'cbCode':       r['cb_code'],
                    'name':         r['company'],
                    'stockCode':    r['stock_code'],
                    'auctionDate':  r['auction_date'],
                    'bidStart':     r['bid_start'],
                    'bidEnd':       r['bid_end'],
                    'amountLots':   r['amount_lots'],
                    'minBidPrice':  r['min_bid_price'],
                    'marginPct':    r['margin_pct'],
                    'leadMgr':      r['lead_mgr'],
                    'listingDate':  r['listing_date'],
                    'market':       r['market'],
                    'nature':       r['nature'],
                    'method':       r['method'],
                    'isCancelled':  bool(r['is_cancelled']),
                })
    except Exception as e:
        print(f'  WARN: upcoming_auctions read fail: {e}')

    conn.close()
    return {'auctions': auctions, 'stocks': stocks, 'issuances': issuances, 'upcomingAuctions': upcoming}


def load_current_closes():
    """讀統一證 cache 當日全市場收盤，給決策助手 auto-fill 用。"""
    path = os.path.join(HERE, 'unisec_closes_cache.json')
    if not os.path.exists(path):
        return {'date': '', 'closes': {}}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            d = json.load(f)
        return {'date': d.get('date', ''), 'closes': d.get('closes', {}) or {}}
    except Exception:
        return {'date': '', 'closes': {}}


def write_chart_files():
    """寫 per-CB 個股走勢圖到 charts/{cb}.json,給 modal 懶載 (SEED 只留近 6 月的,其餘點開才抓)。
    legacy 老案的全生命週期圖 window 到發行期間 [anchor-90d, anchor+540d],避免單檔過大 + 控 repo 大小。"""
    import sqlite3
    charts_dir = os.path.join(os.path.dirname(OUT_PATH), 'charts')
    os.makedirs(charts_dir, exist_ok=True)
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    rows = conn.execute("""SELECT cb_code, substr(listing_date,1,10) ld, substr(eff_date,1,10) ed,
                                  fm_board_decision_date bd, is_legacy, fm_stock_chart_json j
                           FROM issued WHERE fm_stock_chart_json IS NOT NULL AND fm_stock_chart_json != ''""").fetchall()
    conn.close()
    written = total_bytes = 0
    for r in rows:
        cb = r['cb_code']
        if not cb:
            continue
        raw = r['j']
        # legacy 全生命週期 (點數多) → window 到發行期間,縮小檔案
        if r['is_legacy'] == 1:
            try:
                pts = json.loads(raw)
                anchor = (r['ld'] or r['bd'] or r['ed'] or '')[:10]
                if anchor and len(pts) > 200 and re.match(r'\d{4}-\d{2}-\d{2}', anchor):
                    a = datetime.strptime(anchor, '%Y-%m-%d')
                    lo = (a - timedelta(days=90)).strftime('%Y-%m-%d')
                    hi = (a + timedelta(days=540)).strftime('%Y-%m-%d')
                    pts = [p for p in pts if lo <= (p.get('d') or '') <= hi]
                    raw = json.dumps(pts, ensure_ascii=False, separators=(',', ':'))
            except Exception:
                pass
        with open(os.path.join(charts_dir, f'{cb}.json'), 'w', encoding='utf-8') as f:
            f.write(raw)
        written += 1
        total_bytes += len(raw)
    print(f'  charts:    {written} 個 per-CB 走勢檔 ({total_bytes/1024/1024:.1f} MB) → {charts_dir}')


def main():
    data = load_data()
    cc = load_current_closes()
    data['currentCloses'] = cc['closes']
    data['currentClosesDate'] = cc['date']
    seed_json = json.dumps(data, ensure_ascii=False, separators=(',', ':'))

    # 載入分離的 HTML scaffold (避免單檔 Python 過大)
    scaffold_path = os.path.join(HERE, '_html_scaffold.html')
    if not os.path.exists(scaffold_path):
        sys.exit(
            f'ERR: scaffold not found: {scaffold_path}\n'
            '請先確認 _html_scaffold.html 存在於 automation/ 資料夾')
    with open(scaffold_path, 'r', encoding='utf-8') as f:
        scaffold = f.read()

    if '/*__SEED__*/' not in scaffold:
        sys.exit('ERR: scaffold 缺少 /*__SEED__*/ 佔位符')

    built_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    seed_block = f'const SEED = {seed_json};\nconst BUILT_AT = "{built_at}";'
    html = scaffold.replace('/*__SEED__*/', seed_block)

    with open(OUT_PATH, 'w', encoding='utf-8') as f:
        f.write(html)

    write_chart_files()
    print(f'Built: {OUT_PATH}')
    print(f'  auctions:  {len(data["auctions"])}')
    print(f'  stocks:    {len(data["stocks"])}')
    print(f'  issuances: {len(data["issuances"])}')
    print(f'  upcoming:  {len(data.get("upcomingAuctions", []))}  (TWSE 即將開標)')
    print(f'  size:      {os.path.getsize(OUT_PATH)/1024:.1f} KB')
    print(f'  built_at:  {built_at}')


if __name__ == '__main__':
    main()
