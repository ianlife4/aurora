#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CB 自動化系統設定檔
"""
from pathlib import Path

# ── 路徑 ─────────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).parent
ROOT_DIR      = BASE_DIR.parent
EXCEL_PATH    = ROOT_DIR / 'CB競拍整理 AAAS.xlsb.xlsx'
TOKEN_PATH    = BASE_DIR / 'token.json'
CREDS_PATH    = BASE_DIR / 'credentials.json'
DOWNLOAD_DIR  = BASE_DIR / 'downloads'
LOG_PATH      = BASE_DIR / 'sync_log.txt'

DOWNLOAD_DIR.mkdir(exist_ok=True)

# ── Gmail 設定 ────────────────────────────────────────────────────────────────
GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# 元富郵件識別
MASTERLINK_SENDER  = 'wnhuang@masterlink.com.tw'
MASTERLINK_SUBJECT = 'CB初級市場資訊'

# 其他可參考的郵件來源（補充用）
OTHER_CB_SENDERS = {
    'cbas@sinopac.com':           '永豐金',
    'MaychiLiao@yuanta.com':      '元大',
    'joyce.ycy.wang@fubon.com':   '富邦',
}

# ── TWSE 設定 ─────────────────────────────────────────────────────────────────
TWSE_AUCTION_URL = 'https://www.twse.com.tw/rwd/zh/announcement/auction'
TWSE_PAGE_URL    = 'https://www.twse.com.tw/zh/announcement/auction.html'

# CB 發行性質關鍵字（過濾用）
CB_KEYWORDS = ['轉換公司債', '可轉換']

# ── Excel 工作表名稱 ──────────────────────────────────────────────────────────
# 注意：工作表名稱在 openpyxl 中是 bytes，以下用 index 對應
SHEET_INDEX = {
    '總覽':        0,
    '承銷商':      1,
    '發行業務':    2,
    '競拍詳細':    3,
    '轉換價格':    4,
    '信評TCRI':    5,
    '承銷業務':    6,
    '下單計畫':    7,
    '已發行1':     8,   # ← 主要更新目標
    'CB競拍結果':  9,   # ← 主要更新目標
    'CB競拍計算': 10,
    '個股':        11,  # ← 產業資料參考
    '工作表1':     12,
    '工作表2':     13,
}

# ── 已發行1 欄位對應（0-indexed）────────────────────────────────────────────
# 元富 email 欄位 → Excel 已發行1 欄位 index
ISSUED_COL = {
    'cb_code':       0,   # 代碼（CB 5碼）
    'company':       1,   # 發行標的名稱
    'tcri':          2,   # TCRI/等O
    'method':        3,   # 競拍方式（競拍/詢圈）
    'amount':        4,   # 金額(億元)
    'term':          5,   # 年期（3Y/5Y）
    'underwriter':   6,   # 主幹事（承銷券商）
    'issue_price':   7,   # 轉換比例/發行價格%
    'conv_price':    8,   # 競拍價格（轉換價）
    'conv_price2':   9,   # 競拍價格（轉換價值%）
    'put_cond':      10,  # 回^條件（年/賣回條件）
    'receipt_date':  11,  # 備注（收件日）
    'eff_date':      12,  # 生效日
    'bid_period':    13,  # 競拍/承銷競拍（詢圈/競拍期間）
    'listing_date':  14,  # 競價日期（掛牌日）
    'capital':       15,  # 可認購量（資本額億）
    'note':          16,  # 備1
    'stock_code':    17,  # 代碼（股票4碼）
}

# ── CB競拍結果 欄位對應（0-indexed）─────────────────────────────────────────
# TWSE 欄位 → Excel CB競拍結果 欄位 index
AUCTION_COL = {
    'serial':           0,   # 發行序號
    'auction_date':     1,   # 競拍日期
    'company':          2,   # 股票名稱
    'stock_code':       3,   # 股票代號（4碼）
    'cb_code':          4,   # CB代號（5碼）
    'issue_amount':     5,   # 發行量(億元)
    'auction_amount':   6,   # 競拍量
    'term':             7,   # 年期
    'guarantee':        8,   # 擔保 YES/NO
    'tcri':             9,   # TCRI
    'auction_lots':     10,  # 競拍數量(張)
    'min_bid_pct':      11,  # 最低投標價格(%)
    'bid_date':         12,  # 競標日期（投標開始）
    'max_during_bid':   13,  # 競標期間最高股價
    'weighted_avg':     14,  # 競拍加權平均價
    'conv_ratio':       15,  # 投標比率
    'min_award_pct':    16,  # 最低得標價格(%)
    'min_award_chg':    17,  # 最低得標漲跌
    'max_award_pct':    18,  # 最高得標價格(%)
    'max_award_chg':    19,  # 最高得標漲跌
    'avg_award_pct':    20,  # 得標均價(%)
    'avg_award_chg':    21,  # 得標均價漲跌
    't5_high':          22,  # T+5最高
    't5_low':           23,  # T+5最低
    'awarded_lots':     24,  # 得標張數
    'listing_date':     25,  # 掛牌日
    'lead_mgr':         26,  # 主幹事
    'total_award_amt':  27,  # 得標總金額(元)
    'total_valid':      28,  # 總合格件
    'valid_lots':       29,  # 合格投標數量(張)
    'actual_price':     30,  # 實際承銷價格(%)
    'cb_size_range':    31,  # CB發行量（範圍）
    'size_cat':         32,  # 發行大小分類
    'timing':           33,  # 發行時機
    'lots_range':       34,  # 競拍張數範圍
    'main_biz':         35,  # 主業
    'sub_biz':          36,  # 副業
    'biz_desc':         37,  # 主業描述（產業地位）
    'emerging_ind':     38,  # 新興產業
    'emerging_mkt':     39,  # 新興市場
    'underwriter_range':40,  # 承銷商（範圍）
    'note':             41,  # 備1
}
