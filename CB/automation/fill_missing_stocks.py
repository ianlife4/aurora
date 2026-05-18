#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自動補 stocks 表缺漏的個股 (新上市但未在 Excel「個股」sheet 內的)

背景:
  stocks 表來源是用戶手動維護的 Excel sheet[11]. 新上市/興櫃公司 (如 4749 新應材) 沒手動加 → 決策助手顯示「個股庫無此代號」+ 股本沒帶入

策略:
  1. 找 issued/auctions 有 stock_code 但 stocks 沒的
  2. 從 FinMind TaiwanStockInfo 抓 stock_name + industry_category
  3. 從 issued.capital 借用 (元富/統一證 mail 通常有寫)
  4. INSERT INTO stocks (標 note='auto from FinMind+issued')

執行:
  py -3.12 fill_missing_stocks.py
"""
import io
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

HERE = Path(__file__).parent
DB_PATH = HERE / 'cb_data.db'
TOKEN_PATH = HERE / 'finmind_token.txt'
FM_URL = 'https://api.finmindtrade.com/api/v4/data'


def get_missing(conn) -> list[tuple[str, float | None]]:
    """回傳 [(stock_code, capital_from_issued)] - issued/auctions 有 stock_code 但 stocks 沒"""
    cur = conn.cursor()
    cur.execute('''
        WITH used AS (
            SELECT DISTINCT stock_code, MAX(capital) AS cap FROM (
                SELECT stock_code, capital FROM issued WHERE stock_code != ''
            ) GROUP BY stock_code
        )
        SELECT used.stock_code, used.cap FROM used
        LEFT JOIN stocks ON stocks.stock_code = used.stock_code
        WHERE stocks.stock_code IS NULL
        ORDER BY used.stock_code
    ''')
    return cur.fetchall()


def fetch_stock_info(stock_code: str, token: str) -> dict | None:
    """打 FinMind TaiwanStockInfo - 取最新 type=tpex/twse 的紀錄"""
    try:
        r = requests.get(FM_URL, params={
            'dataset':'TaiwanStockInfo','data_id':stock_code,'token':token,
        }, timeout=20)
        rows = r.json().get('data', [])
        if not rows: return None
        # 偏好 tpex/twse 上市上櫃 type,放棄 emerging (興櫃,industry 可能不準)
        for row in reversed(rows):
            if row.get('type') in ('tpex','twse'):
                return row
        return rows[-1]  # fallback 取最新
    except Exception:
        return None


def insert_stock(conn, stock_code: str, info: dict, capital: float | None) -> int:
    cur = conn.cursor()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # 對應 industry_category 到我們 industry 風格 (e.g. "半導體業" → "上櫃半導體" 視 type)
    type_ = info.get('type', '')
    raw_ind = info.get('industry_category', '')
    if type_ == 'tpex':
        industry = '上櫃' + raw_ind.replace('業','').replace('類','') if raw_ind else '上櫃其他'
    elif type_ == 'twse':
        industry = '上市' + raw_ind.replace('業','').replace('類','') if raw_ind else '上市其他'
    else:
        industry = raw_ind
    is_ky = 'KY' in (info.get('stock_name', '') or '')
    cur.execute('''
        INSERT OR REPLACE INTO stocks
          (stock_code, company, industry, sub_industry, biz_desc,
           capital, related, stock_type, ky, updated_at)
        VALUES (?,?,?,?,?, ?,?,?,?, ?)
    ''', (
        stock_code, info.get('stock_name', '') or '', industry, '',
        f'auto-fill from FinMind+issued @ {now}',
        capital, '', type_, 'KY' if is_ky else '', now,
    ))
    conn.commit()
    return cur.rowcount


def main():
    # 優先讀 env (GHA secret),沒有再讀 local 檔
    import os as _os
    token = _os.environ.get('FINMIND_TOKEN', '').strip()
    if not token:
        if not TOKEN_PATH.exists():
            raise RuntimeError('FINMIND_TOKEN env 或 finmind_token.txt 都沒設')
        token = TOKEN_PATH.read_text(encoding='utf-8').strip()
    conn = sqlite3.connect(str(DB_PATH))
    missing = get_missing(conn)
    print(f'缺漏 stock_code: {len(missing)} 筆')

    ok, fail = 0, 0
    for stock_code, capital in missing:
        info = fetch_stock_info(stock_code, token)
        if not info:
            print(f'  [{stock_code}] FinMind 無紀錄 → 跳過'); fail += 1
            time.sleep(0.2)
            continue
        n = insert_stock(conn, stock_code, info, capital)
        print(f'  [{stock_code}] ✓ {info.get("stock_name","?")} ({info.get("industry_category","?")} / {info.get("type","?")}) cap={capital}')
        ok += 1
        time.sleep(0.2)
    conn.close()
    print(f'\n=== DONE: ✓{ok} / ✗{fail} ===')
    print('→ 跑 build_html.py + publish_cb.py')


if __name__ == '__main__':
    main()
