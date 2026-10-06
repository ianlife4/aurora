#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自動補 stocks 表缺漏的個股 (新上市但未在 Excel「個股」sheet 內的) + 補空白股本

背景:
  stocks 表來源是用戶手動維護的 Excel sheet[11]. 新上市/興櫃公司 (如 4749 新應材) 沒手動加 → 決策助手顯示「個股庫無此代號」+ 股本沒帶入

策略:
  1. 找 issued/auctions 有 stock_code 但 stocks 沒的
  2. 從 FinMind TaiwanStockInfo 抓 stock_name + industry_category
  3. 股本:先借 issued.capital (元富/統一證 mail 通常有寫),沒有就查證交所/櫃買 OpenAPI 公司基本資料的「實收資本額」
     (2026-10-06 加:1623/6534/6597/6944/7631/7740/7743 這批 issued.capital 都空、FinMind 又沒股本 → 名片有了股本永遠空)
  4. INSERT INTO stocks (標 note='auto from FinMind+issued')
  5. 順手把 stocks 既有列裡「股本空白」的用 OpenAPI 補上 (只填空,不覆寫 Excel 手填值;新應材手填 9.27 vs OpenAPI 9.47 就不動)

股本單位:億 (跟 Excel 一致,2330 = 2593.27)。OpenAPI 給的是元 → ÷1e8 取兩位。

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

# 證交所/櫃買 OpenAPI 公司基本資料 — (標籤, URL, 代號欄, 實收資本額欄)。
# 三支都是全量清單 (上市 ~1100 / 上櫃 ~900 / 興櫃 ~360 筆),各 ~1 秒,一次抓完當字典用。
# 上市那支欄位是中文、櫃買兩支是英文 key,所以欄名要各自指定。
CAPITAL_SOURCES = [
    ('twse 上市', 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L', '公司代號', '實收資本額'),
    ('tpex 上櫃', 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O', 'SecuritiesCompanyCode', 'Paidin.Capital.NTDollars'),
    ('tpex 興櫃', 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_R', 'SecuritiesCompanyCode', 'Paidin.Capital.NTDollars'),
]
OPENAPI_HEADERS = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}


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


def fetch_capital_map() -> dict[str, float]:
    """證交所/櫃買 OpenAPI 公司基本資料 → {stock_code: 實收資本額(億)}。
    任一來源失敗只印訊息、回部分結果 (補不到就留空,下次再補)。"""
    out: dict[str, float] = {}
    for label, url, code_key, cap_key in CAPITAL_SOURCES:
        try:
            r = requests.get(url, headers=OPENAPI_HEADERS, timeout=30)
            r.raise_for_status()
            rows = r.json()
        except Exception as e:
            print(f'  [capital] {label} 抓取失敗 (跳過): {e}')
            continue
        n = 0
        for row in rows:
            code = str(row.get(code_key, '') or '').strip()
            raw = str(row.get(cap_key, '') or '').replace(',', '').strip()
            if not code or not raw:
                continue
            try:
                yi = round(float(raw) / 1e8, 2)
            except ValueError:
                continue
            if yi > 0 and code not in out:      # 同代號不重複 (上市先列,先到先贏)
                out[code] = yi
                n += 1
        print(f'  [capital] {label}: {n} 檔')
    return out


def fill_blank_capital(conn, capmap: dict[str, float]) -> list[tuple[str, float]]:
    """stocks 既有列股本空白 (NULL/0/'') 且 OpenAPI 查得到 → UPDATE。只填空、不覆寫手填值。"""
    if not capmap:
        return []
    cur = conn.cursor()
    rows = cur.execute('''SELECT stock_code FROM stocks
                          WHERE capital IS NULL OR capital = 0 OR capital = '' ''').fetchall()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    filled = []
    for (code,) in rows:
        cap = capmap.get(code)
        if cap is None:
            continue
        cur.execute('UPDATE stocks SET capital=?, updated_at=? WHERE stock_code=?', (cap, now, code))
        filled.append((code, cap))
    conn.commit()
    return filled


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
    capmap = fetch_capital_map()

    ok, fail = 0, 0
    for stock_code, capital in missing:
        info = fetch_stock_info(stock_code, token)
        if not info:
            print(f'  [{stock_code}] FinMind 無紀錄 → 跳過'); fail += 1
            time.sleep(0.2)
            continue
        cap_src = 'issued'
        if not capital:                               # issued 沒寫股本 → OpenAPI 實收資本額
            capital = capmap.get(stock_code)
            cap_src = 'openapi' if capital is not None else '無'
        n = insert_stock(conn, stock_code, info, capital)
        print(f'  [{stock_code}] ✓ {info.get("stock_name","?")} ({info.get("industry_category","?")} / {info.get("type","?")}) cap={capital} ({cap_src})')
        ok += 1
        time.sleep(0.2)

    # 既有列股本空白的也補 (只填空)。空的多半是老下市股,OpenAPI 沒列就維持空
    filled = fill_blank_capital(conn, capmap)
    if filled:
        print(f'\n補空白股本 (OpenAPI 實收資本額): {len(filled)} 檔 — ' + ', '.join(f'{c}={v}' for c, v in filled))
    else:
        print('\n補空白股本: 0 檔 (空的都是 OpenAPI 查不到的老股)')
    conn.close()
    print(f'\n=== DONE: ✓{ok} / ✗{fail} ===')
    print('→ 跑 build_html.py + publish_cb.py')


if __name__ == '__main__':
    main()
