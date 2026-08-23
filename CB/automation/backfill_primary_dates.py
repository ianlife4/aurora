#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""從 統一證『預計發行CB資料』sheet 批次回填 issued 表時程欄位。

問題: 多數 pipeline CB 只有 fm_board_decision_date (MOPS scan),缺
      eff_date / bid 期間 / listing_date → modal timeline 大多是「預估」。
      但統一證 xlsx 的『預計發行CB資料』sheet 兩段 (近期掛牌 + 近期生效)
      早就有 公告日/送件日/預計生效日/詢圈競拍期間/轉換價/掛牌日。

本支把這些日期 COALESCE 進 issued (只填空欄,不蓋既有權威值):
  預計生效日   → eff_date
  送件日       → receipt_date
  掛牌日       → listing_date  (覆寫 ''/未定)
  詢圈/競拍 M/D-M/D → fm_bid_start_date / fm_bid_end_date
  轉換價(數字) → conv_price
  詢圈/競拍 字樣 → method
偵測到新 eff_date / bid 期間 → 設 last_status_update (HTML 浮頂 + 🆕)。

執行: py -3.12 backfill_primary_dates.py [--dry-run]
資料源: stock-dash\cbas-template\CB報\CB發行資訊與CBAS報價表_統一證_*.xlsx (最新)
"""
import argparse
import glob
import io
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import openpyxl

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

HERE = Path(__file__).parent
DB_PATH = HERE / 'cb_data.db'
CB_DROP = Path(r'C:\Users\J.Chun\Desktop\stock-dash\cbas-template\CB報')


def latest_unisec_xlsx():
    files = sorted(glob.glob(str(CB_DROP / 'CB發行資訊與CBAS報價表_統一證_*.xlsx')))
    return files[-1] if files else None


def to_iso(v):
    """datetime / '2026/6/23' / '2026-06-15 00:00' → 'YYYY-MM-DD'。"""
    if v is None or v == '':
        return None
    if isinstance(v, datetime):
        return v.strftime('%Y-%m-%d')
    s = str(v).strip()
    m = re.match(r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})', s)
    if m:
        return f'{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}'
    return None


_ZH2N = {'一': '1', '二': '2', '三': '3', '四': '4', '五': '5',
         '六': '6', '七': '7', '八': '8', '九': '9'}


def _cb_from_name(name, given_cb):
    """從 CB 名稱末字的中文次數推正確代號 (十銓五 + 4967 → 49675)。

    券商 xlsx 的 CB 代號欄偶爾誤植 (2026-08-13 統一證把十銓五寫成 49674),
    但【名稱】幾乎不會錯 → 拿名稱當校驗碼。回 None 表示無法判定 (不動原值)。
    """
    if not name or not given_cb or len(str(given_cb)) < 5:
        return None
    n = _ZH2N.get(str(name).strip()[-1])
    if not n:
        return None                      # 名稱沒有中文次數 (如 KY/永) → 不判
    stock = str(given_cb)[:-1]           # 去掉最後一碼次數
    return stock + n


def parse_bid_period(text, year):
    """'6/1-6/3詢圈' / '6/12-6/16競拍' → ('2026-06-01','2026-06-03')。
       純 '詢圈'/'競拍' (還沒排期) → (None, None)。"""
    if not text:
        return None, None
    m = re.search(r'(\d{1,2})/(\d{1,2})\s*[-~]\s*(\d{1,2})/(\d{1,2})', str(text))
    if not m:
        return None, None
    a = f'{year:04d}-{int(m.group(1)):02d}-{int(m.group(2)):02d}'
    b = f'{year:04d}-{int(m.group(3)):02d}-{int(m.group(4)):02d}'
    return a, b


def norm_tcri(text):
    """'TCR4/無擔' / 'TCRI6/無擔' → 'TCRI4' / 'TCRI6'。券商檔常寫成 TCR 少一個 I。"""
    m = re.search(r'TCRI?\s*(\d+)', str(text or ''), re.I)
    return f'TCRI{m.group(1)}' if m else None


def norm_term(text):
    """'5年' → '5Y';已是 '5Y' 就原樣回。"""
    m = re.search(r'(\d+)\s*年', str(text or ''))
    if m:
        return f'{m.group(1)}Y'
    m = re.match(r'^\s*(\d+)\s*Y\s*$', str(text or ''), re.I)
    return f'{m.group(1)}Y' if m else None


def norm_amount(v):
    """發行量(億) → float。'4.8' / 4.8 / '15' 都吃。"""
    try:
        f = float(str(v).replace(',', '').strip())
        return f if 0 < f < 100000 else None
    except (ValueError, TypeError):
        return None


def clean_undecided(v):
    """'未定' / 空 → None (不要把『未定』寫進 DB 當成真值)。"""
    s = str(v or '').strip()
    return None if (not s or s == '未定') else s


def method_from(text):
    t = str(text or '')
    if '競拍' in t:
        return '競拍'
    if '詢圈' in t:
        return '詢圈'
    return None


def read_rows(path):
    """讀『預計發行CB資料』,回 [{cb, name, conv_price, listing, receipt, eff, bid_text, method}]。
       兩段 header 欄位略不同 → 動態用 header 文字定位欄。"""
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    ws = wb['預計發行CB資料']
    raw = []
    blank = 0
    for r in ws.iter_rows(values_only=True):
        if all(c is None or str(c).strip() == '' for c in r[:14]):
            blank += 1
            if blank >= 30:
                break
            continue
        blank = 0
        raw.append(list(r))
    wb.close()

    out = []
    header = None
    col = {}
    for r in raw:
        c0 = str(r[0] or '').strip()
        if c0 == '標的代號':  # header row
            header = r
            col = {}
            for i, h in enumerate(header):
                hs = str(h or '').replace(' ', '')
                if 'CB代號' in hs: col['cb'] = i
                elif 'CB名稱' in hs: col['name'] = i
                # 2026-08-23:原本只讀日期類欄位,主辦券商/TCRI/發行量/年期/賣回條件全被忽略 →
                #   尖點三、濱川七、高力六 的卡片一直顯示「? · ?」,但這些值檔案裡明明就有。
                elif 'TCRI' in hs.upper() or '擔保' in hs: col.setdefault('tcri', i)
                elif '發行量' in hs: col.setdefault('amount', i)
                elif '主辦券商' in hs: col.setdefault('underwriter', i)
                elif '年期' in hs: col.setdefault('term', i)
                elif '賣回條件' in hs: col.setdefault('put', i)
                elif '轉換價' == hs or hs == '轉換價': col.setdefault('conv', i)
                elif '掛牌日' in hs: col['listing'] = i
                elif '送件日' in hs: col['receipt'] = i
                elif '預計生效日' in hs: col['eff'] = i
                elif '詢圈/競拍' in hs or hs == '詢圈/競拍': col['bid'] = i
            continue
        if not header or 'cb' not in col:
            continue
        cb = str(r[col['cb']] or '').strip() if col.get('cb') is not None and col['cb'] < len(r) else ''
        if not (cb.isdigit() and len(cb) >= 5):
            continue
        def cell(key):
            i = col.get(key)
            return r[i] if (i is not None and i < len(r)) else None
        conv_raw = cell('conv')
        conv = None
        try:
            cv = float(str(conv_raw).replace(',', ''))
            if 0.01 < cv < 100000:
                conv = cv
        except (ValueError, TypeError):
            conv = None
        out.append({
            'cb': cb,
            'name': str(cell('name') or '').strip() or None,
            'conv_price': conv,
            'tcri': norm_tcri(cell('tcri')),
            'amount': norm_amount(cell('amount')),
            'underwriter': clean_undecided(cell('underwriter')),
            'term': norm_term(cell('term')),
            'put_cond': clean_undecided(cell('put')),
            'listing': to_iso(cell('listing')),
            'receipt': to_iso(cell('receipt')),
            'eff': to_iso(cell('eff')),
            'bid_text': cell('bid'),
            'method': method_from(cell('bid')),
        })
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    path = latest_unisec_xlsx()
    if not path:
        sys.exit('找不到統一證 xlsx (CB報/)')
    print(f'來源: {Path(path).name}')
    rows = read_rows(path)
    print(f'解析 {len(rows)} 筆預計發行 CB')

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    n_eff = n_bid = n_list = n_conv = n_recv = n_method = n_attr = 0
    updates = []

    mismatches = []
    for it in rows:
        # 🔴 券商 xlsx 的 CB 代號會誤植 — 用【CB名稱】交叉驗證,不一致就以名稱為準。
        #    2026-08-13:統一證檔第 17 列寫「4967 | 49674 | 十銓五」,代號其實是十銓【四】,
        #    導致回填全寫到 49674(被 COALESCE 擋掉沒動),而 49675 十銓五 的方式/TCRI/發行量/
        #    轉換價/承銷商 全部空白,一路空到掛牌前 5 天用戶才發現。
        it_cb = it['cb']
        fixed = _cb_from_name(it.get('name'), it_cb)
        if fixed and fixed != it_cb:
            mismatches.append((it_cb, fixed, it.get('name')))
            it_cb = fixed
        cur = conn.execute('SELECT * FROM issued WHERE cb_code=?', (it_cb,)).fetchone()
        if not cur:
            continue  # 不在 issued (新案由 scan_cb_disclosures 處理)
        it = {**it, 'cb': it_cb}
        k = cur.keys()
        sets, vals, notes = [], [], []

        def empty(field):
            v = cur[field] if field in k else None
            return v is None or str(v).strip() == '' or str(v).strip() == '未定'

        if it['eff'] and empty('eff_date'):
            sets.append('eff_date=?'); vals.append(it['eff']); n_eff += 1; notes.append(f'生效{it["eff"][5:]}')
        if it['receipt'] and empty('receipt_date'):
            sets.append('receipt_date=?'); vals.append(it['receipt']); n_recv += 1
        if it['listing'] and empty('listing_date'):
            sets.append('listing_date=?'); vals.append(it['listing']); n_list += 1; notes.append(f'掛牌{it["listing"][5:]}')
        if it['conv_price'] and empty('conv_price'):
            sets.append('conv_price=?'); vals.append(it['conv_price']); n_conv += 1; notes.append(f'轉換價{it["conv_price"]}')
        if it['method'] and empty('method'):
            sets.append('method=?'); vals.append(it['method']); n_method += 1
        # 券商/評等/發行量/年期/賣回條件 — 一樣只填空欄,不蓋既有值 (保護手填如聯電)
        for fld, key, label in (('tcri', 'tcri', ''), ('amount', 'amount', '發行量'),
                                ('underwriter', 'underwriter', '承銷商'),
                                ('term', 'term', '年期'), ('put_cond', 'put_cond', '賣回')):
            if it.get(key) is not None and empty(fld):
                sets.append(f'{fld}=?'); vals.append(it[key])
                n_attr += 1
                notes.append(f'{label}{it[key]}')
        # bid 期間 (年份用 eff 或 listing 推)
        yr = None
        for d in (it['eff'], it['listing'], it['receipt']):
            if d:
                yr = int(d[:4]); break
        if yr:
            bs, be = parse_bid_period(it['bid_text'], yr)
            if bs and empty('fm_bid_start_date'):
                sets.append('fm_bid_start_date=?'); vals.append(bs)
                sets.append('fm_bid_end_date=?'); vals.append(be)
                n_bid += 1; notes.append(f'{it["method"] or "投標"}{bs[5:]}~{be[5:]}')

        if not sets:
            continue
        if notes:
            sets.append('last_status_update=?'); vals.append(now)
            sets.append('last_status_note=?'); vals.append(' / '.join(notes))
        sets.append('updated_at=?'); vals.append(now)
        vals.append(it['cb'])
        if not args.dry_run:
            conn.execute(f'UPDATE issued SET {", ".join(sets)} WHERE cb_code=?', vals)
        updates.append((it['cb'], it['name'], ' / '.join(notes) if notes else '(僅補日期)'))

    if not args.dry_run:
        conn.commit()
    conn.close()

    if mismatches:
        print(f'\n🔴 券商檔 CB 代號與名稱不符 {len(mismatches)} 筆 (已以【名稱】為準改寫):')
        for bad, good, nm in mismatches:
            print(f'   {bad} → {good}  ({nm})')

    print(f'\n回填: 生效{n_eff} / 送件{n_recv} / 掛牌{n_list} / 轉換價{n_conv} / 方式{n_method} / 投標期間{n_bid}')
    print('=== 異動 ===' if updates else '(無異動)')
    for cb, nm, note in updates:
        print(f'  {cb} {nm or ""}  {note}')
    if args.dry_run:
        print('\n[dry-run] 未寫入')


if __name__ == '__main__':
    main()
