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

import db as _dbm                  # retire_reused_code:代號重用 (2026-10-05 光譜三 53813)

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
    if not name or not given_cb or len(str(given_cb)) < 4:
        return None
    n = _ZH2N.get(str(name).strip()[-1])
    if not n:
        return None                      # 名稱沒有中文次數 (如 KY/永) → 不判
    g = str(given_cb)
    # 🔴 2026-09-10 威剛九血案:統一證從 08-03 起【連續 6 期】把 CB 代號寫成股票代號「3260」
    #    (4 碼,正確是 32609)。舊守衛 len<5 → 直接 return None → read_rows 又把 4 碼整列丟掉
    #    → 方式/承銷商/TCRI/發行量/申報日/生效日 六週全沒進 DB → 儀表板認不出它是詢圈、
    #    TWSA 圈購對不上 (要 method LIKE 詢圈)、conv_price job 也不理它 (要 eff_date)
    #    → 09-10 訂價當天窗口完全沒它,用戶看 MOPS 才發現。
    #    4 碼 = 券商只寫了股票代號 → 直接當 stock;5 碼以上 → 去尾碼當 stock。
    stock = g if len(g) == 4 else g[:-1]
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


_NULLISH = ('', '-', '—', '－', 'N/A', 'na', 'None', '未定', '無')

# 只對這四個欄位報不一致 — 它們決定儀表板卡片,而且格式穩定。
#   tcri 不報:信評本來就會被調整,每輪都跳一堆。
#   put_cond 不報:券商檔 2026 年改了寫法 (「3年100」→「YTP(3)=(0%)」),整批都會「不一致」。
REVISION_FIELDS = ('method', 'amount', 'underwriter', 'term')


def _canon_uw(s):
    """券商名正規化到可比對的核心:'凱基證券'/'凱基證' → '凱基';'華南永昌證' → '華南'。"""
    t = re.sub(r'\([^)]*\)', '', str(s or '')).strip()        # 去掉 '(第一銀)' 這種保證行
    t = re.sub(r'(綜合|金鼎|永昌|控股)', '', t)
    return re.sub(r'(證券|證)$', '', t).strip()


def _same_val(a, b, field=None):
    """DB 值 a 與券商檔值 b 算不算「一致」(不一致才示警)。"""
    sa, sb = str(a).strip() if a is not None else '', str(b).strip() if b is not None else ''
    if sa in _NULLISH or sb in _NULLISH:
        return True                      # 任一邊是空/佔位 → 那是「待填」不是「衝突」
    try:
        return abs(float(sa) - float(sb)) < 1e-6
    except (TypeError, ValueError):
        pass
    if sa == sb:
        return True
    if field == 'underwriter':
        return _canon_uw(sa) == _canon_uw(sb)
    # DB 比券商檔【更詳細】不算不一致:'富邦證(上海銀)' vs '富邦證'、'兆豐證(未定)' vs '兆豐證'
    return sa.startswith(sb) and sa[len(sb):].startswith('(')


def _in_flight(cur):
    """還沒掛牌 (或今天以後才掛) 才值得盯 — 已上市的舊案值錯了也沒人要看。"""
    ld = str(cur['listing_date'] or '').strip()[:10]
    return (not ld) or ld == '未定' or ld >= datetime.now().strftime('%Y-%m-%d')


def _richness(it):
    """一列資料的資訊量 — 用來在同一檔 CB 重複出現時挑最完整的那列。"""
    return sum(1 for k in ('conv_price', 'tcri', 'amount', 'underwriter', 'term',
                           'put_cond', 'listing', 'receipt', 'eff', 'method')
               if it.get(k) is not None)


def merge_dupes(rows):
    """🔴 統一證檔同一檔 CB 會在【兩個區段】各列一次 (近期生效/掛牌段 + 董事會通過段),
       而較舊的那段值是過期的:2026-09-21 檔裡由田一同時出現
       「7 億 / 台新證」(新) 和「5 億 / 未定」(舊)。
       舊版照順序處理 → 先遇到哪列就填哪列的值,DB 因此存了錯的 5 億,
       而且改版偵測也會對著舊列狂報假不一致。
       改成:同 cb 依資訊量排序後合併,豐富的那列優先,欄位各取第一個非空。"""
    by_cb = {}
    for it in rows:
        by_cb.setdefault(it['cb'], []).append(it)
    out = []
    for cb, group in by_cb.items():
        if len(group) == 1:
            out.append(group[0]); continue
        group.sort(key=_richness, reverse=True)
        merged = dict(group[0])
        for other in group[1:]:
            for k, v in other.items():
                if merged.get(k) is None and v is not None:
                    merged[k] = v
        out.append(merged)
    return out


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
        # 4 碼也放行 — 讓 main() 的 _cb_from_name 用名稱把「3260 威剛九」修成 32609 (2026-09-10)。
        # 4 碼但名稱沒中文次數的,_cb_from_name 回 None → DB 找不到 → 一樣 skip,不會誤寫。
        if not (cb.isdigit() and len(cb) >= 4):
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

    # 先把代號修正 (名稱當校驗碼),再合併同檔重複列 — 順序不能顛倒,
    # 否則「3260 威剛九」和「32609 威剛九」會被當成兩檔而合不起來。
    mismatches = []
    for it in rows:
        fixed = _cb_from_name(it.get('name'), it['cb'])
        if fixed and fixed != it['cb']:
            mismatches.append((it['cb'], fixed, it.get('name')))
            it['cb'] = fixed
    before = len(rows)
    rows = merge_dupes(rows)
    if before != len(rows):
        print(f'合併同檔重複列: {before} → {len(rows)} 筆 (統一證檔會在兩個區段各列一次)')

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 順手同步假日表 — 這支已經把 xlsx 開起來了,holiday 分頁就在同一個檔。
    # 訂價窗口的 T-5/T-3/T-1 要靠它跳過國定假日 (2026-09-28 教師節踩到,見 tw_calendar)。
    try:
        import tw_calendar
        _add, _tot = tw_calendar.import_from_xlsx(conn, path)
        if _add:
            print(f'假日表: 新增 {_add} 筆 (共 {_tot})')
    except Exception as _e:
        print(f'  [WARN] 假日表同步失敗: {_e}')

    # 順手重建「統一證 mail 收盤」快取 (unisec_closes_cache.json)。
    # 🔴 2026-09-28:這個快取只有 run_update/self_update 會寫,而 cron_pulse 沒跑它們、
    #    舊排程 CB Auto Update Daily 已停用 → 停在 9/9 手動跑的 09-04 收盤,modal 顯示三週前的價。
    #    這支每輪本來就開著最新 xlsx,轉換標的收盤價分頁就在裡面,直接重建最省事。
    #    日期 = 檔名日期的前一個【交易日】(統一證週一寄、內含上週五收盤;要跳假日,用 tw_calendar)。
    try:
        if not args.dry_run:
            from unisec_parser import parse_unisec_excel
            _closes = (parse_unisec_excel(Path(path)) or {}).get('closes') or {}
            _m = re.search(r'(\d{8})', Path(path).stem)
            if _closes and _m:
                import json as _json
                import tw_calendar
                _fd = datetime.strptime(_m.group(1), '%Y%m%d').date()
                _cache_date = tw_calendar.prev_trading_day(_fd).isoformat()
                _cp = HERE / 'unisec_closes_cache.json'
                _old = None
                try:
                    _old = _json.loads(_cp.read_text(encoding='utf-8')).get('date')
                except Exception:
                    pass
                if _old != _cache_date:
                    _cp.write_text(_json.dumps({'date': _cache_date, 'source': 'unisec', 'closes': _closes},
                                               ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
                    print(f'收盤快取: {_old or "(無)"} → {_cache_date} ({len(_closes)} 檔)')
    except Exception as _e:
        print(f'  [WARN] 收盤快取重建失敗: {_e}')
    n_eff = n_bid = n_list = n_conv = n_recv = n_method = n_attr = 0
    updates = []

    revisions = []   # 券商檔的值與 DB 現值不同 (多半是券商改版) → 只報不改
    # 代號誤植修正 (🔴 統一證 2026-08-13 把十銓五寫成 49674、09-10 把威剛九寫成 4 碼 3260)
    # 已在上面 merge 之前做完,mismatches 也在那裡收集。
    for it in rows:
        cur = conn.execute('SELECT * FROM issued WHERE cb_code=?', (it['cb'],)).fetchone()
        if not cur:
            continue  # 不在 issued (新案由 scan_cb_disclosures 處理)
        # 🔴 代號重用:券商檔的新案 (預計生效日在舊列掛牌一年以後) 撞到同代號的到期舊債 →
        #    舊債封存、這列讓給新案。2026-08 光譜三 53813 就是被「僅補日期」併進 2008 合正三那列,
        #    掛著 2008 掛牌日 → _in_flight() 判成已上市 → 發行額 3 億 vs 券商檔 5 億的不一致也從沒報出來。
        #    送件日不拿來判 (券商檔送件欄常錯,見 db.retire_reused_codes_all)。
        if it.get('eff') and not args.dry_run and _dbm.retire_reused_code(conn, it['cb'], it['eff'], '統一證券商檔',
                                                                           new_company=it.get('name')):
            cur = conn.execute('SELECT * FROM issued WHERE cb_code=?', (it['cb'],)).fetchone()
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
        elif (it['method'] and 'method' in k and _in_flight(cur)
              and not _same_val(cur['method'], it['method'], 'method')):
            revisions.append((it['cb'], it['name'], '方式', cur['method'], it['method']))
        # 券商/評等/發行量/年期/賣回條件 — 一樣只填空欄,不蓋既有值 (保護手填如聯電)
        for fld, key, label in (('tcri', 'tcri', ''), ('amount', 'amount', '發行量'),
                                ('underwriter', 'underwriter', '承銷商'),
                                ('term', 'term', '年期'), ('put_cond', 'put_cond', '賣回')):
            if it.get(key) is not None and empty(fld):
                sets.append(f'{fld}=?'); vals.append(it[key])
                n_attr += 1
                notes.append(f'{label}{it[key]}')
            elif (it.get(key) is not None and fld in k and fld in REVISION_FIELDS
                  and _in_flight(cur) and not _same_val(cur[fld], it[key], fld)):
                # 🔴 只填空欄保護了手填值,但也讓【券商改版後的新值】永遠進不來:
                #    由田一 發行量 08 月檔寫 5 億、09/21 檔改成 7 億,DB 卻一直掛 5 億,
                #    畫面錯了半個多月沒人知道 (2026-09-21 用戶抓到方式錯時一併發現)。
                #    不自動覆寫 (怕蓋掉聯電那種手填),但一定要印出來讓人決定。
                revisions.append((it['cb'], it['name'], label or fld, cur[fld], it[key]))
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

    if revisions:
        print(f'\n⚠ 券商檔與 DB 不一致 {len(revisions)} 筆 (未覆寫 — 券商改版或 DB 舊值,請人工確認):')
        for cb, nm, label, old_v, new_v in revisions:
            print(f'   {cb} {nm or ""}  {label}: DB={old_v} / 券商檔={new_v}')

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
