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
import json
import re
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import openpyxl

import db as _dbm                  # resolve_new_cb_code:第N次推代號撞號 (2026-10-05 光譜 53814)

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

HERE = Path(__file__).parent
DB_PATH = HERE / 'cb_data.db'
CB_DROP = Path(r'C:\Users\J.Chun\Desktop\stock-dash\cbas-template\CB報')


def latest_unisec_xlsx():
    files = sorted(glob.glob(str(CB_DROP / 'CB發行資訊與CBAS報價表_統一證_*.xlsx')))
    return files[-1] if files else None


def latest_fubon_primary():
    """富邦『CB初級市場資訊YYYYMMDD.xlsx』(gmail_fetch_cbas_all 抓進 CB報/,原本只有 CBAS 站在讀)。"""
    files = sorted(glob.glob(str(CB_DROP / 'CB初級市場資訊[0-9]*.xlsx')))
    return files[-1] if files else None


def read_fubon_methods(path):
    """富邦初級檔 → {cb: (name, method)}。

    🔴 2026-11-03 映興二 35972:富邦 09-04 檔就改成「詢圈 102%發行」,統一證到 11-02 還寫競拍,
       DB 照統一證掛競拍 → 公會圈購對不上、訂價窗口照競拍算,兩個月沒人知道。富邦檔原本只有 CBAS 站讀,
       這裡拿來當方式的第二意見 (只報衝突,不覆寫;空欄才填)。
    版面同 stock-dash/cbas-template/build.py parse_primary_market_v2:詢圈競拍段/送件段方式在第 9 欄,
    董事會段在第 8 欄。同一檔出現在兩段時以前兩段 (較新) 為準。"""
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    ws = wb['CB初級市場資訊']
    out, sec, blank = {}, None, 0
    for r in ws.iter_rows(values_only=True):
        if all(c is None or str(c).strip() == '' for c in r[:10]):
            blank += 1
            if blank >= 40:
                break
            continue
        blank = 0
        c0 = str(r[0] or '').strip()
        if '詢圈' in c0 and '競拍' in c0 and '標的' in c0:
            sec = 'pipe'; continue
        if c0 == '送件標的' or ('送件' in c0 and len(c0) < 8):
            sec = 'pipe'; continue
        if '董事會' in c0 and ('通過' in c0 or '發行' in c0):
            sec = 'board'; continue
        if not (sec and c0.isdigit() and len(c0) >= 4):
            continue
        idx = 8 if sec == 'pipe' else 7
        txt = str((r[idx] if idx < len(r) else None) or '')
        # 「競拍 底標100% 詢圈 100%~101%發行」(系微一 10-30 檔) 兩種都寫 = 還沒定或兩檔合寫 → 不判
        #   (同 board_attrs.parse_method:留空讓權威來源補,猜了必錯 — 2026-09-21 由田)
        m = None if ('競拍' in txt and '詢圈' in txt) else method_from(txt)
        if not m or (c0 in out and out[c0][2] == 'pipe'):
            continue
        out[c0] = (str(r[1] or '').strip(), m, sec)
    wb.close()
    return {cb: (nm, m) for cb, (nm, m, _s) in out.items()}


def read_fubon_conv(path):
    """富邦初級檔 →{cb: (name, 發行轉換價格)},只讀表頭有「發行轉換價格」那一段 (詢圈/競拍標的)。

    🔴 2026-11-12 濱川七 15697:競拍 11/10~11/12,富邦 11-06 檔早就寫「發行轉換價格 92.5 / 溢價率 102.3%」,
       但這支只拿富邦檔比對方式、轉換價只讀統一證 (統一證到 11-09 還沒寫),MOPS 也沒發訂價公告
       → DB conv_price 空 → 投標三天都「競拍建議價無法計算」。原本讀元富信的 gmail_fetch_masterlink 只搜
       元富黃婉濃的信箱,2026 起這份檔改由富邦轉寄,那條線 09-09 後就沒再跑。
    三段版面不同 (送件段第 10 欄是生效日、董事會段沒有轉換價) → 一律看各段表頭找欄,找不到就整段跳過。"""
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    ws = wb['CB初級市場資訊']
    out, ci, pi, blank = {}, None, None, 0
    for r in ws.iter_rows(values_only=True):
        if all(c is None or str(c).strip() == '' for c in r[:13]):
            blank += 1
            if blank >= 40:
                break
            continue
        blank = 0
        cells = [re.sub(r'\s+', '', str(c or '')) for c in r]
        c0 = cells[0] if cells else ''
        if c0 == 'CB代碼':                                  # 表頭 → 這段有沒有「發行轉換價格」
            ci = next((i for i, h in enumerate(cells) if '發行轉換' in h), None)
            pi = next((i for i, h in enumerate(cells) if '溢價率' in h), None)
            continue
        if not c0.isdigit():
            if c0:
                ci = pi = None                             # 新段落標題 → 等下一個表頭
            continue
        if ci is None or ci >= len(r) or len(c0) < 4:
            continue
        try:
            cv = float(str(r[ci]).replace(',', ''))
        except (TypeError, ValueError):
            continue                                       # 「未定」之類
        # 溢價率:訂價後是數字 1.023 (=102.3%);還沒訂是「102%~110%」區間字串 → 不收
        prem = None
        if pi is not None and pi < len(r):
            try:
                pv = float(str(r[pi]).replace(',', ''))
                prem = pv if 0.9 < pv < 3 else (pv / 100 if 90 < pv < 300 else None)
            except (TypeError, ValueError):
                prem = None
        if 0.01 < cv < 100000:
            out[c0] = (str(r[1] or '').strip(), cv, prem)
    wb.close()
    return out


def bank_short(s):
    """'台新銀行' → '台新銀'、'台新國際商業銀行股份有限公司' → '台新銀'、'中國信託' → '中信銀';多家 '、'/'/' 分隔各自縮。
    跟 issued.underwriter 括號慣例一致 ('凱基證(永豐銀)'、'華南永昌證(土地銀)')。"""
    out = []
    s = re.sub(r'[（(][^）)]*[）)]', '', str(s or ''))        # '台新銀行(保證50%)' 這種括號註記去掉
    for p in re.split(r'[、/,，&＆]', s):
        p = re.sub(r'股份有限公司|分公司.*$', '', p.strip())
        # 'TCRI 4/台新銀行' 混寫 → TCRI 那段不是銀行;無/未定/依委任 也不是
        if not p or re.match(r'^TCRI', p, re.I) or re.match(r'^(無|未定|依委任|-|—|－)', p):
            continue
        if p.startswith('中國信託'):
            p = '中信銀'
        else:
            p = re.sub(r'(國際)?(商業)?(儲蓄)?銀行$', '銀', p)
        out.append(p)
    return '/'.join(dict.fromkeys(out))


def bank_key(s):
    """比對用:'上海商銀'/'上海銀'/'上海商業儲蓄銀行' 都 → {'上海'};'中國信託'→'中信';多家 → 集合。
    DB 舊值寫法不一 (陽信商銀/國泰世華/土地銀行),只比核心名,免得同一家每晚報衝突。"""
    out = set()
    for p in (bank_short(s) or '').split('/'):
        p = re.sub(r'(商)?銀$', '', p.strip())
        if p:
            out.add(p)
    return frozenset(out)


_GTOR_MARK_RE = re.compile(r'\[gtor (auto-filled from 富邦初級檔 \d{8}: ([^\]]+)|removed[^\]]*)\]')


def our_gtor(note):
    """issued.note 裡最後一次擔保銀行動作是我們從富邦初級檔寫的 → 回那個銀行縮寫;否則 None。
    用來分辨括號裡的銀行是【我們自動補的】(富邦改口可以改/撤) 還是別的來源/手填 (只報不改)。"""
    ms = list(_GTOR_MARK_RE.finditer(str(note or '')))
    return ms[-1].group(2) if ms and ms[-1].group(2) else None


fb_conv_provisional = _dbm.fb_conv_provisional     # 富邦補的轉換價 = 暫定值 (定義見 db.py)
conv_provisional = _dbm.conv_provisional           # + 稿本暫定 / 董事會暫定 (定義見 db.py)
uni_conv_own = _dbm.uni_conv_own                   # 統一證券商檔寫的、權威來源還沒確認的值 (定義見 db.py)


# ── 富邦「發行轉換價格」填錯欄防呆 (2026-11-15 二審) ──
# 背景:富邦 10-30 檔 大聯大四 37024「發行轉換價格」寫 115.8 = 溢價率 115.88% 填進轉換價欄 (11-06 檔改回 135)。
# 只看 |轉換價 − 溢價率×100| < 1 會誤擋 865 檔歷史 CB 裡 13 檔真的長這樣的 (母股基準價剛好 ≈ 100:
# 智崴四 106.5/1.0661、世德二 101.0/1.0177…)。→ 長得像填錯欄時,拿訂價前收盤交叉驗「轉換價 ÷ 溢價率 = 基準價」;
# 沒股價才退回窄容差 0.1。窗口/容差是拿 862 檔歷史校出來的 (845 檔在 0.5% 內吻合 1/3/5 日均價)。
_FBG_GATE = 1.0         # |轉換價 − 溢價率×100| < 1.0 才算「長得像把溢價率填進轉換價欄」
_FBG_TIGHT = 0.1        # 沒股價可驗時的窄容差 (37024 是 115.88 截成 115.8,差 0.08)
_FBG_FIT_TOL = 0.03     # 隱含基準價跟任一候選基準價 (1/3/5 日均) 最近差 > 3% → 對不上 = 填錯
_FBG_STALE_DAYS = 14    # 參考日前後 14 個日曆天內一筆收盤都沒有 → 當作沒股價
_FBG_DEFAULT_WIN = (15, 10)


def _fbg_closes(chart_json):
    """issued.fm_stock_chart_json ([{d,c,h,l}]) → [(YYYY-MM-DD, 收盤)] 依日期排序;壞資料回 []。"""
    if not chart_json:
        return []
    try:
        rows = json.loads(chart_json) if isinstance(chart_json, str) else chart_json
        out = sorted((str(x['d'])[:10], float(x['c'])) for x in rows if x.get('d') and x.get('c'))
    except (TypeError, ValueError, KeyError, AttributeError):
        return []
    return [(d, c) for d, c in out if c > 0]


def _fbg_d10(v):
    s = str(v or '')[:10]
    return s if len(s) == 10 and s[4] == '-' and s[7] == '-' else None


def _fbg_get(row, k):
    try:
        return row[k]
    except (KeyError, IndexError):
        return None


def _fbg_ref(row):
    """訂價基準日的參考日 + 交易日窗口 (前 N, 後 M):定價日/基準日 ±3;競拍 投標起日前 6~當天;
    詢圈 迄日前 1~後 5 (沒迄日用起日 前 1~後 8);方式不明 起日前 6~後 8;只剩生效日 前 5~後 15。"""
    m = str(_fbg_get(row, 'method') or '')
    for k in ('fm_conv_price_anchor_date', 'fm_conv_price_set_date'):
        if _fbg_d10(_fbg_get(row, k)):
            return _fbg_d10(_fbg_get(row, k)), (3, 3)
    bs, be = _fbg_d10(_fbg_get(row, 'fm_bid_start_date')), _fbg_d10(_fbg_get(row, 'fm_bid_end_date'))
    if '競拍' in m and bs:
        return bs, (6, 0)
    if '詢圈' in m and be:
        return be, (1, 5)
    if '詢圈' in m and bs:
        return bs, (1, 8)
    if bs:
        return bs, (6, 8)
    if _fbg_d10(_fbg_get(row, 'eff_date')):
        return _fbg_d10(_fbg_get(row, 'eff_date')), (5, 15)
    return None, _FBG_DEFAULT_WIN


def _fbg_bases(closes, ref_date, as_of=None, window=_FBG_DEFAULT_WIN):
    """窗口內每個可能的基準日 t,算「t 之前 1/3/5 個交易日收盤簡單平均」(台灣 CB 基準價就是三選一)。
    as_of (富邦檔日期) 之後的收盤不用 — 37024 後來跌到 99.6,不切會把填錯值「驗成對的」。
    參考日附近沒收盤 (走勢圖過期) → [] 當沒股價。"""
    import bisect
    from datetime import date as _date
    if not closes or not ref_date:
        return []
    as_of = str(as_of or '')
    if len(as_of) == 8 and as_of.isdigit():
        as_of = f'{as_of[:4]}-{as_of[4:6]}-{as_of[6:]}'
    if as_of:
        closes = [(d, c) for d, c in closes if d <= as_of]
    if not closes:
        return []
    try:
        ref = _date.fromisoformat(ref_date)
        if min(abs((_date.fromisoformat(d) - ref).days) for d, _ in closes) > _FBG_STALE_DAYS:
            return []
    except (TypeError, ValueError):
        return []
    ds = [d for d, _ in closes]
    cl = [c for _, c in closes]
    before, after = window
    i0 = bisect.bisect_left(ds, ref_date)
    out = []
    for t in range(max(1, i0 - before), min(len(cl), i0 + after) + 1):
        for n in (1, 3, 5):
            if t - n >= 0:
                out.append(sum(cl[t - n:t]) / n)
    return out


def fb_conv_suspicious(cv, prem, cur, as_of=None, extra_closes=None):
    """富邦「發行轉換價格」疑似填錯 → (True, 原因);看起來正常或無從判斷 → (False, '')。
    cur 要有 method / fm_stock_chart_json / fm_conv_price_anchor_date / fm_conv_price_set_date /
    fm_bid_start_date / fm_bid_end_date / eff_date (少欄不報錯,只退回窄容差)。
    extra_closes:走勢圖缺時的備援,如統一證收盤快取的單日收盤 [(date, close)]。"""
    try:
        cv = float(cv)
    except (TypeError, ValueError):
        return True, f'轉換價 {cv!r} 不是數字'
    if cv <= 0:
        return True, f'轉換價 {cv:g} ≤ 0'
    if not prem or prem <= 0:
        return False, ''
    gap = abs(cv - prem * 100)
    if gap >= _FBG_GATE:
        return False, ''                               # 不像填錯欄
    ref, win = _fbg_ref(cur)
    closes = _fbg_closes(_fbg_get(cur, 'fm_stock_chart_json')) or list(extra_closes or [])
    bases = _fbg_bases(closes, ref, as_of, win)
    ib = cv / prem                                     # 隱含基準價
    if not bases:
        if gap < _FBG_TIGHT - 1e-6:
            return True, f'≈ 溢價率×100 ({prem * 100:.2f}),差 {gap:.2f},無股價可驗'
        return False, ''
    err = min(abs(ib / b - 1) for b in bases)
    if err > _FBG_FIT_TOL:
        return True, (f'≈ 溢價率×100 ({prem * 100:.2f});隱含基準價 {ib:.2f} 跟訂價前 1/3/5 日均價 '
                      f'{min(bases):.2f}~{max(bases):.2f} 對不上 (最近差 {err:.1%})')
    return False, ''                                   # 母股基準價真的 ≈ 100 (智崴四/世德二 這種)


# ── 兩家券商檔轉換價不一致 → 拿訂價前收盤分辨誰對 (2026-11-15 三審) ──
# 統一證/富邦同一檔寫不同轉換價,歷史上共 8 次,第一版誰錯各半:
#   統一證錯:巨漢二 406.6 (印成巨漢一的價)、聯亞一 1665 (印成基準價)、欣大健康 45
#   富邦錯  :星宇二 20.63、萬潤六 1102、晶宏五 79.21/81.83、威剛九 416、大聯大四 115.8 (溢價率填錯欄)
# 以前先到先贏,錯的那個還會被當定案留到掛牌 (欣大健康整個投標期 DB 是 45,實際 54.6,理論價差 2 成)。
# 發行時轉換價 = 基準價 (訂價基準日前 1/3/5 個營業日收盤均價擇一) × 溢價率,真值會【精準】對上:
# 全庫 863 檔有 809 檔 (94%) 誤差在「進位 0.06 元 + 0.01%」內;那 8 次真值全部對上、錯值全部對不上 (最少差 0.38%)。
#   a 對得上、b 明顯對不上 (誤差 > 3 倍容差) → 回 a;反過來回 b;分不出來 / 沒股價 / 沒溢價率 → None (照舊只報不蓋)。
# 亂值 (真值 ±1~5%) 碰巧對上的機率 9~40% — 那時兩邊都對得上 → None,不會選錯。只有真值自己對不上 (約 5%:
# 溢價率抄錯、基準日不在窗口) 而錯值又碰巧對上才會選錯;C6 照樣報兩家不一致、權威來源 (MOPS/B05/FinMind) 照樣追。
def _tb_tol(v):
    return 0.06 + 1e-4 * v


def _prem_of(v):
    """issued.issue_price (存的其實是轉換溢價率,格式混:'1.023' / '102.3' / '102.3%') → 小數。
    認不得、或剛好 1.0 ('100' 多半是「100% 發行」的發行價佔位,不是溢價率) → None。"""
    try:
        p = float(str(v).replace('%', '').strip())
    except (TypeError, ValueError):
        return None
    if 90 <= p <= 300:
        p /= 100
    if not (0.9 <= p <= 3) or abs(p - 1.0) < 1e-9:
        return None
    return p


def _prem_replaces_placeholder(ip, prem):
    """issued.issue_price 欄放的是【訂價前的佔位】而不是實際轉換溢價率,而且富邦初級檔的溢價率跟它不矛盾 → 可以換掉。
    佔位有三種:詢圈發行價「100」「100~101」、董事會公告的溢價率區間「103~107」、下限「不低於102」。
    前端把這欄當溢價率顯示 →「100」會顯示成「溢價率 100%」(東聯互動一實際 105.26%),區間/下限則是空白。
    富邦的值落在區間外/低於下限 (辛耘三 區間 103~107、富邦 109.06) → 兜不攏,不換 (2026-11-15 三審)。"""
    try:
        s, p = str(ip or '').strip(), float(prem) * 100
    except (TypeError, ValueError):
        return False
    if re.fullmatch(r'100(?:\.0+)?%?', s):
        return True
    m = re.fullmatch(r'不低於\s*(\d{2,3}(?:\.\d+)?)%?', s)
    if m:
        return p >= float(m.group(1)) - 0.005
    m = re.fullmatch(r'(\d{2,3}(?:\.\d+)?)%?\s*[~～\-–]\s*(\d{2,3}(?:\.\d+)?)%?', s)
    if m:
        a, b = float(m.group(1)), float(m.group(2))
        return (a - 0.005 <= p <= b + 0.005) or (a <= 100.5 and b <= 102)      # 後者 = 發行價區間 (100~101)
    return False


def broker_conv_pick(a, b, prem, cur, as_of=None):
    """兩個候選轉換價 a / b 誰對得上訂價前收盤 → (贏的值 or None, 說明)。
    prem = 富邦初級檔的溢價率 (1.0558);cur = issued 列 (欄位同 fb_conv_suspicious);as_of = 券商檔日期 (之後的收盤不用)。"""
    try:
        a, b, prem = float(a), float(b), float(prem or 0)
    except (TypeError, ValueError):
        return None, '值不是數字'
    if a <= 0 or b <= 0 or abs(a - b) <= 0.01:
        return None, '兩值相同'
    if prem <= 0:
        return None, '沒有溢價率可驗'
    ref, win = _fbg_ref(cur)
    bases = _fbg_bases(_fbg_closes(_fbg_get(cur, 'fm_stock_chart_json')), ref, as_of, win)
    if len(bases) < 3:
        return None, '沒有訂價前收盤可驗'
    ea = min(abs(a - x * prem) for x in bases)
    eb = min(abs(b - x * prem) for x in bases)
    if ea <= _tb_tol(a) and eb > 3 * _tb_tol(b):
        return a, f'{a:g} 對得上訂價前均價×溢價率、{b:g} 差 {eb / b:.1%}'
    if eb <= _tb_tol(b) and ea > 3 * _tb_tol(a):
        return b, f'{b:g} 對得上訂價前均價×溢價率、{a:g} 差 {ea / a:.1%}'
    return None, f'訂價前收盤分不出來 ({a:g} 差 {ea / a:.1%}、{b:g} 差 {eb / b:.1%})'


def read_fubon_guarantee(path):
    """富邦初級檔 →{cb: (name, 擔保銀行原文 or None, 主辦承銷商原文)}。各段表頭找「擔保行」欄
    (欄值是「TCRI 4」= 無擔保 → None,寫銀行名 = 有擔保;空/未定/依委任 → 不收)。三段都有這欄但位置不同 → 一律看表頭。

    🔴 2026-11-12 用戶看濱川七 modal:「信評/擔保」只寫 TCRI7,其實是台新銀行擔保。
       系統慣例是把擔保銀行寫在 issued.underwriter 括號 ('凱基證(永豐銀)'),前端
       (`/\\(/.test(broker)` → guarantee YES) 和競拍建議價的相似案例比對都靠它;這資訊以前
       從元富信進來,那條線 09-09 後就斷了 → 光譜四/泰谷五/濱川七/正瀚一創 全被當成無擔保。
       競拍案有 TWSE「有擔保」旗標還救得回來,詢圈案 (正瀚一創 王道銀) 就直接被當無擔保比對。"""
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    ws = wb['CB初級市場資訊']
    out, gi, ui, blank = {}, None, None, 0
    for r in ws.iter_rows(values_only=True):
        if all(c is None or str(c).strip() == '' for c in r[:13]):
            blank += 1
            if blank >= 40:
                break
            continue
        blank = 0
        cells = [re.sub(r'\s+', '', str(c or '')) for c in r]
        c0 = cells[0] if cells else ''
        if c0 == 'CB代碼':
            gi = next((i for i, h in enumerate(cells) if '擔保行' in h), None)
            ui = next((i for i, h in enumerate(cells) if '承銷商' in h), None)
            continue
        if not c0.isdigit():
            if c0:
                gi = ui = None
            continue
        if gi is None or gi >= len(r) or len(c0) < 4:
            continue
        if c0 in out:
            continue                                       # 同檔出現在兩段 → 取前段 (較新),不管前段寫銀行還是 TCRI
        g = str(r[gi] or '').strip()
        if re.search(r'銀|信託|農會|合庫|郵局|金庫', g) and bank_short(g):
            bank = g                                       # 有擔保
        elif re.search(r'TCRI|無擔', g, re.I) or g.startswith('無'):
            bank = None                                    # 明寫無擔保 → main() 用來撤掉我們以前補錯的銀行
        else:
            continue                                       # 空/未定/依委任 → 不知道,不記 (後段有值可以補)
        u = str(r[ui] or '').strip() if (ui is not None and ui < len(r)) else ''
        out[c0] = (str(r[1] or '').strip(), bank, u)
    wb.close()
    return out


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
    conv_hint = {}   # {cb: 轉換價不一致時「為什麼沒蓋」的說明} — 附在 C6 訊息後面
    # 富邦初級檔的轉換價/溢價率先讀進來:下面統一證那段遇到「跟富邦暫定值不同」要拿溢價率驗訂價前收盤 (broker_conv_pick)。
    # 超過 14 天的舊檔不用 (跟下面富邦那段同一個門檻)。
    _uni_stamp = Path(path).stem[-8:]
    _fb_stamp0, _fb_now, _fb_conv_raw = '', {}, None
    _fb_path0 = latest_fubon_primary()
    if _fb_path0:
        _m0 = re.search(r'(\d{8})', Path(_fb_path0).stem)
        if _m0 and (datetime.now() - datetime.strptime(_m0.group(1), '%Y%m%d')).days <= 14:
            try:
                _fb_conv_raw = read_fubon_conv(_fb_path0)
                _fb_stamp0 = _m0.group(1)
                for _cb0, (_nm0, _cv0, _pr0) in _fb_conv_raw.items():
                    _fb_now[_cb_from_name(_nm0, _cb0) or _cb0] = (_cv0, _pr0)
            except Exception as _e:
                _fb_conv_raw = None
                print(f'  [WARN] 富邦初級檔轉換價解析失敗: {_e}')
    _recent30 = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    # 代號誤植修正 (🔴 統一證 2026-08-13 把十銓五寫成 49674、09-10 把威剛九寫成 4 碼 3260)
    # 已在上面 merge 之前做完,mismatches 也在那裡收集。
    for it in rows:
        # 🔴 券商檔的代號是「股票+第N次」推的,有/無擔保混著發的公司會撞到自己更早的舊債:
        #    2026-08 起「53813 光譜三」(國內第三次有擔保) 被「僅補日期」併進 2008 合正三那列,
        #    掛著 2008 掛牌日 → _in_flight() 判成已上市 → 發行額 3 億 vs 券商檔 5 億的不一致也從沒報出來。
        #    櫃買代號 = 累計第幾檔 → 該是 53814。撞號時改指同公司在途案 (db.resolve_new_cb_code)。
        # 2026-11-03:券商檔換區段時 (光譜三 11/02 檔移到「近期掛牌」段) 只剩掛牌日、沒有生效/送件日 →
        #   舊版 _ev 空 → 不轉向 → 投標期間/評等被填進 2008 合正三那列,稽核每晚 TG 誤報。掛牌日/投標起也算事件日。
        _bs_guess = None
        for _d in (it['eff'], it['listing'], it['receipt']):
            if _d:
                try:
                    _bs_guess = parse_bid_period(it['bid_text'], int(_d[:4]))[0]
                except Exception:
                    _bs_guess = None
                break
        _ev = it.get('eff') or it.get('receipt') or _bs_guess or it.get('listing')
        if _ev and it['cb'][:4].isdigit():
            _fixed = _dbm.resolve_new_cb_code(conn, it['cb'][:4], it['cb'], _ev, '統一證券商檔')
            if _fixed != it['cb']:
                mismatches.append((it['cb'], _fixed, f'{it.get("name") or ""} (第N次推代號撞到舊債)'))
                it['cb'] = _fixed
        cur = conn.execute('SELECT * FROM issued WHERE cb_code=?', (it['cb'],)).fetchone()
        if not cur:
            continue  # 不在 issued (新案由 scan_cb_disclosures 處理)
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
        elif it['listing'] and _in_flight(cur) and str(cur['listing_date'] or '')[:10] != str(it['listing'])[:10]:
            # 2026-10-26 颱風稽核:掛牌日以前只填空值,券商檔改期 (萬旭三 07-17→07-20) 永遠進不來。
            #   詢圈的掛牌日由承銷商排、沒有官方源,券商檔的新值就是最新的 → 在途案差 ≤30 天且新日期在未來就覆寫留痕。
            #   競拍以 TWSE 為準 (fetch_twse_upcoming 會覆寫),這裡只報不改,免得兩邊打架每輪互蓋。
            _old, _new = str(cur['listing_date'] or '')[:10], str(it['listing'])[:10]
            try:
                _gap = abs((datetime.strptime(_new, '%Y-%m-%d') - datetime.strptime(_old, '%Y-%m-%d')).days)
            except ValueError:
                _gap = 999
            # 競拍以 TWSE 為準:DB 方式寫詢圈但其實是競拍 (61966 帆宣六) 時,只看 method 會跟 TWSE 每輪互蓋 →
            #   TWSE 即將開標 / 開標結果有這檔,就一律當競拍、只報不改。
            _comp = ('競拍' in str(cur['method'] or it['method'] or '')
                     or bool(conn.execute('SELECT 1 FROM upcoming_auctions WHERE cb_code=?', (it['cb'],)).fetchone())
                     or bool(conn.execute('SELECT 1 FROM auctions WHERE cb_code=?', (it['cb'],)).fetchone()))
            if (not _comp) and _gap <= 30 and _new >= datetime.now().strftime('%Y-%m-%d'):
                sets.append('listing_date=?'); vals.append(it['listing']); n_list += 1
                notes.append(f'掛牌 {_old[5:]}→{_new[5:]} (券商檔更新)')
            else:
                revisions.append((it['cb'], it['name'], '掛牌日', _old, _new))
        # 轉換價 (2026-11-15 三審改寫)。券商檔寫的值一律只算「未確認」— 權威來源 (MOPS 訂定/B05/FinMind) 會繼續追
        # (db.conv_unconfirmed),所以這裡只管兩家券商檔之間誰可以動誰:
        #   DB 空                 → 填 (留統一證標記)
        #   DB 是稿本/董事會暫定    → 統一證有值就蓋 (同值也補標記)
        #   DB 是統一證自己寫的     → 統一證改版跟著改 (巨漢二 406.6→369.8 隔週就改了,以前「只填空」跟不上);掛牌超過 30 天不跟
        #                           (掛牌後券商檔可能改登除權息調整後的價,DB 存的是發行時轉換價)
        #   DB 是富邦暫定、值相同   → 補統一證標記 (兩家一致;之後富邦再改版只報不蓋)
        #   DB 是富邦暫定、值不同   → 富邦這期也改成統一證的值 → 蓋;否則拿訂價前收盤驗 (broker_conv_pick),
        #                           統一證對得上、富邦對不上才蓋,分不出來只報 (C6)。
        #                           🔴 二審版是「統一證一律蓋富邦並定案」:審查重播出 巨漢二/聯亞一/欣大健康 三檔
        #                              富邦先填對、統一證第一版印錯蓋掉,之後誰都改不回來。
        #   其他 (權威來源/手動/來源不明) → 不同只報 (在途案)
        # 標記一定要留 — 富邦那段和 cron 追價都靠「最後一個標記是誰、值還是不是那個」判斷。
        _X = it['conv_price']
        if _X:
            _sd = cur['fm_conv_price_set_date'] if 'fm_conv_price_set_date' in k else None
            _note0 = cur['note'] if 'note' in k else None
            _has = not empty('conv_price')
            try:
                _dbv = float(cur['conv_price']) if _has else None
                _differs = (not _has) or abs(_dbv - float(_X)) > 0.01
            except (TypeError, ValueError):
                _dbv, _differs = None, True
            _is_fb = _has and fb_conv_provisional(_note0, _sd, cur['conv_price'])
            _is_prelim = _has and (not _is_fb) and conv_provisional(_note0, _sd, cur['conv_price'])
            _is_uni = _has and uni_conv_own(_note0, _sd, cur['conv_price'])
            _ld0 = str(cur['listing_date'] or '')[:10]
            _live = _in_flight(cur) or _ld0 >= _recent30
            _write, _why = False, ''
            if not _has:
                _write, _why = True, f'轉換價{_X}'
            elif _is_prelim:
                _write, _why = True, f'轉換價{cur["conv_price"]}→{_X} (統一證蓋暫定)'
            elif _is_uni:
                if _differs and _live:
                    _write, _why = True, f'轉換價{cur["conv_price"]}→{_X} (統一證改版)'
            elif _is_fb:
                if not _differs:
                    _write = True                              # 兩家一致 → 只補統一證標記
                else:
                    _fbv, _fbp = _fb_now.get(it['cb'], (None, None))
                    if _fbv and abs(float(_fbv) - float(_X)) <= 0.01:
                        _write, _why = True, f'轉換價{cur["conv_price"]}→{_X} (統一證;富邦 {_fb_stamp0} 檔也改成這個值)'
                    else:
                        _pick, _pwhy = broker_conv_pick(_X, _fbv or _dbv, _fbp or _prem_of(cur['issue_price']), cur,
                                                        as_of=max(_uni_stamp, _fb_stamp0))
                        if _pick is not None and abs(_pick - float(_X)) <= 0.01 and _live:
                            _write, _why = True, f'轉換價{cur["conv_price"]}→{_X} (統一證;{_pwhy})'
                        elif _in_flight(cur):
                            revisions.append((it['cb'], it['name'], '轉換價', cur['conv_price'], _X))
                            conv_hint[it['cb']] = f'DB 是富邦暫定值;{_pwhy}'
            elif _differs and _in_flight(cur):
                # 🔴 2026-11-15 二審清查:以前只填空、不同也不報 → 錯值一路留到掛牌。現在報出來。
                revisions.append((it['cb'], it['name'], '轉換價', cur['conv_price'], _X))
            if _write:
                if _differs:
                    sets.append('conv_price=?'); vals.append(_X); n_conv += 1
                    notes.append(_why)
                sets.append("note=COALESCE(note,'')||?")
                vals.append(_dbm.uni_conv_mark(_uni_stamp, _X, now))
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
            elif bs and _in_flight(cur) and (str(cur['fm_bid_start_date'] or '')[:10] != bs
                                             or str(cur['fm_bid_end_date'] or '')[:10] != be):
                # 期間不同只報不改:TWSE (競拍) / 公會 (詢圈) 才是權威,它們自己會覆寫改期
                revisions.append((it['cb'], it['name'], '投標/圈購期間',
                                  f"{str(cur['fm_bid_start_date'] or '')[:10]}~{str(cur['fm_bid_end_date'] or '')[:10]}",
                                  f'{bs}~{be}'))

        if not sets:
            continue
        if notes:
            sets.append('last_status_update=?'); vals.append(now)
            sets.append('last_status_note=?'); vals.append(' / '.join(notes))
        sets.append('updated_at=?'); vals.append(now)
        vals.append(it['cb'])
        if not args.dry_run:
            conn.execute(f'UPDATE issued SET {", ".join(sets)} WHERE cb_code=?', vals)
        updates.append((it['cb'], it['name'], ' / '.join(notes) if notes else '(僅補日期/來源標記)'))

    # 方式的第二意見:富邦初級檔。統一證/富邦/DB 方式打架 → 寫進 logs/method_conflicts.json (source=broker),
    #   夜間 audit_cb_coverage 發 TG。以前「⚠ 券商檔與 DB 不一致」只印在 pulse.log,沒人會去看。
    #   方式以外的「填滿但填錯」(發行量/承銷商/年期) 也一起落地 — 聯鈞一 DB 15 億、統一證+富邦都寫 30 億,
    #   以前一樣只在 pulse.log。掛牌日/投標期間不放:TWSE/公會自己會覆寫改期。
    _fld = {'方式': 'method', '發行量': 'amount', '承銷商': 'underwriter', '年期': 'term', '轉換價': 'conv_price'}
    m_conf = [{'cb': cb, 'company': nm, 'kind': 'method' if label == '方式' else 'attr', 'field': _fld[label],
               'db': old_v, 'other': new_v,
               'msg': f'DB {label} {old_v} vs 統一證檔 {new_v}'
                      + (f' ({conv_hint[cb]})' if label == '轉換價' and conv_hint.get(cb) else '')}
              for cb, nm, label, old_v, new_v in revisions if label in _fld]
    fb_path = latest_fubon_primary()
    fb, fb_conv = {}, {}
    fb_stamp = Path(fb_path).stem[-8:] if fb_path else ''

    def _fb_attr_conf(cbx, company, field, label, db_raw, db_shown, other, hint=''):
        # db 存【DB 原值】:audit C6 拿它跟 DB 現值比,DB 改過 = 衝突已解就不再報
        m_conf.append({'cb': cbx, 'company': company, 'kind': 'attr', 'field': field, 'db': db_raw, 'other': other,
                       'msg': f'DB {label} {db_shown} vs 富邦初級檔 {other} ({fb_stamp})' + (f' ({hint})' if hint else '')})
    if fb_path:
        # 富邦停寄後別拿舊檔一直比 (舊檔的方式可能早就被改版):超過 14 天就不用
        _fm = re.search(r'(\d{8})', Path(fb_path).stem)
        _age = (datetime.now() - datetime.strptime(_fm.group(1), '%Y%m%d')).days if _fm else 999
        if _age > 14:
            print(f'  [WARN] 富邦初級檔最新是 {Path(fb_path).name} ({_age} 天前) → 太舊不比對方式')
        else:
            try:
                fb = read_fubon_methods(fb_path)
                print(f'富邦初級檔: {Path(fb_path).name} ({len(fb)} 檔有方式)')
            except Exception as _e:
                print(f'  [WARN] 富邦初級檔解析失敗: {_e}')
            if _fb_conv_raw is not None and fb_path == _fb_path0:
                fb_conv = _fb_conv_raw                  # 上面統一證那段前已經讀過同一份
            else:
                try:
                    fb_conv = read_fubon_conv(fb_path)
                except Exception as _e:
                    print(f'  [WARN] 富邦初級檔轉換價解析失敗: {_e}')
            try:
                fb_gtor = read_fubon_guarantee(fb_path)
            except Exception as _e:
                fb_gtor = {}
                print(f'  [WARN] 富邦初級檔擔保行解析失敗: {_e}')
            # 擔保銀行 → underwriter 括號 (系統慣例 '凱基證(永豐銀)';見 read_fubon_guarantee)
            #   括號沒有 / 空 / 佔位 (未定、有擔保、依委任) → 補;括號是【我們上次從富邦補的】(note 標記) → 富邦改口就跟著改、
            #   富邦改寫 TCRI (無擔保) 就撤掉;括號是別的來源/手填的銀行 → 只報不改 (method_conflicts → 夜間 C6 TG)。
            #   承銷商本體不知道時不寫 '未定(台新銀)' 這種佔位 — 等統一證/富邦有主辦承銷商再說。
            for cb0, (nm, g, u_fb) in fb_gtor.items():
                cbx = _cb_from_name(nm, cb0) or cb0
                cur = conn.execute('''SELECT cb_code, company, listing_date, underwriter, note FROM issued WHERE cb_code=?
                                      AND COALESCE(is_withdrawn,0)=0 AND COALESCE(is_legacy,0)!=1''', (cbx,)).fetchone()
                if not cur or not _in_flight(cur):
                    continue
                u = str(cur['underwriter'] or '').strip()
                m_par = re.match(r'^(.*?)\(([^)]*)\)\s*$', u)
                base = (m_par.group(1) if m_par else u).strip()
                inner = m_par.group(2).strip() if m_par else None
                ours = our_gtor(cur['note'])
                is_ours = inner is not None and ours is not None and bank_key(inner) == bank_key(ours)
                placeholder = inner is None or inner in ('', '未定', '有擔', '有擔保') or inner.startswith('依委任')
                if g is None:                                  # 富邦寫無擔保 (TCRI n)
                    if inner is None or inner == '':
                        continue                               # DB 也沒寫銀行 → 一致
                    if is_ours:                                # 我們以前補的 → 撤回 (承銷商本體空/未定就回 NULL)
                        new_u = base if base and base not in _NULLISH else None
                        if not args.dry_run:
                            conn.execute('''UPDATE issued SET underwriter=?, note=COALESCE(note,'')||?, last_status_update=?,
                                            last_status_note=?, updated_at=? WHERE cb_code=?''',
                                         (new_u, f' [gtor removed: 富邦初級檔 {fb_stamp} 改寫無擔保 (原 {inner})]', now,
                                          f'擔保 {inner} 撤回 (富邦初級檔寫無擔保)', now, cbx))
                        n_attr += 1
                        updates.append((cbx, nm, f'承銷商 {u} → {new_u or "(空)"} (富邦初級檔改寫無擔保)'))
                    else:
                        _fb_attr_conf(cbx, cur['company'], 'underwriter', '擔保', u, inner, '無擔保')
                    continue
                short = bank_short(g)
                if not short:
                    continue
                if not placeholder:
                    if bank_key(inner) == bank_key(short):
                        continue                               # 同一家 (上海商銀 vs 上海銀)
                    if not is_ours:
                        _fb_attr_conf(cbx, cur['company'], 'underwriter', '擔保銀行', u, inner, short)
                        continue
                if not base or base in _NULLISH:
                    base = re.sub(r'證券$', '證', u_fb) if (u_fb and u_fb not in _NULLISH) else ''
                if not base:
                    continue                                   # 主辦承銷商還不知道 → 不寫 '未定(台新銀)'
                new_u = f'{base}({short})'
                if new_u == u:
                    continue
                if not args.dry_run:
                    conn.execute('''UPDATE issued SET underwriter=?, note=COALESCE(note,'')||?, last_status_update=?,
                                    last_status_note=?, updated_at=? WHERE cb_code=?''',
                                 (new_u, f' [gtor auto-filled from 富邦初級檔 {fb_stamp}: {short}]', now,
                                  f'擔保 {short} (富邦初級檔)', now, cbx))
                n_attr += 1
                updates.append((cbx, nm, f'承銷商 {u or "(空)"} → {new_u} (擔保 {g},富邦初級檔)'))
    # 轉換價:富邦初級檔「發行轉換價格」— 在途案 DB 空才填,填的值算【暫定】(fb_conv_provisional):
    #   富邦改版跟著改、MOPS 訂定/B05/FinMind 可以直接蓋;別的來源寫的值不同 → 只報 (C6 TG)。
    #   2026-11-12 濱川七競拍三天都沒建議價就是缺這個 (見 read_fubon_conv)
    #   2026-11-15 三審:DB 是【統一證寫的未確認值】(uni_conv_own) 且富邦不同 → 拿訂價前收盤驗 (broker_conv_pick),
    #   富邦對得上、統一證對不上才蓋 (欣大健康:統一證 45 / 富邦 54.6,收盤一驗就知道是 54.6);分不出來只報。
    #   兩段用同一個函式、同一組輸入判斷,結果互斥 → 不會你蓋我、我蓋你來回翻。
    _recent = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    _uc_date, _uc_closes = None, {}
    try:
        _uc = json.loads((HERE / 'unisec_closes_cache.json').read_text(encoding='utf-8'))
        _uc_date, _uc_closes = _uc.get('date'), (_uc.get('closes') or {})
    except Exception:
        pass
    for cb0, (nm, cv, prem) in fb_conv.items():
        cbx = _cb_from_name(nm, cb0) or cb0
        cur = conn.execute('''SELECT cb_code, company, listing_date, conv_price, issue_price, note, fm_conv_price_set_date,
                                     method, stock_code, eff_date, fm_bid_start_date, fm_bid_end_date,
                                     fm_conv_price_anchor_date, fm_stock_chart_json
                              FROM issued WHERE cb_code=?
                              AND COALESCE(is_withdrawn,0)=0 AND COALESCE(is_legacy,0)!=1''', (cbx,)).fetchone()
        if not cur:
            continue
        prov = conv_provisional(cur['note'], cur['fm_conv_price_set_date'], cur['conv_price'])
        _uni_own = uni_conv_own(cur['note'], cur['fm_conv_price_set_date'], cur['conv_price'])
        _ld = str(cur['listing_date'] or '')[:10]
        # 暫定值掛牌後 30 天內富邦改版還收 (掛牌了 _in_flight 就不看,錯的暫定值會永遠留著)
        if not (_in_flight(cur) or (prov and _ld >= _recent)):
            continue
        # 溢價率 (issue_price 欄其實存的是轉換溢價率,見 project_cb_issue_price_trap;格式照多數舊資料用小數 1.023)
        _ip0 = str(cur['issue_price'] or '').strip()
        if prem and (_ip0 in _NULLISH or _prem_replaces_placeholder(_ip0, prem)):
            if not args.dry_run:
                conn.execute('UPDATE issued SET issue_price=?, updated_at=? WHERE cb_code=?', (f'{prem:.4f}'.rstrip('0').rstrip('.'), now, cbx))
            n_attr += 1
            updates.append((cbx, nm, f'溢價率 {prem*100:.2f}% (富邦初級檔)'
                            + (f';原欄位「{_ip0}」是訂價前的佔位' if _ip0 not in _NULLISH else '')))
        try:
            _cur_cp = float(cur['conv_price']) if cur['conv_price'] not in (None, '') else 0.0
        except (TypeError, ValueError):
            _cur_cp = 0.0
        if abs(_cur_cp - cv) <= 0.01:
            continue                                   # DB 已經是這個值 (別的來源確認過) → 不用防呆也不算衝突
        # 🔴 填錯欄防呆:富邦 10-30 檔 大聯大四「發行轉換價格」115.8 = 溢價率 115.88% (正確 135)
        _xc = [(_uc_date, _uc_closes[cur['stock_code']])] if (_uc_date and cur['stock_code'] in _uc_closes) else None
        _sus, _why = fb_conv_suspicious(cv, prem, cur, as_of=fb_stamp, extra_closes=_xc)
        if _sus:
            m_conf.append({'cb': cbx, 'company': cur['company'], 'kind': 'attr', 'field': 'conv_price',
                           'db': cur['conv_price'], 'other': cv,
                           'msg': f'富邦初級檔 {fb_stamp} 轉換價 {cv:g} 疑似填錯 ({_why}) → 不採用'})
            continue
        if _cur_cp <= 0 or (prov and abs(_cur_cp - cv) > 0.01):
            _msg = f'轉換價 {cv:g} (富邦初級檔)' if _cur_cp <= 0 else f'轉換價 {_cur_cp:g}→{cv:g} (富邦初級檔改版)'
            if not args.dry_run:
                conn.execute('''UPDATE issued SET conv_price=?, note=COALESCE(note,'')||?, last_status_update=?,
                                last_status_note=?, updated_at=? WHERE cb_code=?''',
                             (cv, _dbm.fb_conv_mark(fb_stamp, cv, now), now, _msg, now, cbx))
                if prov and prem:                       # 同一列的溢價率一起跟著改版
                    conn.execute('UPDATE issued SET issue_price=? WHERE cb_code=?',
                                 (f'{prem:.4f}'.rstrip('0').rstrip('.'), cbx))
            n_conv += 1
            updates.append((cbx, nm, _msg))
        elif _uni_own and abs(_cur_cp - cv) > 0.01:
            # DB 是統一證寫的未確認值、富邦不同 → 訂價前收盤驗誰對 (同一個函式、同一組輸入,跟統一證那段結果互斥)
            _pick, _pwhy = broker_conv_pick(cv, _cur_cp, prem, cur, as_of=max(_uni_stamp, fb_stamp))
            if _pick is not None and abs(_pick - cv) <= 0.01:
                _msg = f'轉換價 {_cur_cp:g}→{cv:g} (富邦初級檔;{_pwhy})'
                if not args.dry_run:
                    conn.execute('''UPDATE issued SET conv_price=?, note=COALESCE(note,'')||?, last_status_update=?,
                                    last_status_note=?, updated_at=? WHERE cb_code=?''',
                                 (cv, _dbm.fb_conv_mark(fb_stamp, cv, now), now, _msg, now, cbx))
                n_conv += 1
                updates.append((cbx, nm, _msg))
            else:
                _fb_attr_conf(cbx, cur['company'], 'conv_price', '轉換價', cur['conv_price'], f'{_cur_cp:g}', f'{cv:g}',
                              hint=f'DB 是統一證的值;{_pwhy}')
        elif abs(_cur_cp - cv) > 0.01:
            _fb_attr_conf(cbx, cur['company'], 'conv_price', '轉換價', cur['conv_price'], f'{_cur_cp:g}', f'{cv:g}')
    for cb0, (nm, m) in fb.items():
        # 代號用名稱校正 (同統一證);撞到已掛牌舊債 (光譜三→合正三) 時 _in_flight 會擋掉,只是漏比不會寫錯
        cbx = _cb_from_name(nm, cb0) or cb0
        cur = conn.execute('''SELECT cb_code, company, method, listing_date FROM issued WHERE cb_code=?
                              AND COALESCE(is_withdrawn,0)=0 AND COALESCE(is_legacy,0)!=1''', (cbx,)).fetchone()
        if not cur or not _in_flight(cur):
            continue
        dbm = str(cur['method'] or '').strip()
        if dbm in _NULLISH:
            if not args.dry_run:
                conn.execute('''UPDATE issued SET method=?, last_status_update=?, last_status_note=?, updated_at=?
                                WHERE cb_code=?''', (m, now, f'方式 {m} (富邦初級檔)', now, cbx))
            n_method += 1
            updates.append((cbx, nm, f'方式 {m} (富邦初級檔)'))
        elif not _same_val(dbm, m, 'method'):
            m_conf.append({'cb': cbx, 'company': cur['company'], 'kind': 'method', 'field': 'method',
                           'db': dbm, 'other': m,
                           'msg': f'DB 方式 {dbm} vs 富邦初級檔 {m} ({Path(fb_path).stem[-8:]})'})

    if not args.dry_run:
        conn.commit()
        try:
            from fetch_twsa_bookbuilding import save_conflicts
            save_conflicts('broker', m_conf)
        except Exception as _e:
            print(f'  [WARN] 方式衝突落地失敗: {_e}')
    conn.close()
    _fb_conf = [c for c in m_conf if '富邦' in c['msg']]       # 統一證那幾筆下面「券商檔與 DB 不一致」會印
    if _fb_conf:
        print(f'\n⚠ 富邦初級檔與 DB 不一致 (方式/轉換價/擔保) {len(_fb_conf)} 筆 (未覆寫,夜間稽核會發 TG):')
        for c in _fb_conf:
            print(f'   {c["cb"]} {c["company"] or ""}: {c["msg"]}')
    if m_conf:
        print(f'(方式/屬性衝突 {len(m_conf)} 筆' + (' — dry-run 未寫入)' if args.dry_run else '已寫入 logs/method_conflicts.json)'))

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
