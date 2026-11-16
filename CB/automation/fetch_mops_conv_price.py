#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自動抓 CB 轉換價格 (從 MOPS 重大訊息,比 PDF 快 30 倍)

優勢:
  - MOPS 重大訊息每頁 5 KB,而 PDF 13 MB
  - HTML 結構化「(3) 訂定轉換價格為每股新台幣 1,072 元」清楚
  - 抓兩種公告:
    a) 董事會決議發行 → 「暫定」conv_price
    b) 「公告本公司...轉換公司債之轉換價格及溢價率」 → 「訂定」conv_price (權威,訂價版本)
  - 三來源 fallback 鏈 (便宜快的先): MOPS 重大訊息 → FinMind DailyOverview → B05 生效 PDF
    a) FinMind: 已掛牌交易的 CB,首筆 = 掛牌轉換價
    b) B05 公司債生效 (發行價格確認版) PDF: 生效定價後、掛牌前的權威來源,
       抓「本轉換公司債發行時之轉換價格為每股新臺幣 X 元」(全文僅一處,需 NFKC 正規化)

對象:
  - upcoming_auctions 表 (TWSE 已公告即將開標)
  - issued 表 conv_price IS NULL 且 eff_date 近 6 個月

執行:
  py -3.12 fetch_mops_conv_price.py           # auto
  py -3.12 fetch_mops_conv_price.py 47491     # 指定 CB
  py -3.12 fetch_mops_conv_price.py --force   # 覆寫已有
"""
import argparse
import io
import re
import sqlite3
import sys
import time
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path

import requests
import urllib3
from bs4 import BeautifulSoup

import mops_client as MC   # 2026-10-06:MOPS 全域節流 + Overrun 被擋頁偵測
import db as _dbm          # fb_conv_provisional:富邦初級檔補的轉換價 = 暫定值 (2026-11-15)

urllib3.disable_warnings()
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

HERE = Path(__file__).parent
DB_PATH = HERE / 'cb_data.db'
MOPS_URL = 'https://mopsov.twse.com.tw/mops/web/ajax_t05st01'
FINMIND_URL = 'https://api.finmindtrade.com/api/v4/data'
FINMIND_TOKEN_PATH = HERE / 'finmind_token.txt'
TIMEOUT = 25

ZH_NUM = {'一':1,'二':2,'三':3,'四':4,'五':5,'六':6,'七':7,'八':8,'九':9,'十':10}

# 🔴 2026-09-10:舊版只認「轉換價格…訂定/溢價率」語序,但 MOPS 標題至少還有兩種常見寫法:
#    「…轉換公司債【完成訂價】」(3260 威剛九 09-10 實例) 和「…【訂定轉換價格】」(訂定在前)。
#    兩種都回 None → scan_cb_disclosures 的 classify 不標 convprice、本檔 is_definite 也 False
#    → 訂價公告發了系統也不知道,儀表板「近5營業日剛訂價」永遠不會出現這些案。
#    負向前瞻排除「調整/重設/變更」(除權息後的轉換價格調整公告,不是訂價) 與「海外」(ECB 序號
#    會撞國內 CB 代號)。
PAT_CONV_PRICE_TITLE = re.compile(
    r'^(?!.*(?:調整|重設|變更|海外))'
    r'.*?(?:轉換價格.*?(?:溢價率|及.*?率|訂定|確定)|訂定.*?轉換價格|確定.*?轉換價格|完成訂價|訂價完成)'
)
PAT_BOARD_DECISION = re.compile(r'(?:董事會.*?(?:決議|通過|同意).*?發行|擬發行).*?(?:可轉換|轉換)\s*公司債')
PAT_CB_NUM_TITLE = re.compile(r'第\s*([一二三四五六七八九十\d]+)\s*次')

# Body parse: priority order
# 注意:「新台幣」/「新臺幣」/「新臺幤」都要接受 (MOPS 公告用字不統一)
NTD = r'新[台臺][幣幤]'
BODY_PATTERNS = [
    (re.compile(rf'訂定轉換價格為每股{NTD}\s*([\d,]+(?:\.\d+)?)\s*元'), 'mops-definite'),
    (re.compile(rf'訂定.*?轉換價格為每股{NTD}\s*([\d,]+(?:\.\d+)?)\s*元'), 'mops-definite'),
    (re.compile(rf'轉換價格.*?訂定為每股{NTD}\s*([\d,]+(?:\.\d+)?)\s*元'), 'mops-definite'),
    (re.compile(rf'暫定轉換價格為每股{NTD}\s*([\d,]+(?:\.\d+)?)\s*元'), 'mops-preliminary'),
    (re.compile(rf'暫定.*?轉換價格為每股{NTD}\s*([\d,]+(?:\.\d+)?)\s*元'), 'mops-preliminary'),
    (re.compile(rf'轉換公司債之?轉換價格暫定為[每股{NTD}\s]*([\d,]+(?:\.\d+)?)\s*元'), 'mops-preliminary'),
]


def cb_code_seq(cb_code: str) -> int | None:
    """cb_code 末碼 = 第 N 次發行。
    - 5 碼 (e.g. 47491): 末 1 碼 = 序號 1-9
    - 6 碼 (e.g. 622010 岳豐十 / 811211 至上十一): 末 2 碼 = 序號 10+
      不修的話 622010 → 取 '0' = 0,跟 MOPS 標題「第十次」(seqs=[10]) 對不上 → 排除所有公告。
    """
    s = (cb_code or '').strip()
    if not s.isdigit():
        return None
    if len(s) == 5:
        return int(s[-1])
    if len(s) == 6:
        return int(s[-2:])
    return None


def parse_cb_seqs(title: str) -> list[int]:
    seqs = []
    for m in PAT_CB_NUM_TITLE.finditer(title):
        raw = m.group(1)
        if raw.isdigit(): seqs.append(int(raw))
        elif raw in ZH_NUM: seqs.append(ZH_NUM[raw])
    return seqs


def query_mops_list(session, co_id: str, year_roc: int, month: int) -> list[dict]:
    """查某月重大訊息清單, 回傳含 seq_no/spoke_date/spoke_time 的 detail 觸發資訊

    2026-10-06:POST 改走 mops_client.post — 全域節流 + 認得 HTTP 200 的「Overrun - 查詢過於頻繁」被擋頁
    (舊版把被擋頁當成「這個月沒公告」→ 訂價公告靜默漏抓)。見 mops_client.py 檔頭。"""
    text = MC.post(session, {
        'encodeURIComponent':'1','step':'1','firstin':'1','off':'1',
        'queryName':'co_id','inpuType':'co_id','TYPEK':'all','isnew':'false',
        'co_id': str(co_id),
        'year': str(year_roc), 'month': f'{month:02d}',
    }, verify=False)
    if not text:
        return []
    soup = BeautifulSoup(text, 'html.parser')
    items = []
    for tr in soup.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) < 5: continue
        title = tds[4].get_text(' ', strip=True)
        title = re.sub(r'\s+', ' ', title)
        # 從 onclick 解析 detail 觸發參數
        onclick = ''
        for tag in tr.find_all(onclick=True):
            onclick = tag.get('onclick') or ''
            if 'seq_no' in onclick: break
        if not onclick: continue
        params = {}
        for m in re.finditer(r"(\w+)\.value\s*=\s*'([^']*)'", onclick):
            params[m.group(1)] = m.group(2)
        if 'seq_no' not in params: continue
        date = tds[2].get_text(' ', strip=True)
        items.append({
            'date': date, 'title': title,
            'co_id': params.get('co_id', co_id),
            'TYPEK': params.get('TYPEK', 'sii'),
            'seq_no': params.get('seq_no'),
            'spoke_date': params.get('spoke_date'),
            'spoke_time': params.get('spoke_time'),
        })
    return items


def fetch_mops_detail(session, item: dict) -> str:
    """抓某筆重大訊息詳細內文 (step=2),回傳純文字 (被擋/失敗回 '';2026-10-06 改走 mops_client.post)"""
    try:
        text = MC.post(session, {
            'firstin':'true','b_date':'','e_date':'',
            'TYPEK': item.get('TYPEK','sii'),
            'year':'', 'month':'', 'type':'',
            'co_id': item['co_id'],
            'spoke_date': item['spoke_date'],
            'spoke_time': item['spoke_time'],
            'seq_no': item['seq_no'],
            'MEETING_STEP':'','MODEL':'','ITEM':'',
            'step':'2','off':'1',
        }, verify=False)
        return BeautifulSoup(text, 'html.parser').get_text(' ', strip=True) if text else ''
    except Exception:
        return ''


# ── MOPS 內文轉換價:多券合併公告逐檔歸屬 (2026-11-15 二審) ──
# 🔴 舊版 parse_body_conv_price 取「第一個」價;合併公告 (第四次及第五次) 時兄弟券會拿到同一個價。
#    實測 12 則訂價公告都是一檔一則 (目前是潛在風險),兄弟券錯價都來自 B05 (見下面 B05 段)。
_ZH_DIGIT = {'零': 0, '〇': 0, '一': 1, '二': 2, '兩': 2, '三': 3, '四': 4, '五': 5,
             '六': 6, '七': 7, '八': 8, '九': 9,
             # 大寫數字 (fetch_mops_milestones.PAT_CB_NUM 也收):壹貳參肆伍陸柒捌玖,「拾」當「十」
             '壹': 1, '貳': 2, '參': 3, '肆': 4, '伍': 5, '陸': 6, '柒': 7, '捌': 8, '玖': 9}


def _zh_to_int(s: str) -> int | None:
    """「四」→4、「十」→10、「十一」→11、「二十」→20、「二十三」→23、「12」→12;認不得回 None。
    (舊 ZH_NUM 只認單字 → 「第十一次」被丟掉,6 碼 CB 如 811211 至上十一的標題序號校驗形同虛設)"""
    s = (s or '').strip().replace('拾', '十')
    if not s:
        return None
    if s.isdigit():
        return int(s)
    if '十' in s:
        a, _, b = s.partition('十')
        if len(a) > 1 or len(b) > 1 or (a and a not in _ZH_DIGIT) or (b and b not in _ZH_DIGIT):
            return None
        return (_ZH_DIGIT[a] if a else 1) * 10 + (_ZH_DIGIT[b] if b else 0)
    if len(s) == 1:
        return _ZH_DIGIT.get(s)
    return None


# 「第N次」提及 (比對對象是 _norm_body 後、已去空白的字串):
#   - 也認「第四及第五次」裡少了「次」的「第四」(後面直接接 及/暨/、/與/和 + 第):
#     實例 5284 115/04/20 標題「簽訂發行第四及第五次無擔保轉換公司債」,舊 parse_cb_seqs 只抓到 [5]。
#   - 排除「第十二屆第五次董事會」「第五次會議」(董事會屆次),
#     以及「海外第N次」「第N次海外」(ECB 序號會撞國內 CB 序號,同 PAT_CONV_PRICE_TITLE 排除海外的理由)。
_TRANCHE_MENTION = re.compile(
    r'(?<![屆外])第([一二三四五六七八九十兩〇零壹貳參肆伍陸柒捌玖拾\d]{1,3})'
    r'(?:次|(?=(?:及|暨|、|與|和|以及)第))'
    r'(?!(?:董事會|會議|臨時|股東|海外|買回))'
)
# 兩個提及之間扣掉這些字後只剩連接詞 → 同一個「合併片語」
#   (實例 8996 標題「國內第五次無擔保轉換公司債及國內第六次無擔保轉換公司債」、6101「第一次有擔保轉換公司債暨國內第二次」)
_PHRASE_FILLER = re.compile(r'中華民國境內|境內|國內|本公司|發行|有擔保|無擔保|可轉換公司債|轉換公司債|公司債|之|案')
_PHRASE_JOINER = re.compile(r'(?:及|暨|、|與|和|以及|,)')
# 通用「每股新台幣 X 元」(合併公告時拿來逐一歸屬;同一句內前面要有「轉換價格」才算轉換價)
_PRICE_ANY = re.compile(rf'每股(?:{NTD})?([\d,]+(?:\.\d+)?)元')
# 「第四次及第五次…轉換價格分別(訂定)為每股新台幣 A 元及 B 元」
_PRICE_RESPECTIVELY = re.compile(
    rf'轉換價格[^。;]{{0,12}}?分別[^。;]{{0,8}}?每股(?:{NTD})?([\d,]+(?:\.\d+)?)元'
    rf'((?:(?:及|、|與|和|,)(?:每股)?(?:{NTD})?[\d,]+(?:\.\d+)?元)+)'
)
# 句子邊界 (去空白後):句號/分號、條列「(3)」「1.」(不吃小數點 105.256)
_SENT_BOUNDARY = re.compile(r'[。;]|\(\d+\)|(?<![\d.])\d+\.(?!\d)')
# 後綴標示「…268元(國內第四次無擔保轉換公司債)」
_POST_LABEL = re.compile(r'\(([^()]{0,24})\)')


def _norm_body(body: str) -> str:
    """NFKC (全形數字/括號/冒號/分號→半形) + 去掉所有空白。
    MOPS 內文會在句中斷行 (實例 8996 115/10/01「轉換溢價\\n率為105.256%」),逐句判讀前先壓平。"""
    import unicodedata
    return re.sub(r'\s+', '', unicodedata.normalize('NFKC', body or ''))


def _tranche_mentions(t: str) -> list[tuple[int, int, int]]:
    """[(起點, 終點, 序號)],依出現順序。"""
    out = []
    for m in _TRANCHE_MENTION.finditer(t):
        n = _zh_to_int(m.group(1))
        if n is not None and 0 < n < 100:
            out.append((m.start(), m.end(), n))
    return out


def _tranche_phrases(t: str) -> list[tuple[int, int, tuple]]:
    """[(起點, 終點, (序號…依出現順序))]:相鄰提及若中間只有填充字+單一連接詞,併成同一片語 (= 多檔合併)。"""
    out = []
    for s, e, n in _tranche_mentions(t):
        if out:
            ps, pe, pseq = out[-1]
            gap = _PHRASE_FILLER.sub('', t[pe:s])
            if s - pe <= 30 and _PHRASE_JOINER.fullmatch(gap):
                out[-1] = (ps, e, pseq + ((n,) if n not in pseq else ()))
                continue
        out.append((s, e, (n,)))
    return out


def tranche_seqs_in_text(text: str) -> list[int]:
    """標題/內文提到的所有國內 CB 序號 (依出現順序、去重)。
    main() 的標題校驗可用它取代 parse_cb_seqs (多認得「第四及第五次」「第十一次」,不認董事會屆次/海外)。"""
    seen = []
    for _, _, n in _tranche_mentions(_norm_body(text)):
        if n not in seen:
            seen.append(n)
    return seen


def _parse_body_conv_price_first(body: str) -> tuple[float | None, str]:
    """舊行為 (2026-11-15 前的 parse_body_conv_price,一字未改):BODY_PATTERNS 依優先序,取第一個命中。"""
    for pat, hint in BODY_PATTERNS:
        m = pat.search(body)
        if m:
            try:
                v = float(m.group(1).replace(',', ''))
                if 0.01 < v < 100000:
                    return v, hint
            except ValueError:
                pass
    return None, 'not-found'


def _price_class(prefix: str) -> str:
    """價格所在句子的前半段 → 'preliminary'(暫定) / 'definite'(訂定/訂為/確定/訂價) / 'unknown'。"""
    if '暫定' in prefix:
        return 'preliminary'
    if re.search(r'訂定|訂為|確定|訂價', prefix):
        return 'definite'
    return 'unknown'


def parse_body_conv_price(body: str, target_seq: int | None = None) -> tuple[float | None, str | None]:
    """從 MOPS 重大訊息內文抓轉換價。

    target_seq=None → 舊行為 (BODY_PATTERNS 第一個命中;回 (價, hint) 或 (None, 'not-found'))。
    target_seq=N    → 只回「第 N 次」那一檔的價:
      ① 內文只提到第 N 次,或完全沒寫序號 → 跟舊行為完全一樣 (單檔公告;實測 12 則訂價公告全是這型)。
      ② 內文只提到別的序號、沒有第 N 次 → (None, None):這則是兄弟券的公告。
      ③ 內文同時提到多檔 (合併公告) → 每個「轉換價格…每股新台幣 X 元」歸屬到前面最近的序號片語:
         - 片語只有一檔 → 歸那一檔;片語是「第四次及第五次」→ 無法歸屬;
         - 「轉換價格分別為每股 A 元及 B 元」→ 依片語內序號出現順序一一對應 (個數要相等才算);
         - 價格後面緊接括號「(國內第四次…)」→ 以括號標示為準;
         第 N 次有唯一值 → 回它 (訂定優先於暫定);沒有 / 兩個以上不同值 → (None, None)。
    hint:'mops-definite' / 'mops-preliminary' / 'not-found'(內文沒有任何價) / None(合併公告無法歸屬或不是本檔)。"""
    if target_seq is None:
        return _parse_body_conv_price_first(body)

    t = _norm_body(body)
    phrases = _tranche_phrases(t)
    mentioned = {n for _, _, seq in phrases for n in seq}
    if not mentioned or mentioned == {target_seq}:
        return _parse_body_conv_price_first(body)          # ① 單檔公告:完全照舊
    if target_seq not in mentioned:
        return None, None                                   # ② 兄弟券的公告

    # ③ 合併公告:逐一收集轉換價並歸屬
    def _phrase_before(pos):
        best = None
        for ph in phrases:
            if ph[1] <= pos:
                best = ph
            else:
                break
        return best

    def _sent_prefix(pos):
        starts = [b.end() for b in _SENT_BOUNDARY.finditer(t, 0, pos)]
        return t[(starts[-1] if starts else 0):pos]

    def _cls_at(pos):
        # 暫定/訂定 看「價格所在子句」(逗號切開):「原暫定轉換價格為每股250元，訂定轉換價格為每股268元」
        # 的 268 不能因為同一句前面有「暫定」就被當暫定;子句判不出來才退回整句。
        pre = _sent_prefix(pos)
        c = _price_class(re.split(r'[，,]', pre)[-1])
        return c if c != 'unknown' else _price_class(pre)

    cands = []       # (值, 類別, 歸屬序號 tuple;空 tuple = 無法歸屬)
    taken = set()    # 已被「分別」句吃掉的數字位置,通用句不重複算
    for m in _PRICE_RESPECTIVELY.finditer(t):
        tail_nums = list(re.finditer(r'([\d,]+(?:\.\d+)?)元', m.group(2)))
        nums = [(m.start(1), m.group(1))] + [(m.start(2) + x.start(1), x.group(1)) for x in tail_nums]
        ph = _phrase_before(m.start())
        order = ph[2] if ph else ()
        cls = _cls_at(m.start(1))
        for i, (pos, raw) in enumerate(nums):
            taken.add(pos)
            try:
                v = float(raw.replace(',', ''))
            except ValueError:
                continue
            attr = (order[i],) if len(order) == len(nums) else ()
            cands.append((v, cls, attr))
    for m in _PRICE_ANY.finditer(t):
        if m.start(1) in taken:
            continue
        pre = _sent_prefix(m.start())
        if '轉換價格' not in pre[-60:]:
            continue                                        # 不是轉換價 (發行價格、面額、收盤價…)
        try:
            v = float(m.group(1).replace(',', ''))
        except ValueError:
            continue
        ph = _phrase_before(m.start(1))
        attr = ph[2] if ph else ()
        lab = _POST_LABEL.match(t, m.end())               # 「268元(國內第四次)」後綴標示優先
        if lab:
            post = [n for _, _, n in _tranche_mentions(lab.group(1))]
            if post:
                attr = tuple(dict.fromkeys(post))
        cands.append((v, _cls_at(m.start()), attr))

    cands = [c for c in cands if 0.01 < c[0] < 100000]
    if not cands:
        return None, 'not-found'
    mine = [c for c in cands if c[2] == (target_seq,)]
    for cls, hint in (('definite', 'mops-definite'), ('preliminary', 'mops-preliminary')):
        vals = {c[0] for c in mine if c[1] == cls}
        if not vals:
            continue
        unknown = {c[0] for c in mine if c[1] == 'unknown'}
        if len(vals) == 1 and not (unknown - vals):
            return vals.pop(), hint
        return None, None                                   # 同一檔出現兩個以上不同價 → 不猜
    return None, None


def _has_sibling_inflight(conn, cb_code: str, stock_code: str, days: int = 60) -> bool:
    """同一檔股票是否還有「兄弟券」:另一檔 CB 的生效日與本檔相差 ≤days 天;
    兩邊生效日都還空時,改看董事會決議日相同 (同一次董事會決議的詢圈+競拍兩檔)。
    給 main() 判斷「標題、內文都沒寫第幾次的訂價公告」能不能直接套用。"""
    from datetime import date as _d
    me = conn.execute('SELECT substr(eff_date,1,10), fm_board_decision_date FROM issued WHERE cb_code=?',
                      (cb_code,)).fetchone()
    if not me:
        return False
    my_eff, my_board = (me[0] or '').strip(), (me[1] or '').strip()
    for ef, bd in conn.execute('SELECT substr(eff_date,1,10), fm_board_decision_date FROM issued '
                               'WHERE stock_code=? AND cb_code<>? AND (is_withdrawn IS NULL OR is_withdrawn=0)',
                               (stock_code, cb_code)):
        ef, bd = (ef or '').strip(), (bd or '').strip()
        try:
            if my_eff and ef and abs((_d.fromisoformat(my_eff) - _d.fromisoformat(ef)).days) <= days:
                return True
        except ValueError:
            pass
        if (not my_eff or not ef) and my_board and bd and my_board[:10] == bd[:10]:
            return True
    return False


# 「定價時間」parse — 抓兩個關鍵日期
PAT_SET_DATE_FACT = re.compile(r'事實發生日\s*[::]\s*([0-9]+)[/-]([0-9]+)[/-]([0-9]+)')
PAT_SET_DATE_ANCHOR = re.compile(r'以民國\s*([0-9]+)\s*年\s*([0-9]+)\s*月\s*([0-9]+)\s*日為.{0,15}訂定基準日')


def roc_to_ad(yr: str, mo: str, day: str) -> str | None:
    """民國 YY/MM/DD → 西元 YYYY-MM-DD"""
    try:
        ad = int(yr) + 1911
        return f'{ad:04d}-{int(mo):02d}-{int(day):02d}'
    except (ValueError, TypeError):
        return None


def parse_body_pricing_dates(body: str) -> tuple[str | None, str | None]:
    """從公告 body 抓兩個定價日期:
       - set_date    = 事實發生日 (MOPS 公告當天,正式對外公告)
       - anchor_date = 訂定基準日 (個股 close 樣本截止日,定價計算用)
    """
    set_date = None
    m = PAT_SET_DATE_FACT.search(body)
    if m:
        set_date = roc_to_ad(*m.groups())
    anchor_date = None
    m2 = PAT_SET_DATE_ANCHOR.search(body)
    if m2:
        anchor_date = roc_to_ad(*m2.groups())
    return set_date, anchor_date


def load_finmind_token():
    # 雲端 (GitHub Actions) 用 FINMIND_TOKEN env var;本機用 finmind_token.txt
    import os
    tok = os.environ.get('FINMIND_TOKEN', '').strip()
    if tok:
        return tok
    try:
        return FINMIND_TOKEN_PATH.read_text(encoding='utf-8').strip()
    except OSError:
        return ''



def fetch_finmind_conv_price(session, cb_code, eff_date, token):
    """已掛牌交易的 CB,從 FinMind DailyOverview 取掛牌初始轉換價 (元/股)。
       用於 MOPS 沒有對應「訂定轉換價格」公告格式的案 (如部分有擔保競拍/海外 E 型)。
       eff_date 當 start_date 排除「代號重用」的舊債;eff_date 缺時拉寬到 2005,
       再以 >365 天斷層切出最近一段連續交易 (單一檔 CB 不可能有逾年斷層),
       取該段首筆 = 本檔初始轉換價。回傳 (conv_price, date) 或 (None, None)。"""
    if not token:
        return None, None
    start = (eff_date or '')[:10]
    if not re.match(r'\d{4}-\d{2}-\d{2}$', start):
        start = '2005-01-01'
    try:
        r = session.get(FINMIND_URL, params={
            'dataset': 'TaiwanStockConvertibleBondDailyOverview',
            'data_id': cb_code, 'start_date': start, 'token': token,
        }, timeout=TIMEOUT, verify=False)
        data = r.json().get('data') or []
    except Exception:
        return None, None
    if not data:
        return None, None
    data.sort(key=lambda x: x.get('date') or '')
    block_start = 0
    for i in range(len(data) - 1, 0, -1):
        try:
            gap = (datetime.strptime(data[i]['date'], '%Y-%m-%d')
                   - datetime.strptime(data[i-1]['date'], '%Y-%m-%d')).days
        except (ValueError, KeyError, TypeError):
            gap = 0
        if gap > 365:
            block_start = i
            break
    first = data[block_start]
    try:
        cp = float(first.get('ConversionPrice'))
    except (TypeError, ValueError):
        return None, None
    return (cp, first.get('date')) if 0.01 < cp < 100000 else (None, None)


# ── B05 公司債生效 PDF (發行價格確認版) ──────────────────────────────
# 權威定價版,生效後上傳;掛牌前 (FinMind 尚無資料) 即可取得。
# 「本轉換公司債(發行時)之轉換價格(訂)為每股新臺幣 X 元」— 「本轉換公司債」主詞鎖定本檔,
# 排除前次/同業比較與試算列。「發行時」可有可無;「訂為/為」皆接受 (宜鼎二 B05 用「訂為」)。
# PDF 文字常在「之」跟「轉換價格」中間有換行,組件間都加 \s* 緩衝。
B05_CONV_PRICE_PAT = re.compile(rf'本轉換公司債(?:\s*發行時)?\s*之\s*轉換價格\s*(?:訂為|為)\s*每股\s*{NTD}\s*([\d,]+(?:\.\d+)?)\s*元')
B05_BASIS_DATE_PAT = re.compile(r'民國\s*([0-9]+)\s*年\s*([0-9]+)\s*月\s*([0-9]+)\s*日為[^。]{0,20}基準日')


def _ym_month_dist(fn_ym, eff_iso):
    """B05 檔名 yyyymm 與 eff_date 的月份距離 — 同股多檔 CB 時對到正確那檔"""
    try:
        y1, m1 = int(fn_ym[:4]), int(fn_ym[4:6])
        y2, m2 = int(eff_iso[:4]), int(eff_iso[5:7])
        return abs((y1 * 12 + m1) - (y2 * 12 + m2))
    except (ValueError, TypeError):
        return 999


# ── B05 分次 (tranche) 判別 ── 2026-11-15
# 🔴 啓碁 6285 四/五 共用一份 B05 (封面「國內第四次…暨國內第五次…」),附件一=四的發行及轉換辦法 (268.0)、
#    附件二=五的 (242.0)。舊版逐頁找「第一個」定價句 → 五也拿到 268.0 (DB 62855 錯);晟田 4541 五/六
#    同一型 (45416 晟田六 DB=68.8,富邦/統一證都是 62.8)。合併版 B05 的分界:每一次的「發行及轉換辦法」
#    開頭有標題「國內第N次…轉換公司債發行及轉換辦法」+ 第一條「債券名稱 …第N次…(以下簡稱「本轉換公司債」)」,
#    定價句「本轉換公司債…之轉換價格為每股…」裡的「本轉換公司債」就是指最近一個定義它的那一次。
_B05_ZH = {'一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9}
_B05_SEQ = r'第\s*([一二三四五六七八九十]{1,3}|\d{1,2})\s*次'
# 辦法標題 (排除內文引用:「…發行及轉換辦法第十一條規定」「…辦法」案」「…辦法之規定」)
_B05_TITLE_RE = re.compile(_B05_SEQ + r'[^第。,，;；]{0,20}?轉換公司債之?發行及轉換辦法'
                           r'(?![第」』”"案之有部規])')
# 承銷商「發行及轉換價格計算書/說明書」標題 (計算書表格裡也有「本轉換公司債發行時之轉換價格為每股…」)
_B05_CALC_RE = re.compile(_B05_SEQ + r'[^第。,，;；]{0,20}?轉換公司債之?發行及?轉換價格(?:計算書|說明書)'
                          r'(?![第」』”"案之有部規])')
# 第一條「債券名稱」的定義句:…國內第N次無擔保轉換公司債(以下簡稱「本轉換公司債」)
_B05_DEF_RE = re.compile(_B05_SEQ + r'[^第。,，;；]{0,20}?轉換公司債[(（]以下(?:簡)?稱[「『“"]?本(?:轉換公司債|債券|公司債)')
# 定價句:strict = 舊版 B05_CONV_PRICE_PAT;loose 只在「已確定屬於目標那一次」時才採用
_B05_PRICE_NUM = rf'每股{NTD}([\d,]+(?:\.\d+)?)元'
_B05_LOOSE_PATS = [
    # 「定為」(長興二/旺矽六/大聯大四/聯電一/至上十一 B05 都用「定為」,舊版只收「訂為/為」)
    re.compile(rf'本轉換公司債(?:發行時)?之轉換價格(?:定為|訂定為)' + _B05_PRICE_NUM),
    # 「本債券發行時之轉換價格為每股」(巨漢一/二)
    re.compile(rf'本債券(?:發行時)?之轉換價格(?:訂為|定為|為)' + _B05_PRICE_NUM),
    # 「依上述方式,…轉換價格為每股」(博智一/萬潤六/聯亞一二/辛耘三四/國統五 中間夾基準價或溢價率)
    re.compile(rf'依上述方式[,，、]?[^。●○]{{0,120}}?轉換價格(?:訂為|定為|為)' + _B05_PRICE_NUM),
]
# 基準日:民國年 (2~3 位) 或西元年 (4 位)。(?<!\d) = 年份不准從數字中間開始 —
# 🔴 2026-11-15 三審:舊版 (\d{2,3})年 會從「西元2026年3月5日為轉換價格訂定基準日」(KY 公司 6862/4190 的寫法) 中間
#    咬出「026年」→ 26+1911 = 1937-03-05 寫進 fm_conv_price_anchor_date (疊圖的開標事件因此整個消失)。
_B05_BASIS_LOOSE_RE = re.compile(r'(?<!\d)(\d{2,4})年(\d{1,2})月(\d{1,2})日為[^。]{0,12}?基準日')
_B05_PAGE_MARK_RE = re.compile(r'^[\-－—–~～\s]*(?:第\s*)?\d{1,4}(?:\s*頁)?[\-－—–~～\s]*$')
_B05_SECTION_WINDOW = 4000   # 定價句距最近一個「次」錨點的最大字數 (壓掉空白後;實測正確歸屬 ≤1,976、誤歸屬 ≥8,070)
_B05_TOC_PAGES = 12          # 封面+目錄頁數:目錄的「附件X:國內第N次…發行及轉換辦法」= 本份涵蓋的次數


def _b05_zh_int(s):
    """'五'→5、'十一'→11、'二十'→20、'11'→11;認不得 → None。"""
    s = (s or '').strip()
    if s.isdigit():
        return int(s)
    if not s or any(c not in _B05_ZH and c != '十' for c in s):
        return None
    if '十' not in s:
        return _B05_ZH.get(s) if len(s) == 1 else None
    a, _, b = s.partition('十')
    if '十' in b or len(a) > 1 or len(b) > 1:
        return None
    return (_B05_ZH.get(a, 1) if a else 1) * 10 + (_B05_ZH.get(b, 0) if b else 0)


def _b05_clean_page(raw):
    """NFKC + 去掉頁首/頁尾獨立成行的頁碼,再壓掉全部空白。
    🔴 雙鍵二 4764 p132/133:「…每股新臺幣 336.8」|下一頁「3 ⏎ 元。」— 那個 3 是辦法的頁碼,
       直接串接會讀成 336.83 (正確 336.8;光譜 5381 同型 22.0→22.03)。頁碼行先剝掉才能跨頁接句。"""
    lines = [ln.strip() for ln in unicodedata.normalize('NFKC', raw or '').splitlines()]
    lines = [ln for ln in lines if ln]
    while lines and _B05_PAGE_MARK_RE.match(lines[0]):
        lines.pop(0)
    while lines and _B05_PAGE_MARK_RE.match(lines[-1]):
        lines.pop()
    return re.sub(r'\s+', '', ''.join(lines))


def _b05_page_texts(pdf_path):
    """逐頁原始文字。先用 pypdfium2 (pdfplumber 0.11 的必要相依套件,618 頁約 5~10 秒;pdfplumber 要數分鐘);
    沒裝 pypdfium2、或它開不了但檔案完整 (檔尾有 %%EOF) 才退 pdfplumber。
    半截檔 (下載中斷) 直接回 [] — pdfplumber 硬讀半截檔一份要 10~70 秒,讀出來也不完整。"""
    try:
        import pypdfium2 as pdfium
    except ImportError:
        pdfium = None
    if pdfium is not None:
        try:
            doc = pdfium.PdfDocument(str(pdf_path))
            try:
                out = []
                for i in range(len(doc)):
                    pg = doc[i]
                    tp = pg.get_textpage()
                    out.append(tp.get_text_range() or '')
                    tp.close()
                    pg.close()
                return out
            finally:
                doc.close()
        except Exception:
            if not _b05_pdf_ok(Path(str(pdf_path))):
                return []
    try:
        import pdfplumber
        with pdfplumber.open(str(pdf_path)) as pdf:
            return [pg.extract_text() or '' for pg in pdf.pages]
    except Exception:
        return []


def _b05_seqs_in(seg):
    """片段裡每個「第N次」後 30 字內接「轉換公司債」者 (「第一次暨第二次無擔保轉換公司債」兩個都收;
    「第一次有擔保普通公司債」不是 CB 不收)。"""
    out = set()
    for m in re.finditer(_B05_SEQ, seg):
        tail = seg[m.end():m.end() + 30]
        k = tail.find('轉換公司債')
        if k < 0 or '普通公司債' in tail[:k]:
            continue
        n = _b05_zh_int(m.group(1))
        if n:
            out.add(n)
    return out


def _b05_declared_seqs(clean_pages):
    """本份 B05 涵蓋哪幾次 (只看封面與目錄,不看內文 — 內文的公司債辦理情形/財報附註會提到舊債):
      (1) 封面標題「公開說明書(…第N次…轉換公司債…)」到「一、公司名稱」之前 + 「編印目的:…」;
      (2) 前 12 頁目錄的「附件X:國內第N次…發行及轉換辦法/轉換價格計算書」(封面是圖檔時靠這個,如宏致 3605)。
    抓不到 → 空集合 (= 不知道,呼叫端只收歸屬明確的定價句)。"""
    seqs = set()
    for t in clean_pages[:3]:
        k = t.find('說明書')
        if k < 0:
            continue
        seg = t[k:k + 220]
        e = re.search(r'一、(?:本)?公司名稱', seg)
        seqs |= _b05_seqs_in(seg[:e.start()] if e else seg)
        j = t.find('編印目的')
        if j >= 0:
            seg2 = t[j:j + 140]
            e2 = re.search(r'三、', seg2)
            seqs |= _b05_seqs_in(seg2[:e2.start()] if e2 else seg2)
        break
    for t in clean_pages[:_B05_TOC_PAGES]:
        for rx in (_B05_TITLE_RE, _B05_CALC_RE):
            for m in rx.finditer(t):
                if re.search(r'附件|附錄', t[max(0, m.start() - 14):m.start()]):
                    n = _b05_zh_int(m.group(1))
                    if n:
                        seqs.add(n)
    return seqs


def _b05_tranche_price(pages, target_seq, doc_ym=None):
    """在已抽好的逐頁文字裡找「第 target_seq 次」的發行時轉換價。回傳 (價, 基準日ISO, 說明)。
    doc_ym = 'YYYYMM' (B05 檔名開頭),只用來擋「單一次 B05 裡附的舊辦法」。"""
    if not pages:
        return None, None, 'no-text'
    clean = [_b05_clean_page(p) for p in pages]
    stream = ''.join(clean)
    declared = _b05_declared_seqs(clean)
    if declared and target_seq not in declared:
        return None, None, f'not-covered declared={sorted(declared)}'

    anchors = []                                     # (pos, seq)
    for rx in (_B05_TITLE_RE, _B05_CALC_RE, _B05_DEF_RE):
        for m in rx.finditer(stream):
            n = _b05_zh_int(m.group(1))
            if n:
                anchors.append((m.start(), n))
    anchors.sort()

    cands = []                                       # (pos, price, ctx, kind)
    seen = set()
    for kind, rx in [('strict', B05_CONV_PRICE_PAT)] + [('loose', r) for r in _B05_LOOSE_PATS]:
        for m in rx.finditer(stream):
            pos = m.start(1)
            if pos in seen:
                continue
            if re.search(r'暫[定訂]', stream[max(0, m.start() - 12):m.start(1)]):
                continue                             # 「暫定/暫訂為每股」= 還沒定價 (佐登四/正文七八)
            try:
                cp = float(m.group(1).replace(',', ''))
            except ValueError:
                continue
            if not (0.01 < cp < 100000):
                continue
            seen.add(pos)
            ctx = None
            for apos, n in reversed(anchors):
                if apos < m.start():
                    if m.start() - apos <= _B05_SECTION_WINDOW:
                        ctx = n
                    break
            cands.append((m.start(), cp, ctx, kind))
    cands.sort()

    def _basis(pos):
        bms = list(_B05_BASIS_LOOSE_RE.finditer(stream[max(0, pos - 1500):pos]))
        if not bms:
            return None
        y, mo, dd = bms[-1].groups()
        try:
            yy = int(y) if len(y) == 4 else int(y) + 1911      # 4 位 = 西元 (KY 公司),2~3 位 = 民國
            return datetime(yy, int(mo), int(dd)).strftime('%Y-%m-%d')
        except ValueError:
            return None

    mine = [c for c in cands if c[2] == target_seq]
    if not mine and declared == {target_seq}:
        # 單一次的 B05:辦法頁是圖檔、抓不到「次」錨點時 (萬潤六 p134-135 空白) 才接受無歸屬的定價句,
        # 而且前 1500 字內要有「…年…月…日為轉換價格訂定基準日」(= 辦法第十一條的訂價條文,
        # 財報附註裡舊債的「本轉換公司債之轉換價格定為每股 85.6 元」(晶豪科 p222) 沒有這句),
        # 基準日也要在檔名年月 ±6 個月內 (B05 附的上一次辦法基準日是舊的)。
        # 歸屬到「別的次」的 (至上十一 B05 裡附的第十次辦法 82.7) 一律不收。
        mine = []
        for c in cands:
            if c[2] is not None:
                continue
            b = _basis(c[0])
            if not b:
                continue
            if doc_ym and _ym_month_dist(doc_ym, b) > 6:
                continue
            mine.append(c)
    if not mine:
        return None, None, f'no-price-for-seq declared={sorted(declared)} cands={[(c[1], c[2]) for c in cands[:6]]}'
    vals = {c[1] for c in mine}
    if len(vals) > 1:
        return None, None, f'conflict {sorted(vals)}'
    return mine[0][1], _basis(mine[0][0]), f'ok declared={sorted(declared)} n={len(mine)}'


# ── B05 解析快取 (2026-11-15 三審) ──
# 抽 600 多頁 PDF 文字一次要 8~15 秒 CPU;cron 是低優先權,機器忙的時候牆上時間會拖到幾分鐘。
# 還沒定價的 B05 (暫定版) 每 30 分鐘被整份重讀一次、兄弟券 (正文七/八) 同一份還讀兩次 → conv_price 步驟吃掉大半時間。
#   ① 同一輪內:逐頁文字留在記憶體 (兄弟券共用一份 PDF 只抽一次)
#   ② 跨輪:「這份檔 (檔名+大小+修改時間) 解析過、沒有第 N 次的價」記在 logs/b05_parse_cache.json,檔案沒變就不重讀。
#      TWSE 重新上傳 (訂價版) 大小會變 → 重新下載成新檔名 → 新的 key,不會把後來的價藏起來。
#      解析規則 (_b05_*) 有改 → B05_PARSER_VER +1,舊紀錄全部作廢。
B05_PARSER_VER = 3
_B05_NEG_PATH = HERE / 'logs' / 'b05_parse_cache.json'
_B05_NEG_KEEP_DAYS = 200
_b05_text_memo = {}          # {(path, size, mtime): [page texts]} — 只留最近 2 份
_b05_neg_mem = None


def _b05_file_key(path):
    st = Path(str(path)).stat()
    return f'{Path(str(path)).name}|{st.st_size}|{int(st.st_mtime)}'


def _b05_pages_cached(pdf_path):
    try:
        key = _b05_file_key(pdf_path)
    except OSError:
        return _b05_page_texts(pdf_path)
    if key not in _b05_text_memo:
        while len(_b05_text_memo) >= 2:
            _b05_text_memo.pop(next(iter(_b05_text_memo)))
        _b05_text_memo[key] = _b05_page_texts(pdf_path)
    return _b05_text_memo[key]


def _b05_parse_tranche(pdf_path, target_seq):
    """(價, 基準日, 說明, 抽到文字的頁數)。頁數 0 = 檔案讀不出來 (別當成「沒有價」記進快取)。"""
    ym = Path(str(pdf_path)).name[:6]
    ym = ym if re.match(r'(?:19|20)\d{2}(?:0[1-9]|1[0-2])$', ym) else None
    pages = _b05_pages_cached(pdf_path)
    cp, basis, why = _b05_tranche_price(pages, target_seq, ym)
    return cp, basis, why, sum(1 for p in pages if (p or '').strip())


def _b05_neg_load():
    global _b05_neg_mem
    if _b05_neg_mem is None:
        try:
            import json as _json
            _b05_neg_mem = _json.loads(_B05_NEG_PATH.read_text(encoding='utf-8'))
            if not isinstance(_b05_neg_mem, dict):
                _b05_neg_mem = {}
        except Exception:
            _b05_neg_mem = {}
    return _b05_neg_mem


def _b05_neg_add(key, why):
    """記一筆「解析過、沒有價」。寫檔失敗不影響主流程 (頂多下一輪再讀一次)。"""
    try:
        import json as _json
        import os as _os
        data = _b05_neg_load()
        now = datetime.now()
        cut = (now - timedelta(days=_B05_NEG_KEEP_DAYS)).strftime('%Y-%m-%d')
        for k in [k for k, v in data.items() if str((v or {}).get('at', ''))[:10] < cut]:
            data.pop(k, None)
        data[key] = {'at': now.strftime('%Y-%m-%d %H:%M:%S'), 'why': str(why)[:200]}
        _B05_NEG_PATH.parent.mkdir(exist_ok=True)
        tmp = _B05_NEG_PATH.with_suffix('.json.tmp')
        tmp.write_text(_json.dumps(data, ensure_ascii=False, indent=0), encoding='utf-8')
        _os.replace(str(tmp), str(_B05_NEG_PATH))
    except Exception:
        pass


def parse_b05_pdf_conv_price(pdf_path, target_seq=None):
    """解析 B05 生效 PDF 的發行時轉換價,回傳 (conv_price, 基準日 ISO) 或 (None, None)。

    target_seq=None:舊行為不變 (逐頁找第一個「本轉換公司債(發行時)之轉換價格(訂)為每股…」句,命中即停)。
    target_seq=N  :只回「第 N 次」那一份發行及轉換辦法的價格 (合併版 B05 一份涵蓋兩次以上時必用):
      - 封面/目錄說本份只涵蓋別的次 → (None, None)
      - 定價句歸屬靠最近的「第N次…發行及轉換辦法」標題/計算書標題/「(以下簡稱「本轉換公司債」)」定義句;
        合併版裡歸屬不明的句子不收,同一次出現兩個不同價也不收 → (None, None),不猜。"""
    if target_seq is not None:
        cp, basis, _why, _n = _b05_parse_tranche(pdf_path, target_seq)
        return cp, basis
    try:
        import pdfplumber
        with pdfplumber.open(str(pdf_path)) as pdf:
            for pg in pdf.pages:
                raw = unicodedata.normalize('NFKC', pg.extract_text() or '')
                tc = re.sub(r'\s+', '', raw)
                if '之轉換價格為每股' not in tc and '之轉換價格訂為每股' not in tc:
                    continue
                m = B05_CONV_PRICE_PAT.search(tc)
                if not m:
                    continue
                try:
                    cp = float(m.group(1).replace(',', ''))
                except ValueError:
                    continue
                if not (0.01 < cp < 100000):
                    continue
                bm = B05_BASIS_DATE_PAT.search(tc)
                return cp, (roc_to_ad(*bm.groups()) if bm else None)
    except Exception:
        return None, None
    return None, None


def _b05_pdf_ok(path, want_size=None):
    """快取檔能不能用:大小跟 TWSE 清單一致 (清單有寫時),或檔尾有 %%EOF。
    🔴 download_pdf 串流中斷會留下 64KB 整數倍的半截檔 (4967/3260/4906/3597/4916/2316/4973/6546 共 10 份),
       舊版照樣拿最新那份去 parse → 失敗 → 回 None,而且 30 天內不重抓。"""
    try:
        size = path.stat().st_size
        if want_size:
            return size == want_size
        if size < 1024:
            return False
        with open(path, 'rb') as f:
            f.seek(max(0, size - 2048))
            return b'%%EOF' in f.read()
    except OSError:
        return False


def fetch_b05_conv_price(stock_code, eff_date, cb_code=None):
    """抓 stock 的 B05 公司債生效 PDF (發行價格確認版) 解析轉換價。
       cb_code 有給 → 用 cb_code_seq(cb_code) 只取「第 N 次」那一份辦法的價格 (合併版 B05 必要);
       eff_date 前後 6 個月內有多份 B05 時,TWSE 清單備註寫到「第N次」的優先,再依月份距離、上傳時間。
       快取檔大小對得上 TWSE 清單 (或 30 天內且檔尾完整) 才用,否則重抓。回傳 (conv_price, 基準日) 或 (None, None)。"""
    if not stock_code:
        return None, None
    target_seq = cb_code_seq(cb_code) if cb_code else None
    try:
        import fetch_prospectus_pdf as fp
        items = fp.list_prospectuses(stock_code, fp._new_session())
    except Exception:
        return None, None
    b05 = [x for x in items if x.get('type_code') == 'B05' and x.get('status') == '生效']
    if not b05:
        return None, None
    eff = (eff_date or '')[:10]
    if not re.match(r'\d{4}-\d{2}-\d{2}', eff):
        # eff_date 為空 = 本案還沒生效定價。不要 fallback 撿「最新 B05」(會撿到上一檔同股 CB 的舊 PDF,
        # 如 61876 萬潤六 eff='' 卻被填 61874 萬潤四 conv_price=127)。寧可留空,等之後生效再補。
        return None, None
    b05 = [x for x in b05 if _ym_month_dist(x['filename'][:6], eff) <= 6]
    if not b05:
        return None, None

    if target_seq is None:
        # 沒給 cb_code = 舊行為:只看月份最近的一份
        picks = [min(b05, key=lambda x: _ym_month_dist(x['filename'][:6], eff))]
    else:
        def _note_hit(x):
            # TWSE 清單備註常寫次數 (晟田「…國內第六次無擔保轉換公司債(訂價版)」、世紀鋼「國內第九次…(定價版)」)
            txt = f"{x.get('note') or ''}{x.get('detail') or ''}"
            return any(_b05_zh_int(m.group(1)) == target_seq for m in re.finditer(_B05_SEQ, txt))

        def _newest_first(x):
            try:
                return tuple(-v for v in fp._upload_key(x))
            except Exception:
                return (0,)

        # 備註點名本次的優先 → 月份近的 → 上傳新的;最多試 3 份 (備註只拿來排序,不拿來排除:
        # 晟田 B05 備註只寫第六次,內容卻含五、六兩份辦法)
        picks = sorted(b05, key=lambda x: (0 if _note_hit(x) else 1,
                                           _ym_month_dist(x['filename'][:6], eff),
                                           _newest_first(x)))[:3]

    out_dir = Path(fp.DEFAULT_OUT_DIR)
    for pick in picks:
        try:
            want = int(str(pick.get('size') or '').replace(',', ''))
        except ValueError:
            want = None
        cached = sorted(out_dir.glob(f"{pick['filename'][:6]}_{stock_code}_B05*.pdf"),
                        key=lambda p: p.stat().st_mtime, reverse=True)
        if want:
            # 清單有大小 → 只認大小相同的快取 (TWSE 重新上傳或快取是半截檔 → 大小不同 → 重抓)
            path = next((p for p in cached if _b05_pdf_ok(p, want)), None)
        else:
            path = next((p for p in cached
                         if (datetime.now().timestamp() - p.stat().st_mtime) < 86400 * 30 and _b05_pdf_ok(p)), None)
        if path is None:
            try:
                # items=items:上面剛列過清單,別讓它馬上再列一次 (doc.twse 連打會回空清單 → 白等 24 秒)
                path = Path(fp.fetch_latest_prospectus(stock_code, kind='B05', only_status='生效',
                                                       filename=pick['filename'], out_dir=str(out_dir),
                                                       items=items))
            except Exception:
                continue
            if not _b05_pdf_ok(path):
                continue                  # 下載又斷在半途 (檔尾沒有 %%EOF) → 不拿半截檔 parse
        if target_seq is None:
            return parse_b05_pdf_conv_price(path, None)
        neg_key = None
        try:
            neg_key = f'{_b05_file_key(path)}|seq{target_seq}|v{B05_PARSER_VER}'
        except OSError:
            pass
        if neg_key and neg_key in _b05_neg_load():
            continue                      # 這份檔上次解析過、沒有第 N 次的價,檔案沒變 → 不重讀 (見「B05 解析快取」)
        cp, basis, why, n_text = _b05_parse_tranche(path, target_seq)
        if cp is not None:
            return cp, basis
        if neg_key and n_text > 0:        # 檔案讀不出文字 (0 頁) 不記 — 那是讀檔失敗,不是「沒有價」
            _b05_neg_add(neg_key, why)
    return None, None


def get_targets(conn, force=False, cb_codes=None, also_missing_dates=False, months_window=6):
    cur = conn.cursor()
    if cb_codes:
        out = []
        for cb in cb_codes:
            r = cur.execute('SELECT cb_code, stock_code, conv_price, eff_date FROM issued WHERE cb_code=?', (cb,)).fetchone()
            if r: out.append(r)
        return out

    cur.execute('''SELECT u.cb_code, u.stock_code, i.conv_price, i.eff_date
                   FROM upcoming_auctions u LEFT JOIN issued i ON i.cb_code = u.cb_code
                   WHERE u.is_cancelled = 0''')
    rows = list(cur.fetchall())
    six_mo_ago = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
    cur.execute('''SELECT cb_code, stock_code, conv_price, eff_date FROM issued
                   WHERE eff_date IS NOT NULL AND substr(eff_date,1,10) >= ?
                   AND (conv_price IS NULL OR conv_price = 0)''', (six_mo_ago,))
    rows.extend(cur.fetchall())
    # 也補「進行中但 eff_date 空/舊」的案: 近 6 月才公告董事會、conv_price 空、非 legacy
    # (修補: 原本只用 eff_date 篩,漏掉 eff_date='' 的進行中案,如 52892 宜鼎二 — eff 沒填就永遠不被 targeting)
    cur.execute('''SELECT cb_code, stock_code, conv_price, eff_date FROM issued
                   WHERE (conv_price IS NULL OR conv_price = 0)
                     AND (is_legacy IS NULL OR is_legacy != 1)
                     AND fm_board_decision_date >= ?
                     AND (eff_date IS NULL OR eff_date = '' OR substr(eff_date,1,10) < ?)''',
                (six_mo_ago, six_mo_ago))
    rows.extend(cur.fetchall())
    # 也補「定價日期」缺的 (conv_price 已有但 fm_conv_price_set_date 沒抓的)
    # 用 months_window (跟 --months 一致) 而非寫死 6 個月,讓 --months 24 真的能搜近 24 月生效的案
    if also_missing_dates:
        win_ago = (datetime.now() - timedelta(days=30*months_window)).strftime('%Y-%m-%d')
        cur.execute('''SELECT cb_code, stock_code, conv_price, eff_date FROM issued
                       WHERE eff_date IS NOT NULL AND substr(eff_date,1,10) >= ?
                         AND conv_price IS NOT NULL AND conv_price > 0
                         AND (fm_conv_price_set_date IS NULL OR fm_conv_price_set_date = '')''',
                    (win_ago,))
        rows.extend(cur.fetchall())
    seen = set(); out = []
    for cb, sc, cp, ef in rows:
        if cb in seen: continue
        seen.add(cb)
        if not force and cp and cp > 0 and not also_missing_dates: continue
        out.append((cb, sc, cp, ef))
    return out


def update_db(conn, cb_code, conv_price, source, mops_date, set_date=None, anchor_date=None):
    cur = conn.cursor()
    # 不一定要更新 conv_price (若僅補日期欄位也呼叫此函數)
    if conv_price is not None:
        now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # 順便標記「狀態更新」→ HTML 已發行列表把近期填到轉換價的浮頂 + 🆕 badge
        cur.execute('''UPDATE issued SET conv_price=?,
                       fm_conv_price_set_date=COALESCE(?, fm_conv_price_set_date),
                       fm_conv_price_anchor_date=COALESCE(?, fm_conv_price_anchor_date),
                       last_status_update=?, last_status_note=?,
                       note=COALESCE(note,'')||?, updated_at=?
                       WHERE cb_code=?''',
                    (conv_price, set_date, anchor_date,
                     now_ts, f'轉換價 {conv_price}',
                     f' [conv_price auto-filled from {source} {mops_date} @ {datetime.now():%Y-%m-%d %H:%M}]',
                     now_ts,
                     cb_code))
        # source 'finmind-overview' 代表 FinMind overview 已有此 CB → 確定已掛牌,
        # mops_date 就是 FinMind 第一筆資料日 = 實際掛牌首日。
        # 覆寫掉空白 / 「未定」(那兩種狀態都已過時)。
        if source == 'finmind-overview' and mops_date:
            cur.execute('''UPDATE issued SET listing_date=?
                           WHERE cb_code=? AND (listing_date IS NULL OR listing_date='' OR listing_date='未定')''',
                        (mops_date + ' 00:00:00', cb_code))
    else:
        cur.execute('''UPDATE issued SET
                       fm_conv_price_set_date=COALESCE(?, fm_conv_price_set_date),
                       fm_conv_price_anchor_date=COALESCE(?, fm_conv_price_anchor_date),
                       updated_at=?
                       WHERE cb_code=?''',
                    (set_date, anchor_date,
                     datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                     cb_code))
    conn.commit()
    return cur.rowcount


def _save_conv_conflicts(processed, items):
    """轉換價衝突落地 logs/method_conflicts.json 的 'mops_conv' 段 → 夜間 audit_cb_coverage C6 發 TG。
    只換掉【這輪有處理】的 CB 的舊項目 (手動只跑一檔時,別把其他檔的衝突洗掉)。"""
    try:
        from fetch_twsa_bookbuilding import load_conflicts, write_conflicts
        data = load_conflicts()
        if data is None:
            print('  [WARN] method_conflicts.json 讀不出來 → 這輪轉換價衝突不寫')
            return
        old = (data.get('mops_conv') or {}).get('items') or []
        keep = [x for x in old if x.get('cb') not in processed]
        if not items and len(keep) == len(old):
            return                                         # 沒有變化就不動檔案
        data['mops_conv'] = {'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'items': keep + items}
        write_conflicts(data)
    except Exception as e:
        print(f'  [WARN] 轉換價衝突落地失敗: {e}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('cb_codes', nargs='*')
    ap.add_argument('--force', action='store_true')
    ap.add_argument('--months', type=int, default=4, help='查最近 N 個月 MOPS (default 4)')
    ap.add_argument('--also-dates', action='store_true', help='也補 conv_price 有但定價日期沒抓的案件')
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    targets = get_targets(conn, force=args.force, cb_codes=args.cb_codes or None,
                          also_missing_dates=args.also_dates,
                          months_window=args.months)
    print(f'處理 {len(targets)} 檔 CB')

    session = requests.Session()
    session.headers['User-Agent'] = 'Mozilla/5.0'
    fm_token = load_finmind_token()

    today = datetime.now()
    ok, miss = 0, 0
    conv_conflicts = []

    # ── MOPS 查詢省流量 (2026-11-15 三審) ──
    # 這步每輪對 ~25 檔各查 4 個月清單 = 100 次請求 (節流 ≈1.2 次/秒),加上固定 sleep,平日白天常撞 600s 逾時,
    # 排後面的案每輪都輪不到。其中 3 個月是【已結束的月份】,清單不會再變:
    #   ① 已結束月份先看共用月清單快取 (mops_client.CACHE;milestone 步驟每輪都在養,只存標題) —
    #      標題沒有訂價/董事會公告 → 這個月不用查 (要內文參數時才打清單)。快取沒中/沒有明確答覆 → 照舊查。
    #   ② 同一輪內同一檔股票的同一個月只查一次 (兄弟券:正文七/八、兆聯一/二…),內文也只抓一次。
    #   ③ 拿掉每次請求後的固定 sleep — mops_client 的全域節流閥已經在控速。
    _list_memo, _body_memo = {}, {}

    def _mops_list(co, yr, mo):
        key = (str(co), int(yr), int(mo))
        if key in _list_memo:
            return _list_memo[key]
        items = None
        try:
            if MC.month_closed(yr, mo):
                heads, st = MC.fetch_month_retry(session, co, yr, mo, tries=3)
                if st in MC.FINAL and not any(
                        PAT_CONV_PRICE_TITLE.search(_t) or PAT_BOARD_DECISION.search(_t)
                        for _t in (re.sub(r'\s+', ' ', h.get('title') or '') for h in heads)):
                    items = []
        except Exception:
            items = None
        if items is None:
            items = query_mops_list(session, co, yr, mo)
        _list_memo[key] = items
        return items

    def _mops_body(it):
        key = (it.get('co_id'), it.get('seq_no'), it.get('spoke_date'), it.get('spoke_time'))
        if key not in _body_memo or not _body_memo[key]:
            _body_memo[key] = fetch_mops_detail(session, it)
        return _body_memo[key]

    for row in targets:
        cb_code, stock_code, current_cp, eff_date = row[0], row[1], row[2], (row[3] or '')[:10]
        print(f'\n=== {cb_code} (股 {stock_code}) — current={current_cp} eff={eff_date} ===')
        if not stock_code: print('  跳過: 缺 stock_code'); continue

        target_seq = cb_code_seq(cb_code)

        # 掃近 N 個月 MOPS
        found_cp = None; found_source = None; found_date = None
        found_set_date = None; found_anchor_date = None
        found_seqs = []
        for mo_off in range(args.months):
            dt = (today.replace(day=15) - timedelta(days=30*mo_off))
            yr_roc, mo = dt.year - 1911, dt.month
            items = _mops_list(stock_code, yr_roc, mo)
            for it in items:
                title = it['title']
                # 條件: 標題符合「轉換價格...溢價率」(訂定) 或 「董事會...決議發行...轉換公司債」(暫定)
                is_definite = bool(PAT_CONV_PRICE_TITLE.search(title))
                is_board = bool(PAT_BOARD_DECISION.search(title))
                if not (is_definite or is_board): continue
                # 海外 ECB 的董事會/訂價公告一律跳過 (PAT_BOARD_DECISION 沒排除海外;新的序號解析也不把
                #   「海外第N次」當國內序號,不先擋掉「海外第七次」標題會變成「沒寫序號」而通過校驗)
                if '海外' in title: continue
                # 「第 N 次」校驗 — 標題有序號要跟 cb_code 末碼對得上
                #   tranche_seqs_in_text:多認「第四及第五次」(5284 舊版只抓到 [5])、「第十一次」(舊版認不得),
                #   不把董事會屆次 (第十二屆第五次董事會) 當 CB 序號
                seqs = tranche_seqs_in_text(title)
                if target_seq and seqs and target_seq not in seqs: continue
                # 抓 detail body parse conv_price
                body = _mops_body(it)
                body_seqs = tranche_seqs_in_text(body)
                # 標題、內文都沒寫第幾次,而這檔股票同時有兄弟券 → 無從判斷是哪一檔的價,不採用 (改走 FinMind/B05)
                if target_seq and not seqs and not body_seqs and _has_sibling_inflight(conn, cb_code, stock_code):
                    print(f'  [SKIP] {it["date"]}「{title}」沒寫第幾次、本股有兄弟券 → 不採用')
                    continue
                # 帶 target_seq:合併公告 (第四次及第五次) 只取寫明屬於第 N 次的價;歸屬不出來回 (None, None)
                cp, hint = parse_body_conv_price(body, target_seq=target_seq)
                if cp is None:
                    if hint is None:
                        print(f'  [SKIP] {it["date"]}「{title}」多檔合併/兄弟券公告,內文無法歸屬第 {target_seq} 次 → 不採用')
                    continue
                # 順便抓「定價時間」(只對 definite 公告有意義 — 暫定公告通常還沒定基準日)
                set_d, anchor_d = (None, None)
                if 'definite' in hint:
                    set_d, anchor_d = parse_body_pricing_dates(body)
                    if len(body_seqs) > 1:
                        anchor_d = None   # 合併公告各檔基準日可能不同,parse_body_pricing_dates 只取第一個 → 不採用
                # found_seqs 用標題序號 (標題沒寫才用內文):合併公告即使已歸屬,下方 _ambiguous 仍把
                #   「暫定值 ≠ 新值」記成衝突、不自動覆寫 (真實合併訂價公告尚未實測過,先保守)
                _seqs_for_check = seqs or body_seqs
                # 「訂定」優於「暫定」: 訂定就 break,暫定先暫存繼續找看有沒有更新的訂定
                if 'definite' in hint:
                    found_cp, found_source, found_date = cp, hint, it['date']
                    found_set_date, found_anchor_date = set_d, anchor_d
                    found_seqs = _seqs_for_check
                    break
                elif not found_cp:
                    found_cp, found_source, found_date = cp, hint, it['date']
                    found_seqs = _seqs_for_check
            if found_cp and 'definite' in (found_source or ''): break

        # FinMind fallback: MOPS 無對應公告但 CB 已掛牌交易 → DailyOverview 有掛牌轉換價
        if found_cp is None:
            fm_cp, fm_date = fetch_finmind_conv_price(session, cb_code, eff_date, fm_token)
            if fm_cp is not None:
                found_cp, found_source, found_date = fm_cp, 'finmind-overview', fm_date
                print(f'  ↪ MOPS 無公告,改用 FinMind 掛牌轉換價 (首日 {fm_date})')

        # B05 公司債生效 PDF fallback: 生效定價後、掛牌前 (MOPS/FinMind 都還沒有時) 的權威來源
        if found_cp is None:
            b05_cp, b05_basis = fetch_b05_conv_price(stock_code, eff_date, cb_code)
            if b05_cp is not None and b05_basis:
                # 基準日合理性:訂價基準日該在生效日前 2 個月 ~ 後 6 個月內;對不上寧可不寫日期 (價照用)
                try:
                    _bd = (datetime.strptime(b05_basis, '%Y-%m-%d') - datetime.strptime(eff_date, '%Y-%m-%d')).days
                    if not (-60 <= _bd <= 180):
                        print(f'  [WARN] B05 基準日 {b05_basis} 離生效日 {eff_date} 太遠 → 不採用這個日期')
                        b05_basis = None
                except ValueError:
                    b05_basis = None
            if b05_cp is not None:
                found_cp, found_source, found_date = b05_cp, 'b05-pdf', (b05_basis or '')
                found_anchor_date = b05_basis or found_anchor_date
                print(f'  ↪ MOPS/FinMind 無,改用 B05 生效 PDF 定價 (基準日 {b05_basis})')

        if found_cp is None:
            print('  ❌ MOPS/FinMind/B05 都找不到 conv_price (尚未生效定價)')
            miss += 1; continue

        date_info = ''
        if found_set_date:
            date_info += f' 公告日={found_set_date}'
        if found_anchor_date:
            date_info += f' 基準日={found_anchor_date}'
        print(f'  ✓ {found_date}: {found_cp} 元 ({found_source}){date_info}')
        # DB 是暫定值 (db.conv_provisional:富邦初級檔補的 / 稿本暫定 / 董事會暫定):
        #   - 值一樣 → 照寫 (留標記+定價日 = 確認,之後不再當暫定追)
        #   - 只找到 MOPS「暫定」公告 (董事會時的暫定價) → 不動
        #   - 值不同:FinMind 掛牌 (按 CB 代號) / B05 PDF (已按第 N 次歸屬,fetch_b05_conv_price 帶 cb_code) /
        #     單一券次的 MOPS 訂定公告 → 蓋;多券合併的 MOPS 公告 → 可能是兄弟券的價,不蓋、記衝突
        #     🔴 2026-11-15 二審:啟碁四/五共用一份 B05 (附件一 268、附件二 242),舊解析器兩檔都回 268 → 已改按券次歸屬
        # 🔴 2026-11-15 三審:統一證券商檔寫的值也算「未確認」(db.conv_unconfirmed) — 以前統一證一寫就當定案,
        #    巨漢二/聯亞一/欣大健康 統一證第一版印錯,B05 後來的正確值被「保留 DB」擋掉,一路錯到掛牌。
        _nr = conn.execute('SELECT note, fm_conv_price_set_date, company FROM issued WHERE cb_code=?', (cb_code,)).fetchone()
        _prov = bool(_nr) and _dbm.conv_unconfirmed(_nr[0], _nr[1], current_cp)
        _same = bool(current_cp) and abs(current_cp - found_cp) <= 0.01
        if _prov and found_source not in ('mops-definite', 'b05-pdf', 'finmind-overview'):
            print(f'  DB={current_cp} 是暫定值,這次只有 {found_source} → 不動 (等訂定公告/B05/掛牌)')
            continue
        _ambiguous = found_source == 'mops-definite' and len(set(found_seqs)) > 1
        if _prov and not _same and _ambiguous:
            print(f'  [KEEP] DB 暫定 {current_cp} vs {found_source} {found_cp} — 多券合併公告,不蓋 (記衝突)')
            conv_conflicts.append({'cb': cb_code, 'company': _nr[2], 'kind': 'attr', 'field': 'conv_price',
                                   'db': current_cp, 'other': found_cp,
                                   'msg': f'DB 轉換價 {current_cp:g} (暫定) vs {found_source} {found_cp:g} {found_date or ""}'
                                          f' (多券合併公告,未自動覆寫)'})
            # 當場落地 (只換這一檔的舊項目):這步被 600s 逾時砍掉時,結尾那次存檔跑不到
            _save_conv_conflicts({cb_code}, [conv_conflicts[-1]])
            continue
        if _prov and not _same:
            print(f'  [FIX] DB 暫定 {current_cp} → {found_source} {found_cp} (權威來源覆寫)')
        elif current_cp and abs(current_cp - found_cp) > 0.01:
            print(f'  [WARN] DB={current_cp} vs {found_source}={found_cp} — {"覆寫" if args.force else "保留 DB"}')
            if not args.force:
                # 仍 update 日期 (即使保留 conv_price)
                if found_set_date or found_anchor_date:
                    update_db(conn, cb_code, None, None, None, found_set_date, found_anchor_date)
                    print(f'  → 仍更新定價日期欄位')
                continue
        n = update_db(conn, cb_code, found_cp, found_source, found_date, found_set_date, found_anchor_date)
        if n: ok += 1

    _save_conv_conflicts({row[0] for row in targets}, conv_conflicts)
    print(f'\n=== DONE: ✓{ok} / ❌{miss} ===')
    print('→ 跑 build_html.py + publish_cb.py 上線')


if __name__ == '__main__':
    main()
