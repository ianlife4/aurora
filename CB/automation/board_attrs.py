#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""board_attrs.py — 從 MOPS「董事會決議發行轉換公司債」公告【內文】抓發行屬性。

為什麼 (2026-09-17 用戶:「華邦電是中國信託承銷,未來請看資料也要進來看一下」):
  掃描器只看標題 → 新案進 DB 只有董事會日,方式/承銷商/發行額/年期全空,
  要等券商 xlsx (每週一次,還會寫錯代號,見 威剛九) 才補。但內文第 4/7/11/13 項都有:
    4.發行總額:發行總面額上限新台幣壹佰肆拾億元整
    7.發行期間:5年
    11.承銷方式:採詢價圈購方式辦理公開承銷
    13.承銷或代銷機構:中國信託綜合證券股份有限公司
  公告當天就能填齊,不用等。

兩個入口:
  - fill_from_announcement(conn, sess, cb, stock, board_iso, dry_run)
      scan_cb_disclosures 抓到董事會公告時直接呼叫 (即時)
  - CLI: py board_attrs.py [--dry-run] [--cbs 23445,23444] [--days 400]
      補所有「有董事會日但方式/承銷商/發行額/年期任一空」的在途案 (夜間 deep 跑,當安全網)

順手處理 ECB 污染 (2026-09-17 華邦電四血案):
  DB 23444 董事會日 2026-02-10 其實是「海外第四次」(ECB) 公告,國內第四次是 09-16 才決議。
  貿聯 36657「第七次海外」、和碩 49382「海外第二次」整檔都是 ECB 卻躺在 issued 當國內案。
  → 比對時排除標題含「海外」的公告;若 DB 董事會日只對得上 ECB 公告 → 改成國內公告日;
    若該序號【只有】ECB 公告 → 標 is_withdrawn=1 + withdrawn_note (不刪,可翻回)。

只填空欄 (COALESCE 語意) — 手填值 (聯電 23031/23032) 不動。
內文格式實測變體 (2026-09-17 抓 6 家):
  發行總額: 壹佰肆拾億元整 / 為新台幣60億元整 / 新臺幣150億元整 / 不超過新台幣貳佰億元(含) / 空白
  發行期間: 5年 / 五年 / 三年 / 暫定為五年。
  承銷方式: 採詢價圈購方式辦理公開承銷 / ::證券承銷商團以詢價圈購方式包銷 (雙冒號) / 空白
  承銷機構: 中國信託綜合證券股份有限公司 / 富邦綜合證券股份有限公司。 / 授權董事長… / 待定
  科嶠 45422+45423 一則公告涵蓋兩檔 (第二次暨第三次) → 兩檔都填同一承銷商。
"""
import argparse
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
import urllib3

import discover_new_cbs as D
import fetch_mops_conv_price as P

urllib3.disable_warnings()
HERE = Path(__file__).parent
DB_PATH = HERE / 'cb_data.db'
ATTRS = ('method', 'underwriter', 'amount', 'term')
# 「…轉換公司債變更承銷方式」公告 (2026-11-03 映興二:09-01 就公告「由競價拍賣變更為詢價圈購」,標題沒有「董事會」
#   → PAT_BOARD 不認、掃描器也不分類,DB 掛競拍兩個月)。內文格式同董事會公告,第 11 項是新方式。
#   寫法兩種都見過:映興「…公司債變更承銷方式」、華建 10-16「…公司債申請承銷方式由競價拍賣變更為詢價圈購,業經金管會同意備查」
#   (後者是第 51 款自由格式,沒有第 11 項 → apply_method_change 再用「由X變更為Y」補判)。
PAT_METHOD_CHANGE = re.compile(r'公司債.{0,20}變更承銷方式|變更.{0,30}公司債.{0,10}承銷方式|公司債.{0,30}承銷方式.{0,20}變更')
_PAT_CHANGE_TO = re.compile(r'由(?:競價拍賣|詢價圈購)變更為(詢價圈購|競價拍賣)')
METHOD_PROTECTED = {'23031', '23032'}          # 手填值保護 (聯電一二),方式永不自動改

# ── 中文數字 ──────────────────────────────────────────────────────────
_DIG = {'零': 0, '〇': 0, '一': 1, '壹': 1, '二': 2, '貳': 2, '兩': 2, '三': 3, '參': 3, '叁': 3,
        '四': 4, '肆': 4, '五': 5, '伍': 5, '六': 6, '陸': 6, '七': 7, '柒': 7, '八': 8, '捌': 8,
        '九': 9, '玖': 9}
_UNIT = {'十': 10, '拾': 10, '百': 100, '佰': 100, '千': 1000, '仟': 1000}
_BIG = {'萬': 10 ** 4, '億': 10 ** 8}
_ZH_CHARS = ''.join(_DIG) + ''.join(_UNIT) + ''.join(_BIG)


def zh_to_int(s):
    """'壹佰肆拾' → 140、'陸億伍仟萬' → 650000000、'十五' → 15、'五' → 5。不合法回 None。"""
    s = (s or '').strip()
    if not s:
        return None
    total = section = num = 0
    for ch in s:
        if ch in _DIG:
            num = _DIG[ch]
        elif ch in _UNIT:
            section += (num if num else 1) * _UNIT[ch]
            num = 0
        elif ch in _BIG:
            section += num
            num = 0
            total += section * _BIG[ch]
            section = 0
        else:
            return None
    return total + section + num


def parse_amount_yi(text):
    """發行總額文字 → 億 (float)。外幣 (ECB) 回 None。"""
    t = (text or '').replace(',', '').replace('，', '')
    if not t or re.search(r'美金|美元|USD|US\$|人民幣|日圓|歐元|港幣', t):
        return None
    v = None
    m = re.search(r'(\d+(?:\.\d+)?)\s*億', t)
    if m:
        v = float(m.group(1))
        # 「3億5千萬」「3億5000萬」
        m2 = re.search(r'億\s*(\d+(?:\.\d+)?)\s*([千仟]?)\s*萬', t)
        if m2:
            v += float(m2.group(1)) / (10 if m2.group(2) else 10000)
    else:
        m = re.search(r'(\d{9,})\s*元', t)                # 4000000000元
        if m:
            v = int(m.group(1)) / 1e8
        else:
            m = re.search(rf'([{_ZH_CHARS}]+)\s*(?:元|圓)', t)   # 壹佰肆拾億元整
            if m:
                n = zh_to_int(m.group(1))
                v = n / 1e8 if n else None
    if v is None:
        return None
    v = round(v, 4)
    return v if 0.1 <= v <= 2000 else None


def parse_term(text):
    t = text or ''
    m = re.search(r'(\d+)\s*年', t)
    if m:
        n = int(m.group(1))
    else:
        m = re.search(r'([一二三四五六七八九十]{1,3})\s*年', t)
        if not m:
            return None
        n = zh_to_int(m.group(1))
    return f'{n}Y' if n and 1 <= n <= 30 else None


def parse_method(text_method, text_price=''):
    """承銷方式 → 競拍/詢圈/公開申購/洽特定人。承銷方式空白時退看發行價格 (「依競價拍賣結果而定」)。

    🔴 競拍與詢圈【同時出現】就回 None,絕不取「先出現的那個」(2026-09-21 由田血案):
       由田 3455 一則公告發兩檔,第 11 項只寫
       「國內第一、二次…採詢價圈購及競價拍賣承銷方式辦理對外公開承銷」— 沒說哪檔哪種。
       舊版取先出現的「詢圈」套到兩檔 → 34551/34552 都變詢圈,用戶看畫面抓到。
       實際 (統一證 0921 + 富邦 0918 皆同):一=詢圈 7 億、二=競拍 3 億。
       這種一則公告兩檔、一詢圈一競拍是常態 (近一年 46 組),而序號小的是哪種【沒有規律】
       (詢圈 37 / 競拍 9),所以不能猜 — 留空讓 TWSE 開標公告 / TWSA 圈購 / 券商檔去填。
       註:競拍常搭「公開申購」(零股配售)、詢圈也可能搭,那是同一檔的兩個 tranche,不算衝突。
    """
    for src in (text_method or '', text_price or ''):
        if not str(src).strip():
            continue
        hits = []
        for pat, lab in ((r'競價拍賣|競拍', '競拍'), (r'詢價圈購|詢圈', '詢圈'),
                         (r'公開申購', '公開申購'), (r'洽特定人|洽商銷售|洽特定', '洽特定人')):
            m = re.search(pat, src)
            if m:
                hits.append((m.start(), lab))
        labs = {l for _, l in hits}
        if '競拍' in labs and '詢圈' in labs:
            return None                        # 兩種主要方式並存 = 分屬不同檔,無法歸屬 → 不猜
        if hits:
            return sorted(hits)[0][1]
    return None


# 全名 → DB 慣用簡稱 (依 issued.underwriter 近一年頻率:凱基證/富邦證/台新證/統一證/群益證/元大證…)
# 順序有意義:永豐金 在 永豐 前、華南永昌 在 華南 前、中國信託 在 中信 前。
_UW_MAP = [
    ('中國信託', '中信證'), ('中信', '中信證'), ('凱基', '凱基證'), ('富邦', '富邦證'), ('台新', '台新證'),
    ('統一', '統一證'), ('群益', '群益證'), ('元大', '元大證'), ('永豐金', '永豐金證'), ('永豐', '永豐金證'),
    ('福邦', '福邦證'), ('兆豐', '兆豐證'), ('宏遠', '宏遠證'), ('華南永昌', '華南證'), ('華南', '華南證'),
    ('國票', '國票證'), ('元富', '元富證'), ('德信', '德信證'), ('國泰', '國泰證'), ('康和', '康和證'),
    ('第一金', '第一金證'), ('玉山', '玉山證'), ('合作金庫', '合庫證'), ('合庫', '合庫證'), ('日盛', '日盛證'),
    ('大昌', '大昌證'), ('亞東', '亞東證'), ('致和', '致和證'), ('新光', '新光證'), ('光和', '光和證'),
    ('陽信', '陽信證'), ('中租', '中租證'), ('高盛', '高盛證'), ('摩根', '摩根證'), ('大和', '大和證'),
    ('麥格理', '麥格理證'), ('台中銀', '台中銀證'), ('臺銀', '臺銀證'), ('台銀', '臺銀證'),
]
_UW_REJECT = re.compile(r'授權|待定|未定|另行|視市場|全權|洽|尚未|擬於|由董事長|依.{0,6}規定')


def norm_underwriter(text):
    t = re.sub(r'[\s。;；,，、.]+$', '', (text or '').strip())
    if not t or _UW_REJECT.search(t):
        return None
    first = re.split(r'[、,，;；/／]|及其|以及|\s及\s', t)[0]      # 多家 → 第一家 (主辦)
    for key, short in _UW_MAP:
        if key in first:
            return short
    m = re.search(r'([一-龥]{2,6}?)(?:綜合|金鼎)?證券', first)  # 通用退路:XX綜合證券 → XX證
    return (m.group(1) + '證') if m else None


def parse_guarantor_bank(text):
    t = (text or '').strip()
    if not t or '銀行' not in t:
        return None
    m = re.search(r'([一-龥]{2,6}?)(?:商業儲蓄|國際商業|商業|工商|儲蓄)?銀行', t)
    return (m.group(1) + '銀') if m else None


def _field(body, key_pat):
    """抓內文「N.<key>:<text>」到下一個「N.」項目為止。容忍雙冒號、空白值、無換行。"""
    m = re.search(r'\d{1,2}\s*\.\s*' + key_pat + r'\s*[:：]+\s*(.*?)(?=\s*\n?\s*\d{1,2}\s*\.\s*[^\d\s.%]|\Z)',
                  body or '', re.S)
    return m.group(1).strip() if m else ''


def _split_by_seq(text):
    """把一個欄位的文字依「第N次」拆成各序號專屬段落,回 (per_seq, 未指明序號的共用文字)。

    科嶠 4542 是「有拆」的樣板 — 第 11 項逐行寫:
        國內第二次無擔保轉換公司債：採詢價圈購方式辦理公開承銷。
        國內第三次無擔保轉換公司債：採競價拍賣方式辦理公開承銷。
    由田 3455 是「沒拆」的樣板 —「國內第一、二次…採詢價圈購及競價拍賣…」合寫成一句。
    (「第一、二次」中間的頓號讓 PAT_CB_NUM 抓不到序號 → 自然落到共用,正是我們要的。)
    """
    per, shared = {}, []
    for line in re.split(r'[\n；;]+', text or ''):
        if not line.strip():
            continue
        seqs = set(P.parse_cb_seqs(line))
        if len(seqs) == 1:
            per.setdefault(seqs.pop(), []).append(line.strip())
        else:
            shared.append(line.strip())
    return {k: ' '.join(v) for k, v in per.items()}, ' '.join(shared)


def _field_seq(body, key_pat, target_seq):
    """回 (適用本檔的文字, 是否為本檔專屬)。
       欄位有逐序號拆分且含 target → 專屬段落;有拆分但不含 target → ('', True) 本欄不適用;
       沒拆分 → (全文, False) 由呼叫端判斷能不能共用。"""
    txt = _field(body, key_pat)
    if not txt:
        return '', False
    per, shared = _split_by_seq(txt)
    if per and target_seq:
        return (per[target_seq], True) if target_seq in per else ('', True)
    return txt, False


def parse_board_body(body, title='', target_seq=None):
    b = body or ''
    # 一則公告涵蓋多檔 (「第一次及第二次」「第二次…暨第三次」) → 共用欄位不能直接套到單一檔
    multi = len(set(P.parse_cb_seqs(title or ''))) >= 2
    amt_txt, amt_own = _field_seq(b, '發行總額', target_seq)
    mth_txt, _ = _field_seq(b, '承銷方式', target_seq)
    prc_txt, _ = _field_seq(b, '發行價格', target_seq)
    amount = parse_amount_yi(amt_txt)
    # 🔴 多檔公告的共用「發行總額」是【兩檔合計】,不是本檔的 (由田:合計上限十億,實為 7+3)。
    #    沒逐檔拆分就不填,寧可留空等券商檔。
    if amount is not None and multi and not amt_own:
        amount = None
    attrs = {
        'amount': amount,
        'term': parse_term(_field_seq(b, '發行期間', target_seq)[0]),
        'method': parse_method(mth_txt, prc_txt),
        'underwriter': norm_underwriter(_field_seq(b, r'(?:承銷或代銷機構|承銷機構|主辦承銷商|承銷商)', target_seq)[0]),
        'is_ecb': ('海外' in (title or '')) or bool(re.search(r'美金|美元|USD', amt_txt)),
    }
    if attrs['underwriter'] and ('有擔保' in (title or '') or '有擔保' in b[:400]):
        bank = (parse_guarantor_bank(_field(b, r'擔保品[^:：]*'))
                or parse_guarantor_bank(_field(b, '發行保證人')))
        if bank:
            attrs['underwriter'] = f'{attrs["underwriter"]}({bank})'
    return attrs


# ── DB ────────────────────────────────────────────────────────────────
def _empty(v):
    return v is None or str(v).strip() in ('', '未定', '0', '0.0', 'None', '未定(未定)')


def _same(a, b):
    try:
        return abs(float(a) - float(b)) < 1e-6
    except (TypeError, ValueError):
        return str(a).strip() == str(b).strip()


def apply_attrs(conn, cb, attrs, dry_run=False):
    """只填空欄 (手填值如聯電 23031/23032 不動)。回 (note, warns)。

    warns = 公告內文與 DB 現值【不一致】的欄位。不自動覆寫 — 但一定要印出來,
    否則像由田那種「DB 5 億、券商檔後來改成 7 億」的舊值會一直掛在畫面上沒人發現。
    """
    row = conn.execute('SELECT method, underwriter, amount, term FROM issued WHERE cb_code=?', (cb,)).fetchone()
    if not row:
        return None, []
    sets, vals, notes, warns = [], [], [], []
    for k in ATTRS:
        v = attrs.get(k)
        if v is None:
            continue
        if _empty(row[k]):
            sets.append(f'{k}=?')
            vals.append(v)
            notes.append(f'{k}={v}')
        elif not _same(row[k], v):
            warns.append(f'{k}: 公告={v} / DB={row[k]}')
    if sets:
        sets.append('updated_at=?')
        vals.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        vals.append(cb)
        if not dry_run:
            conn.execute(f'UPDATE issued SET {", ".join(sets)} WHERE cb_code=?', vals)
    return (' / '.join(notes) if notes else None), warns


def _iso(spoke_date):
    s = str(spoke_date or '')
    return f'{s[:4]}-{s[4:6]}-{s[6:8]}' if len(s) == 8 else None


def apply_method_change(conn, sess, cb, item, dry_run=False):
    """MOPS「變更承銷方式」公告 → 內文第 11 項的新方式覆寫 DB。回 (note|None, 說明)。

    公司自己的重大訊息是權威序最高的來源 (高於公會/稿本/券商檔),所以這裡是【覆寫】不是只報;
    但內文解析不出單一方式 (兩檔合寫/措辭沒見過) 就不動,海外 CB 與手填保護也不動。"""
    if cb in METHOD_PROTECTED:
        return None, '手填保護'
    if '海外' in (item.get('title') or '') or '轉換公司債' not in re.sub(r'\s+', '', item.get('title') or ''):
        return None, '海外CB/非轉換公司債'
    _ld = conn.execute('SELECT listing_date FROM issued WHERE cb_code=?', (cb,)).fetchone()
    _ld = str((_ld[0] if _ld else '') or '')[:10]
    if _ld and _ld != '未定' and _ld < (_iso(item.get('spoke_date')) or ''):
        return None, f'已於 {_ld} 掛牌 (公告晚於掛牌 = 代號推錯到舊債)'
    body = P.fetch_mops_detail(sess, item)
    if not body:
        return None, '內文抓不到'
    attrs = parse_board_body(body, item.get('title') or '', target_seq=P.cb_code_seq(cb))
    new = attrs.get('method')
    if not new:
        # 自由格式 (華建五 10-16):標題+內文找「由競價拍賣變更為詢價圈購」,只有一種目標方式才算數
        tos = {m.group(1) for m in _PAT_CHANGE_TO.finditer(re.sub(r'\s+', '', (item.get('title') or '') + body))}
        if len(tos) == 1:
            new = '詢圈' if tos.pop() == '詢價圈購' else '競拍'
    row = conn.execute('SELECT method FROM issued WHERE cb_code=?', (cb,)).fetchone()
    if not row:
        return None, '不在 issued'
    if not new:
        return None, '內文解析不出單一承銷方式'
    cur = str(row['method'] or '').strip()
    if new in cur:
        return None, f'DB 已是{new}'
    iso = _iso(item.get('spoke_date'))
    note = f'方式 {cur or "空"}→{new} (MOPS {iso} 公告變更承銷方式)'
    if not dry_run:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute('''UPDATE issued SET method=?, note=COALESCE(note,'')||?, last_status_update=?,
                        last_status_note=?, updated_at=? WHERE cb_code=?''',
                     (new, f' [{note} · scan_cb_disclosures @ {now[:16]}]', now, note, now, cb))
    return note, 'ok'


def _key(it):
    """公告唯一鍵。🔴 MOPS 的 seq_no【不是】唯一 — 緯穎 6669 在 2026-08-07 有三則公告都是 seq_no='1'
    (調整配股率 / 海外CB轉換基準日 / 董事會決議發行國內第一次CB),只用 (日期, seq_no) 去重會把
    董事會那則當重複丟掉 → 「找不到董事會公告」。(日期, 時間, seq_no) 才是 MOPS detail 的真正主鍵。"""
    return (str(it.get('spoke_date')), str(it.get('spoke_time')), str(it.get('seq_no')))


def find_board_items(sess, stock, months, cb_seq):
    """該公司在指定 (西元年, 月) 清單裡的「董事會決議發行轉換公司債」公告,依序號過濾。
       回 (國內清單, 海外清單)。"""
    seen, dom, ecb = set(), [], []
    for yy, mm in months:
        items = P.query_mops_list(sess, stock, yy - 1911, mm)
        if not items:
            # MOPS 被擋時回空清單不是例外 (見 scan_cb_disclosures.query_company 的血案);
            # 上市櫃公司一個月幾乎不可能零公告 → 空 = 多半被擋,等一下重試一次。
            time.sleep(1.2)
            items = P.query_mops_list(sess, stock, yy - 1911, mm)
        for it in items:
            key = _key(it)
            if key in seen:
                continue
            seen.add(key)
            t = re.sub(r'\s+', '', it['title'])
            if not D.PAT_BOARD.search(t):
                continue
            if re.search(r'更正|撤銷|停止|終止|廢止', t):
                continue
            seqs = P.parse_cb_seqs(t)
            if cb_seq and seqs and cb_seq not in seqs:
                continue
            (ecb if '海外' in t else dom).append(it)
        time.sleep(0.4)
    return dom, ecb


def _months_around(iso, span):
    y, m = int(iso[:4]), int(iso[5:7])
    out = []
    for off in range(-span, span + 1):
        yy, mm = y, m + off
        while mm < 1:
            mm += 12; yy -= 1
        while mm > 12:
            mm -= 12; yy += 1
        out.append((yy, mm))
    return out


def _months_last(n):
    today = datetime.now()
    out = []
    for i in range(n):
        d = (today.replace(day=15) - timedelta(days=30 * i))
        out.append((d.year, d.month))
    return sorted(set(out))


def fill_from_announcement(conn, sess, cb, stock, board_iso, dry_run=False, verify=False):
    """給一檔 CB:找它的董事會公告,抓內文,回填屬性。
       回 {'note': 寫了什麼|None, 'warns': [公告與DB不符], 'ecb_only': bool,
           'board_fixed': (舊,新)|None, 'skip': str|None}"""
    out = {'note': None, 'warns': [], 'ecb_only': False, 'board_fixed': None, 'skip': None}
    row = conn.execute('SELECT method, underwriter, amount, term, fm_board_decision_date FROM issued WHERE cb_code=?',
                       (cb,)).fetchone()
    if not row:
        out['skip'] = '不在 issued'
        return out
    if not any(_empty(row[k]) for k in ATTRS) and not verify:
        # 預設早退省網路 (scan 每抓到一則公告就呼叫一次)。--verify 時仍要往下比對,
        # 否則「欄位都填滿但值是錯的」永遠不會被發現 (科嶠 2.5 vs 公告 5 億就是這樣潛伏的)。
        out['skip'] = '屬性已齊'
        return out
    cb_seq = P.cb_code_seq(cb)
    dom, ecb = find_board_items(sess, stock, _months_around(board_iso, 1), cb_seq)
    if not dom:
        # 董事會月附近只有海外 (或什麼都沒有) → 放寬到近 12 個月找國內公告 (華邦電四:2 月是 ECB、9 月才國內)
        dom2, ecb2 = find_board_items(sess, stock, _months_last(12), cb_seq)
        dom = dom2
        _have = {_key(x) for x in ecb}
        ecb = ecb + [e for e in ecb2 if _key(e) not in _have]
    if not dom:
        out['ecb_only'] = bool(ecb)
        out['skip'] = '只有海外CB公告' if ecb else '找不到董事會公告'
        return out
    best = sorted(dom, key=lambda x: (str(x.get('spoke_date')), str(x.get('spoke_time'))))[-1]   # 重新決議 → 最新為準
    body = P.fetch_mops_detail(sess, best)
    time.sleep(0.3)
    if not body:
        out['skip'] = '內文抓不到'
        return out
    attrs = parse_board_body(body, best['title'], target_seq=cb_seq)
    if attrs['is_ecb']:
        out['ecb_only'] = True
        out['skip'] = '內文是外幣 (ECB)'
        return out
    out['note'], out['warns'] = apply_attrs(conn, cb, attrs, dry_run)
    if not out['note']:
        # 2026-11-03 映興二:公告明寫「採競價拍賣」、DB 也是競拍,這裡卻印「內文無可用屬性」→ 被誤讀成公告沒寫方式。
        #   有解析出值、只是跟 DB 一樣 → 照實說一致。
        if any(attrs.get(k) is not None for k in ATTRS):
            out['skip'] = '內文屬性與 DB 一致 (' + ' / '.join(f'{k}={attrs[k]}' for k in ATTRS
                                                     if attrs.get(k) is not None) + ')'
        else:
            out['skip'] = '內文無可用屬性 (授權董事長/待定/多檔合寫無法歸屬)'
    # 董事會日被 ECB 污染?DB 日期不在任何國內公告日、卻在海外公告日 → 改成最新國內公告日
    cur_bd = (row['fm_board_decision_date'] or '')[:10]
    dom_dates = {_iso(i.get('spoke_date')) for i in dom}
    ecb_dates = {_iso(i.get('spoke_date')) for i in ecb}
    best_iso = _iso(best.get('spoke_date'))
    if cur_bd and best_iso and cur_bd not in dom_dates and cur_bd in ecb_dates:
        if not dry_run:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            conn.execute('''UPDATE issued SET fm_board_decision_date=?, last_status_update=?, last_status_note=?
                            WHERE cb_code=?''',
                         (best_iso, now, f'董事會決議 {best_iso} (原 {cur_bd} 為海外CB公告,已更正)', cb))
        out['board_fixed'] = (cur_bd, best_iso)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--cbs', help='逗號分隔 cb_code,只補這些')
    ap.add_argument('--days', type=int, default=400, help='董事會日在近 N 天內的在途案 (預設 400)')
    ap.add_argument('--no-ecb-fix', action='store_true', help='不要把純海外CB列標為 withdrawn')
    ap.add_argument('--verify', action='store_true',
                    help='連屬性已齊的也拉公告比對,印出不一致 (夜間 deep 用;會多打 MOPS)')
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    if args.cbs:
        codes = [c.strip() for c in args.cbs.split(',') if c.strip()]
        ph = ','.join('?' * len(codes))
        rows = conn.execute(f'SELECT cb_code, stock_code, company, fm_board_decision_date FROM issued '
                            f'WHERE cb_code IN ({ph})', codes).fetchall()
    else:
        since = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
        rows = conn.execute('''SELECT cb_code, stock_code, company, fm_board_decision_date FROM issued
            WHERE fm_board_decision_date >= ?
              AND (is_legacy IS NULL OR is_legacy != 1) AND (is_withdrawn IS NULL OR is_withdrawn != 1)
              AND (listing_date IS NULL OR listing_date = '' OR listing_date = '未定'
                   OR substr(listing_date,1,10) >= date('now'))
              AND (? = 1 OR method IS NULL OR method = '' OR method = '未定'
                   OR underwriter IS NULL OR underwriter = '' OR underwriter LIKE '未定%'
                   OR amount IS NULL OR amount = 0 OR term IS NULL OR term = '')
            ORDER BY fm_board_decision_date''', (since, 1 if args.verify else 0)).fetchall()
    print(f'目標 {len(rows)} 檔 ('
          + ('--verify:全部在途案,連已齊的也比對' if args.verify else '有董事會日、方式/承銷商/發行額/年期任一空') + ')')

    sess = requests.Session()
    sess.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
    n_fill = n_fix = n_warn = 0
    ecb_list = []
    for r in rows:
        cb, stock, name = r['cb_code'], r['stock_code'], (r['company'] or '')[:8]
        bd = (r['fm_board_decision_date'] or '')[:10]
        if not (stock and bd):
            continue
        try:
            res = fill_from_announcement(conn, sess, cb, stock, bd, dry_run=args.dry_run, verify=args.verify)
        except Exception as e:
            print(f'  [WARN] {cb} {name}: {e}')
            continue
        if res['note']:
            n_fill += 1
            print(f'  ✓ {cb} {name:8s} {res["note"]}')
        for w in res.get('warns') or []:
            n_warn += 1
            print(f'  ⚠ {cb} {name:8s} 公告與 DB 不符 → {w}  (未覆寫,請人工確認)')
        if res['board_fixed']:
            n_fix += 1
            print(f'  🔧 {cb} {name:8s} 董事會日 {res["board_fixed"][0]} → {res["board_fixed"][1]} (原為海外CB公告)')
        if res['ecb_only']:
            ecb_list.append((cb, name))
            if not args.no_ecb_fix and not args.dry_run:
                conn.execute('UPDATE issued SET is_withdrawn=1, withdrawn_note=? WHERE cb_code=?',
                             (f'海外CB(ECB),非國內轉換公司債 — board_attrs {datetime.now():%Y-%m-%d}', cb))
        if not (res['note'] or res['board_fixed'] or res['ecb_only'] or res.get('warns')):
            print(f'  · {cb} {name:8s} {res["skip"]}')
    if not args.dry_run:
        conn.commit()
    conn.close()

    if ecb_list:
        tag = ' — 已標 is_withdrawn=1 (可翻回)' if (not args.no_ecb_fix and not args.dry_run) else ' (未動)'
        print(f'\n🔴 純海外CB (ECB) 混在 issued 當國內案 {len(ecb_list)} 檔{tag}:')
        for cb, nm in ecb_list:
            print(f'   {cb} {nm}')
    print(f'\n回填 {n_fill} / 董事會日更正 {n_fix} / ECB {len(ecb_list)}' + ('  [dry-run]' if args.dry_run else ''))


if __name__ == '__main__':
    main()
