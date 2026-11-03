#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fetch_twsa_bookbuilding.py — 從證券商公會 edoc 抓「詢圈公告」圈購期間 (詢圈 CB 的權威來源)。

來源: https://web.twsa.org.tw/edoc2/default.aspx  (ASP.NET; 查詢類型=詢圈公告 rblReportType=BookBuilding)
表格欄: 案號 / 發行公司 / 主辦承銷商 / 發行性質 / 承銷股數 / 詢圈銷售股數 / 圈購期間 / 價格 / 公告檔
用途: 「圈購期間」= 詢圈 CB 的 fm_bid_start_date~fm_bid_end_date,比統一證 xlsx 更即時權威。
      對到 issued (發行公司名→在途詢圈 CB):空欄就填;2026-10-26 起同一家有多則公告取【最新一則】(案號最大),
      在途案的 DB 值跟它不同、而且差距在 60 天內 (= 重新公告/改期,不是配錯案) 就覆寫並留痕
      (廣閎科 06-24~06-25 → 07-08~07-09 第二則公告以前永遠進不來)。差超過 60 天只報不改 (保護手填/防配錯)。
      2026-11-03 起 DB 方式【不是詢圈】的在途案也拿來對 (映興二 35972:7 月稿本寫競拍、9 月稿本改詢圈,
      DB 一直掛競拍 → 以前 `method LIKE 詢圈` 直接濾掉,公會 115075 圈購公告靜默略過、訂價窗口照競拍算)。
      公告沒被詢圈檔認走 + 公會公司名 = OpenAPI 全名 (正面確認同一家) + TWSE 沒有競拍紀錄 + 張數 = DB 發行額
      + 有生效日且圈購在其後 + 同公司 ±400 天內沒有別檔 CB → 改寫成詢圈並留痕;
      任一條不成立或缺資料 → 只報不改,寫進 logs/method_conflicts.json,夜間 audit_cb_coverage 發 TG。
      公會清單抓取失敗/0 筆時不判方式、不寫衝突檔 (清單不完整會藏掉衝突)。
      詢圈案的承銷張數 (1 張 = 10 萬) 跟 DB 發行額對不上也一併報 (只報不改)。

用法:
  py fetch_twsa_bookbuilding.py --dry-run      # 抓+對,不寫 DB (看對到誰)
  py fetch_twsa_bookbuilding.py                # 抓+對+更新 DB
  py fetch_twsa_bookbuilding.py --year 2025    # 指定年 (預設今年+去年都抓)
  py fetch_twsa_bookbuilding.py --raw          # 只印抓到的詢圈表 (debug)
"""
import argparse
import json
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HERE = Path(__file__).parent
DB = HERE / 'cb_data.db'
URL = 'https://web.twsa.org.tw/edoc2/default.aspx'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
ZH_NUM = '一二三四五六七八九十'
# 方式衝突的落地檔:這支 (source=twsa) 與 backfill_primary_dates (source=fubon) 各寫自己那段,
# audit_cb_coverage 夜間讀出來發 TG。只報不改的衝突放這裡,改寫過的不放 (改完就不衝突了)。
CONFLICTS_PATH = HERE / 'logs' / 'method_conflicts.json'
# 用戶明給的時程/方式 (memory 手填值保護),方式永遠不自動改寫
MANUAL_PROTECTED = {'23031', '23032'}
# 公司全名 (證交所/櫃買 OpenAPI 公司基本資料,同 fill_missing_stocks 補股本那三支)。
# 🔴 2026-11-03 華建五 25305:公會寫「大華建設股份有限公司」,簡稱「華建」不是它的開頭 → startswith 永遠對不到,
#    圈購 10-29~11-02 進不來、DB 一直掛競拍。全名對得上就直接算同一家。
FULLNAME_CACHE = HERE / 'logs' / 'company_fullnames.json'
FULLNAME_SRC = (
    ('https://openapi.twse.com.tw/v1/opendata/t187ap03_L', '公司代號', '公司名稱'),
    ('https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O', 'SecuritiesCompanyCode', 'CompanyName'),
    ('https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_R', 'SecuritiesCompanyCode', 'CompanyName'),
)


def _hid(name, html):
    m = re.search(r'id="' + re.escape(name) + r'"[^>]*value="([^"]*)"', html)
    return m.group(1) if m else ''


def fetch_bookbuilding(year_ce):
    """抓指定西元年的詢圈公告表 → list[dict]。ASP.NET: GET 取 token → POST rblReportType=BookBuilding。"""
    s = requests.Session()
    s.headers.update({'User-Agent': UA})
    r0 = s.get(URL, timeout=25, verify=False)
    r0.encoding = 'utf-8'
    data = {
        '__EVENTTARGET': 'ctl00$cphMain$rblReportType', '__EVENTARGUMENT': '', '__LASTFOCUS': '',
        '__VIEWSTATE': _hid('__VIEWSTATE', r0.text),
        '__VIEWSTATEGENERATOR': _hid('__VIEWSTATEGENERATOR', r0.text),
        '__EVENTVALIDATION': _hid('__EVENTVALIDATION', r0.text),
        'ctl00$cphMain$ddlYear': str(year_ce),
        'ctl00$cphMain$rblReportType': 'BookBuilding',
    }
    r1 = s.post(URL, data=data, timeout=25, verify=False)
    r1.encoding = 'utf-8'
    return parse_table(r1.text)


def parse_table(html):
    """解析詢圈 GridView。列: 0案號 1發行公司 2承銷商 3發行性質 4承銷股數 5詢圈銷售股數 6圈購期間 7價格。"""
    rows = []
    for tr in re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.S):
        cells = [re.sub(r'<[^>]+>', '', c).replace('&nbsp;', ' ').strip()
                 for c in re.findall(r'<td[^>]*>(.*?)</td>', tr, re.S)]
        if len(cells) < 8:
            continue
        m = re.match(r'(\d{4})/(\d{2})/(\d{2})\s*~\s*(\d{4})/(\d{2})/(\d{2})', cells[6])
        if not m:
            continue
        try:
            units = int(cells[4].replace(',', ''))      # CB 的「承銷股數」其實是張數 (1 張 = 10 萬)
        except ValueError:
            units = None
        rows.append({
            'seq': cells[0], 'company': cells[1], 'broker': cells[2], 'kind': cells[3],
            'bid_start': f'{m.group(1)}-{m.group(2)}-{m.group(3)}',
            'bid_end':   f'{m.group(4)}-{m.group(5)}-{m.group(6)}',
            'price': cells[7], 'units': units,
        })
    return rows


def fullname_map(max_age_days=3):
    """{股票代號: 公司全名}。快取 3 天;抓不到 (或筆數不像全市場) 就用舊快取,都沒有回空 dict (退回只靠簡稱)。"""
    cached, age_d = {}, None
    try:
        obj = json.loads(FULLNAME_CACHE.read_text(encoding='utf-8'))
        cached = obj.get('names') or {}
        age_d = (datetime.now() - datetime.strptime(obj['fetched_at'], '%Y-%m-%d %H:%M:%S')).days
    except Exception:
        pass
    if cached and age_d is not None and age_d < max_age_days:
        return cached
    fetched = {}
    for url, k_code, k_name in FULLNAME_SRC:
        try:
            r = requests.get(url, headers={'User-Agent': UA}, timeout=30)
            for x in r.json():
                if x.get(k_code) and x.get(k_name):
                    fetched[str(x[k_code]).strip()] = str(x[k_name]).strip()
        except Exception as e:
            print(f'  [WARN] 公司全名抓取失敗 {url.rsplit("/", 1)[-1]}: {e}')
    if len(fetched) >= 1500:
        FULLNAME_CACHE.parent.mkdir(exist_ok=True)
        FULLNAME_CACHE.write_text(json.dumps({'fetched_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                              'names': fetched}, ensure_ascii=False), encoding='utf-8')
        return fetched
    return cached


def _units_yi(tr):
    """承銷張數 → 億 (1000 張 = 1 億)。"""
    return tr['units'] / 1000 if tr.get('units') else None


def _amount_ok(db_amount, tr):
    """DB 發行額 (億) 跟公會承銷張數對得上嗎。任一邊沒值 → 不判 (回 None)。"""
    a, b = _units_yi(tr), None
    try:
        b = float(db_amount) if db_amount not in (None, '') else None
    except (TypeError, ValueError):
        b = None
    if not a or not b:
        return None
    return abs(a - b) <= max(0.01, b * 0.01)


def load_conflicts():
    """讀 logs/method_conflicts.json。檔案不存在 → {};讀到壞檔/寫一半 → None (呼叫端放棄這次寫入,
    不能拿空 dict 把另外兩個 writer 的段落蓋掉 — 三支會寫:這支、backfill_primary_dates、auto_analyze_cb)。"""
    if not CONFLICTS_PATH.exists():
        return {}
    try:
        return json.loads(CONFLICTS_PATH.read_text(encoding='utf-8'))
    except Exception:
        return None


def write_conflicts(data):
    """先寫暫存檔再 os.replace (原子換檔),讀的人不會讀到半份。"""
    import os
    CONFLICTS_PATH.parent.mkdir(exist_ok=True)
    tmp = CONFLICTS_PATH.with_name(f'{CONFLICTS_PATH.name}.{os.getpid()}.tmp')
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding='utf-8')
    try:
        os.replace(tmp, CONFLICTS_PATH)
    except OSError as e:                       # Windows:目標剛好被別人開著 → 這輪放棄,下輪再寫
        print(f'  [WARN] method_conflicts.json 換檔失敗: {e}')
        try:
            tmp.unlink()
        except OSError:
            pass


def save_conflicts(source, items, **extra):
    """把這支看到的方式衝突寫進 logs/method_conflicts.json 的 source 那段 (整段覆寫 = 只留現況)。
    extra:例如 twsa 的 confirmed (被公會詢圈公告確認的 cb) — 稽核拿來壓掉「券商檔過期」的假衝突。"""
    data = load_conflicts()
    if data is None:
        print('  [WARN] method_conflicts.json 讀不出來 (可能別支正在寫) → 這輪不寫')
        return
    data[source] = {'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'items': items, **extra}
    write_conflicts(data)


def _strip_seq(name):
    """去 CB 序號尾碼: 群聯二→群聯 / 寬魚國際一→寬魚國際 / 騰輝-KY 保留。"""
    n = re.sub(f'[{ZH_NUM}]+(?=(?:-?KY)?$)', '', name or '').strip()
    return n


def _norm_co(name):
    """正規化公司全名以利比對: 去『股份有限公司』『（KY）』等。"""
    n = re.sub(r'股份有限公司|股份份有限公司|有限公司', '', name or '')
    n = re.sub(r'（[^）]*）|\([^)]*\)', '', n)
    n = n.replace('－', '-').strip()
    return n


def _period_plausible(board_iso, bid_start_iso, min_days=0, max_days=400):
    """圈購日相對董事會日是否合理:必須在董事會【之後】且不超過 max_days。

    公會清單含同一家公司【歷次】詢圈公告,只比公司名會配到舊案 (台燿六配到台燿五 2025 年那筆)。
    董事會 → 送件 → 生效 → 專戶 → 圈購,實務約 1.5~3 個月,放寬到 400 天涵蓋拖很久的案。
    董事會日缺就不判 (回 True),避免把還沒抓到 board 的新案全擋掉。
    """
    if not board_iso or not bid_start_iso:
        return True
    try:
        b = datetime.strptime(str(board_iso)[:10], '%Y-%m-%d').date()
        s = datetime.strptime(str(bid_start_iso)[:10], '%Y-%m-%d').date()
    except ValueError:
        return True
    return min_days <= (s - b).days <= max_days


def match_and_update(rows, dry_run=False, trusted=True):
    """trusted=False (有年份抓取失敗/抓到 0 筆):只做詢圈案補期間,不判方式、不寫衝突檔
    (清單不完整時寫空的 twsa 段 → 衝突被藏、confirmed 清空讓券商舊值誤報)。"""
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    # 在途案 (未掛牌、董事會近 180 天),方式不限 — 詢圈的要補 bid 期間;
    # 其他 (競拍/空白) 拿來抓「方式記錯」(2026-11-03 映興二:以前這裡 `method LIKE 詢圈`,記成競拍的永遠對不到)
    cbs = conn.execute('''
        SELECT i.cb_code, i.company, i.stock_code, i.fm_bid_start_date, i.fm_bid_end_date,
               i.fm_board_decision_date AS board, i.method, i.amount, i.eff_date, s.company AS stk_name
        FROM issued i LEFT JOIN stocks s ON s.stock_code = i.stock_code
        WHERE (i.is_legacy IS NULL OR i.is_legacy != 1) AND (i.is_withdrawn IS NULL OR i.is_withdrawn != 1)
          AND (i.listing_date IS NULL OR i.listing_date = '' OR i.listing_date = '未定'
               OR substr(i.listing_date,1,10) >= date('now','-30 days'))
          AND i.fm_board_decision_date >= date('now','-180 days')
    ''').fetchall()
    xun = [c for c in cbs if '詢圈' in (c['method'] or '')]
    others = [c for c in cbs if '詢圈' not in (c['method'] or '')]

    norm_rows = [(_norm_co(r['company']), r) for r in rows]
    fullnames = fullname_map()
    # 全名 → 股票代號:公會那則是【別家】公司就不准配 (華新 1605 vs 華新科技、聯華 vs 聯華電子、大華 vs 大華建設、
    #   世紀 vs 世紀鋼鐵 — 簡稱前綴撞名,2026-11-03 審查用現行清單實測 10 組)
    owner = {}
    for _code, _nm in fullnames.items():
        owner.setdefault(_norm_co(_nm), _code)
    updated, matched, skipped, revised = 0, [], [], []
    method_fixed, conflicts = [], []
    xun_hit = []             # (cb_code, 案號, 張數是否吻合)
    claimed = set()          # 已被詢圈檔認走的公告案號 (不分公司)
    tw_auc = {r[0] for r in conn.execute('SELECT cb_code FROM upcoming_auctions UNION SELECT cb_code FROM auctions')}
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def _seq_num(s):
        digits = re.sub(r'\D', '', str(s or ''))
        return int(digits) if digits else -1

    def _hits_for(cb):
        # 比對用名: 優先個股簡稱,退 CB 名去序號
        cand = _strip_seq(cb['stk_name'] or '') or _strip_seq(cb['company'] or '')
        cand_norm = _norm_co(cand)
        full = _norm_co(fullnames.get(str(cb['stock_code'] or ''), ''))
        if not cand_norm and not full:
            return []
        hits = []
        for ncompany, tr in norm_rows:
            if '轉換公司債' not in tr['kind'] and '交換公司債' not in tr['kind']:
                continue
            # 嚴謹: 全名【以個股簡稱開頭】(定穎投資控股 startswith 定穎),或 KY 外商「…商XXX」緊接。
            #   不用子字串 in — 否則「群聯電子」會誤含「聯電」害聯電一對到群聯 (2026-07-10 dry-run 抓到)。
            #   或跟 OpenAPI 公司全名完全一樣 (大華建設 = 華建,簡稱不是全名開頭)。
            if (full and ncompany == full) or (
                    len(cand_norm) >= 2 and (ncompany.startswith(cand_norm) or ('商' + cand_norm) in ncompany)):
                # 🔴 名字對上還不夠 — 同一家公司會發很多次 CB,公會清單裡有它【歷次】的詢圈公告。
                #    必須檢查【時序合理性】:圈購一定在董事會【之後】(中間還要送件→生效→專戶,
                #    實務約 1.5~3 個月),且不會拖過一年。
                #    2026-08-17 血案:62746 台燿六 (董事會 2026-07-29) 配到台燿【五】2025-10-30
                #    的圈購 → 圈購日比董事會早 9 個月,畫面顯示「詢圈 2025/10/30~11/03」。
                if not _period_plausible(cb['board'], tr['bid_start']):
                    continue
                if owner.get(ncompany, str(cb['stock_code'] or '')) != str(cb['stock_code'] or ''):
                    continue                        # 全名查得到、而且是別家公司
                hits.append(tr)
        return hits

    for cb in xun:
        hits = _hits_for(cb)
        if not hits:
            continue
        claimed.update(t['seq'] for t in hits)
        # 2026-10-26:同一案可能有多則公告 (颱風/改期 → 重新公告圈購期間,廣閎科 115036 → 115039)。
        #   以前取第一則就 break,第二則永遠進不來。改取案號最大 (= 最新) 的那則。
        # 覆寫只看【轉換公司債】的公告 (同公司的交換公司債公告不能拿來蓋 CB 的圈購期間)
        cb_hits = [t for t in hits if '轉換公司債' in t['kind']] or hits
        hit = max(cb_hits, key=lambda t: (_seq_num(t['seq']), t['bid_start']))
        matched.append((cb['cb_code'], cb['company'], hit['company'], hit['bid_start'], hit['bid_end'], hit['price']))
        xun_hit.append((cb['cb_code'], hit['seq'], bool(_amount_ok(cb['amount'], hit))))
        if _amount_ok(cb['amount'], hit) is False and '轉換公司債' in hit['kind']:
            # 公會承銷張數 = 這檔詢圈的量;對不上多半是 DB 存了合計/舊值 (由田、星宇那類),只報不改
            conflicts.append({'cb': cb['cb_code'], 'company': cb['company'], 'kind': 'amount', 'field': 'amount',
                              'db': cb['amount'], 'other': _units_yi(hit), 'seq': hit['seq'],
                              'msg': f'發行額 DB {cb["amount"]} 億 vs 公會 {hit["seq"]} 承銷 {hit["units"]:,} 張 '
                                     f'(= {_units_yi(hit):g} 億)'})
        cur_bs = (cb['fm_bid_start_date'] or '').strip()[:10]
        cur_be = (cb['fm_bid_end_date'] or '').strip()[:10]
        sets, vals, notes = [], [], []
        if not cur_bs and not cur_be:
            sets += ['fm_bid_start_date=?', 'fm_bid_end_date=?']; vals += [hit['bid_start'], hit['bid_end']]
        elif cur_bs != hit['bid_start'] or cur_be != hit['bid_end']:
            # DB 已有值但跟公會最新公告不同:差距 ≤60 天 = 同一案改期/重新公告 → 覆寫留痕;
            # 差太多多半是配到別案或手填的特殊值 → 只報不改。
            try:
                gap = abs((datetime.strptime(hit['bid_start'], '%Y-%m-%d') -
                           datetime.strptime(cur_bs or cur_be, '%Y-%m-%d')).days)
            except ValueError:
                gap = 999
            if gap <= 60:
                sets += ['fm_bid_start_date=?', 'fm_bid_end_date=?']; vals += [hit['bid_start'], hit['bid_end']]
                notes.append(f'圈購 {cur_bs[5:]}~{cur_be[5:]}→{hit["bid_start"][5:]}~{hit["bid_end"][5:]} (公會重新公告)')
                revised.append((cb['cb_code'], cb['company'], notes[-1]))
            else:
                revised.append((cb['cb_code'], cb['company'],
                                f'圈購 DB {cur_bs}~{cur_be} vs 公會 {hit["bid_start"]}~{hit["bid_end"]} (差 {gap} 天,未覆寫)'))
        if sets:
            # bid_period 是畫面顯示的期間字串,跟著換
            sets.append('bid_period=?'); vals.append(f'{hit["bid_start"]}~{hit["bid_end"]}')
            if notes:
                sets += ['last_status_update=?', 'last_status_note=?']; vals += [now, ' / '.join(notes)]
            sets.append('updated_at=?'); vals.append(now)
            if not dry_run:
                vals.append(cb['cb_code'])
                conn.execute(f'UPDATE issued SET {", ".join(sets)} WHERE cb_code=?', vals)
            updated += 1
        else:
            skipped.append(cb['cb_code'])

    # ── DB 方式不是詢圈,公會卻有它的詢圈公告 ──
    # 🔴 2026-11-03 映興二 35972:6~7 月稿本、統一證、富邦 08 月前都寫競拍 → DB 競拍;9 月稿本 (09-21) 改
    #    「採詢價圈購…依票面 102% 發行」、富邦 09-04 檔改詢圈,統一證到 11-02 還寫競拍。公會 115075 圈購
    #    11-03~11-04 上了,這裡卻因 `method LIKE 詢圈` 把它濾掉 → 圈購期間空、訂價窗口照「開標−7」算。
    #    同公司一詢一拍是常態 (華邦電四五、神達一二),所以只有【沒被詢圈兄弟認走】的公告才算數。
    #    改寫是最危險的一步 (真競拍被改成詢圈 → 訂價窗口/T 全錯,而且之後券商檔的「競拍」會被 confirmed 壓掉),
    #    所以每一條都要【正面確認】,缺資料一律只報不改 (2026-11-03 審查:發行額/生效日空值原本會放行)。
    def _siblings(cb, hit):
        """同股票、事件日在 ±400 天內、而且在這則公告圈購開始時【還沒掛牌】的其他 CB
        (整張 issued,不限在途 — 兄弟檔已過在途視窗也要算)。圈購前就掛牌的舊案不可能是這則公告的主角
        (華建五 vs 2025-06 已掛牌的大華建設三/四)。"""
        ref = str(cb['board'] or cb['eff_date'] or '')[:10]
        if not ref:
            return 1
        return conn.execute('''SELECT COUNT(*) FROM issued WHERE stock_code=? AND cb_code!=?
            AND COALESCE(is_withdrawn,0)=0
            AND (listing_date IS NULL OR listing_date='' OR listing_date='未定' OR substr(listing_date,1,10) >= ?)
            AND (abs(julianday(substr(fm_board_decision_date,1,10)) - julianday(?)) <= 400
              OR abs(julianday(substr(eff_date,1,10)) - julianday(?)) <= 400
              OR abs(julianday(substr(listing_date,1,10)) - julianday(?)) <= 400)''',
            (cb['stock_code'], cb['cb_code'], hit['bid_start'], ref, ref, ref)).fetchone()[0]

    for cb in (others if trusted else []):
        hits = [t for t in _hits_for(cb) if '轉換公司債' in t['kind'] and t['seq'] not in claimed]
        if not hits:
            continue
        hit = max(hits, key=lambda t: (_seq_num(t['seq']), t['bid_start']))
        db_m = (cb['method'] or '').strip() or '(空)'
        eff = (cb['eff_date'] or '').strip()[:10]
        why = []
        if cb['cb_code'] in MANUAL_PROTECTED:
            why.append('手填保護')
        if cb['cb_code'] in tw_auc:
            why.append('TWSE 有競拍公告/開標紀錄 (多半是同公司另一檔詢圈還沒入庫)')
        if owner.get(_norm_co(hit['company'])) != str(cb['stock_code'] or ''):
            why.append('公會公司名對不到 OpenAPI 全名,無法確認是同一家')
        if _siblings(cb, hit):
            why.append('同公司還有別檔未掛牌/同期 CB,無法確定公告是哪一檔')
        if not _amount_ok(cb['amount'], hit):
            why.append(f'張數 {hit["units"] or "?"} 張 ≠ / 無法對 DB 發行額 {cb["amount"]} 億')
        if not eff:
            why.append('DB 沒生效日,無法確認圈購在生效後')
        elif hit['bid_start'] < eff:
            why.append(f'圈購 {hit["bid_start"]} 早於生效 {eff}')
        tag = f'公會詢圈公告 {hit["seq"]} 圈購 {hit["bid_start"]}~{hit["bid_end"]}'
        if why:
            conflicts.append({'cb': cb['cb_code'], 'company': cb['company'], 'kind': 'method', 'field': 'method',
                              'db': cb['method'], 'other': '詢圈', 'seq': hit['seq'],
                              'msg': f'DB 方式 {db_m},但有{tag} — 未改寫:{";".join(why)}'})
            continue
        cur_bs = (cb['fm_bid_start_date'] or '').strip()[:10]
        cur_be = (cb['fm_bid_end_date'] or '').strip()[:10]
        old_bid = f',原投標欄 {cur_bs}~{cur_be}' if (cur_bs or cur_be) else ''
        msg = f'方式 {db_m}→詢圈 ({tag})'
        trail = (f' [{msg} · 承銷 {hit["units"] or "?"} 張{old_bid} · fetch_twsa_bookbuilding'
                 f' @ {datetime.now():%Y-%m-%d %H:%M}]')
        if not dry_run:
            # 投標欄一併換:競拍案那兩欄存的是開標日 (塞法),方式一換就失去意義,舊值記進 note
            conn.execute('''UPDATE issued SET method='詢圈', fm_bid_start_date=?, fm_bid_end_date=?, bid_period=?,
                            note=COALESCE(note,'')||?, last_status_update=?, last_status_note=?, updated_at=?
                            WHERE cb_code=?''',
                         (hit['bid_start'], hit['bid_end'], f'{hit["bid_start"]}~{hit["bid_end"]}',
                          trail, now, msg, now, cb['cb_code']))
        method_fixed.append((cb['cb_code'], cb['company'], msg))
        updated += 1

    # confirmed (稽核拿來壓掉券商檔/稿本的「競拍」舊值) 只給證據夠硬的:公告只對到這一檔、張數吻合、TWSE 沒競拍紀錄。
    #   同公司兩檔都標詢圈 (其實一檔是競拍,由田二/帆宣六那種) 會同時對到同一則 → 兩檔都不算確認。
    _per_seq = {}
    for _cb, _seq, _ok in xun_hit:
        _per_seq[_seq] = _per_seq.get(_seq, 0) + 1
    confirmed = {_cb for _cb, _seq, _ok in xun_hit if _ok and _per_seq[_seq] == 1 and _cb not in tw_auc}
    confirmed |= {c for c, _n, _m in method_fixed}
    if not dry_run:
        conn.commit()
        if trusted:
            save_conflicts('twsa', conflicts, confirmed=sorted(confirmed), amount_confirmed=sorted(confirmed))
        else:
            print('  [WARN] 公會清單不完整 (有年份抓取失敗/0 筆) → 不判方式、不更新 method_conflicts.json')
    conn.close()
    if revised:
        print(f'\n⚠ 公會公告與 DB 圈購期間不同 {len(revised)} 筆:')
        for cbc, nm, msg in revised:
            print(f'   {cbc} {nm}: {msg}')
    if method_fixed:
        print(f'\n🔴 方式改寫 {len(method_fixed)} 筆 (公會有詢圈公告、TWSE 無競拍紀錄、張數吻合){" [dry-run 未寫]" if dry_run else ""}:')
        for cbc, nm, msg in method_fixed:
            print(f'   {cbc} {nm}: {msg}')
    if conflicts:
        print(f'\n⚠ 與公會公告不一致 {len(conflicts)} 筆 (未覆寫,夜間稽核會發 TG):')
        for c in conflicts:
            print(f'   {c["cb"]} {c["company"]}: {c["msg"]}')
    if method_fixed and not dry_run:
        try:
            from notify_tg import send_tg
            send_tg('<b>🔧 CB 方式改寫 (公會詢圈公告)</b>\n' +
                    '\n'.join(f'{cbc} {nm}: {msg}' for cbc, nm, msg in method_fixed))
        except Exception as e:
            print(f'[WARN] TG 通知失敗: {e}')
    return matched, updated, skipped


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='抓+對,不寫 DB')
    ap.add_argument('--year', type=int, help='指定西元年 (預設今年+去年)')
    ap.add_argument('--raw', action='store_true', help='只印抓到的詢圈表')
    args = ap.parse_args()

    years = [args.year] if args.year else [datetime.now().year, datetime.now().year - 1]
    all_rows = []
    failed = False
    for y in years:
        try:
            rows = fetch_bookbuilding(y)
            print(f'[{y}] 抓到詢圈公告 {len(rows)} 筆')
            all_rows.extend(rows)
            # 今年 0 筆 = 被擋/頁面改版 (台灣官網被擋常回空而非報錯);1 月初今年還沒公告不算
            if not rows and (y != datetime.now().year or datetime.now().month > 1):
                failed = True
        except Exception as e:
            print(f'[{y}] 抓取失敗: {e}')
            failed = True

    if args.raw:
        for r in all_rows:
            print(f"  {r['seq']} {r['company'][:16]:<16} {r['kind'][:8]:<8} 圈購 {r['bid_start']}~{r['bid_end']} 價 {r['price']}")
        return

    matched, updated, skipped = match_and_update(all_rows, dry_run=args.dry_run,
                                                 trusted=bool(all_rows) and not failed)
    print(f"\n=== 對到在途詢圈 CB: {len(matched)} 檔 ({'DRY-RUN 不寫' if args.dry_run else f'更新 {updated} 檔, {len(skipped)} 檔已有值跳過'}) ===")
    for cb, co, twco, bs, be, price in matched:
        print(f"  {cb} {co[:8]:<9} ← [{twco[:18]}] 圈購 {bs}~{be}  價 {price}")


if __name__ == '__main__':
    main()
