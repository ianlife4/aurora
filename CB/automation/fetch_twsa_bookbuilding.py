#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fetch_twsa_bookbuilding.py — 從證券商公會 edoc 抓「詢圈公告」圈購期間 (詢圈 CB 的權威來源)。

來源: https://web.twsa.org.tw/edoc2/default.aspx  (ASP.NET; 查詢類型=詢圈公告 rblReportType=BookBuilding)
表格欄: 案號 / 發行公司 / 主辦承銷商 / 發行性質 / 承銷股數 / 詢圈銷售股數 / 圈購期間 / 價格 / 公告檔
用途: 「圈購期間」= 詢圈 CB 的 fm_bid_start_date~fm_bid_end_date,比統一證 xlsx 更即時權威。
      對到 issued (發行公司名→在途詢圈 CB) 只填【空欄】的 bid 期間 (COALESCE,不覆寫、保護手填如聯電)。

用法:
  py fetch_twsa_bookbuilding.py --dry-run      # 抓+對,不寫 DB (看對到誰)
  py fetch_twsa_bookbuilding.py                # 抓+對+更新 DB
  py fetch_twsa_bookbuilding.py --year 2025    # 指定年 (預設今年+去年都抓)
  py fetch_twsa_bookbuilding.py --raw          # 只印抓到的詢圈表 (debug)
"""
import argparse
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
        rows.append({
            'seq': cells[0], 'company': cells[1], 'broker': cells[2], 'kind': cells[3],
            'bid_start': f'{m.group(1)}-{m.group(2)}-{m.group(3)}',
            'bid_end':   f'{m.group(4)}-{m.group(5)}-{m.group(6)}',
            'price': cells[7],
        })
    return rows


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


def match_and_update(rows, dry_run=False):
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    # 在途詢圈案 (未掛牌、method 詢圈、董事會近 180 天) — 要補 bid 期間的目標
    cbs = conn.execute('''
        SELECT i.cb_code, i.company, i.stock_code, i.fm_bid_start_date, i.fm_bid_end_date,
               i.fm_board_decision_date AS board, s.company AS stk_name
        FROM issued i LEFT JOIN stocks s ON s.stock_code = i.stock_code
        WHERE (i.is_legacy IS NULL OR i.is_legacy != 1) AND (i.is_withdrawn IS NULL OR i.is_withdrawn != 1)
          AND i.method LIKE '%詢圈%'
          AND (i.listing_date IS NULL OR i.listing_date = '' OR i.listing_date = '未定'
               OR substr(i.listing_date,1,10) >= date('now','-30 days'))
          AND i.fm_board_decision_date >= date('now','-180 days')
    ''').fetchall()

    norm_rows = [(_norm_co(r['company']), r) for r in rows]
    updated, matched, skipped = 0, [], []
    for cb in cbs:
        # 比對用名: 優先個股簡稱,退 CB 名去序號
        cand = _strip_seq(cb['stk_name'] or '') or _strip_seq(cb['company'] or '')
        cand_norm = _norm_co(cand)
        if not cand_norm:
            continue
        hit = None
        for ncompany, tr in norm_rows:
            if '轉換公司債' not in tr['kind'] and '交換公司債' not in tr['kind']:
                continue
            # 嚴謹: 全名【以個股簡稱開頭】(定穎投資控股 startswith 定穎),或 KY 外商「…商XXX」緊接。
            #   不用子字串 in — 否則「群聯電子」會誤含「聯電」害聯電一對到群聯 (2026-07-10 dry-run 抓到)。
            if len(cand_norm) >= 2 and (ncompany.startswith(cand_norm) or ('商' + cand_norm) in ncompany):
                # 🔴 名字對上還不夠 — 同一家公司會發很多次 CB,公會清單裡有它【歷次】的詢圈公告。
                #    必須檢查【時序合理性】:圈購一定在董事會【之後】(中間還要送件→生效→專戶,
                #    實務約 1.5~3 個月),且不會拖過一年。
                #    2026-08-17 血案:62746 台燿六 (董事會 2026-07-29) 配到台燿【五】2025-10-30
                #    的圈購 → 圈購日比董事會早 9 個月,畫面顯示「詢圈 2025/10/30~11/03」。
                if not _period_plausible(cb['board'], tr['bid_start']):
                    continue
                hit = tr
                break
        if not hit:
            continue
        matched.append((cb['cb_code'], cb['company'], hit['company'], hit['bid_start'], hit['bid_end'], hit['price']))
        # 只填空欄 (COALESCE) — 不覆寫既有 / 手填值
        sets, vals = [], []
        if not (cb['fm_bid_start_date'] or '').strip():
            sets.append('fm_bid_start_date=?'); vals.append(hit['bid_start'])
        if not (cb['fm_bid_end_date'] or '').strip():
            sets.append('fm_bid_end_date=?'); vals.append(hit['bid_end'])
        if sets and not dry_run:
            vals.append(cb['cb_code'])
            conn.execute(f'UPDATE issued SET {", ".join(sets)} WHERE cb_code=?', vals)
            updated += 1
        elif not sets:
            skipped.append(cb['cb_code'])
    if not dry_run:
        conn.commit()
    conn.close()
    return matched, updated, skipped


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='抓+對,不寫 DB')
    ap.add_argument('--year', type=int, help='指定西元年 (預設今年+去年)')
    ap.add_argument('--raw', action='store_true', help='只印抓到的詢圈表')
    args = ap.parse_args()

    years = [args.year] if args.year else [datetime.now().year, datetime.now().year - 1]
    all_rows = []
    for y in years:
        try:
            rows = fetch_bookbuilding(y)
            print(f'[{y}] 抓到詢圈公告 {len(rows)} 筆')
            all_rows.extend(rows)
        except Exception as e:
            print(f'[{y}] 抓取失敗: {e}')

    if args.raw:
        for r in all_rows:
            print(f"  {r['seq']} {r['company'][:16]:<16} {r['kind'][:8]:<8} 圈購 {r['bid_start']}~{r['bid_end']} 價 {r['price']}")
        return

    matched, updated, skipped = match_and_update(all_rows, dry_run=args.dry_run)
    print(f"\n=== 對到在途詢圈 CB: {len(matched)} 檔 ({'DRY-RUN 不寫' if args.dry_run else f'更新 {updated} 檔, {len(skipped)} 檔已有值跳過'}) ===")
    for cb, co, twco, bs, be, price in matched:
        print(f"  {cb} {co[:8]:<9} ← [{twco[:18]}] 圈購 {bs}~{be}  價 {price}")


if __name__ == '__main__':
    main()
