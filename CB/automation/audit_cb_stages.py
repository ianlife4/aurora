#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CB 發行階段一致性稽核 (監督者自檢)。

用戶 2026-06-29 要求「釐清過程步驟、當監督者檢查、不要再出 bug」。
對所有「進行中」CB 依標準 8 階段檢查欄位邏輯有無矛盾,分 ERROR/INFO 兩級。

== CB 標準發行階段 (canonical;dashboard 與 timeline 都依此) ==
  1. 董事會決議    fm_board_decision_date     董事會通過發行
  2. 申報/送件     receipt_date               向證期局/櫃買送件
  3. 申報生效      eff_date                   主管機關核准生效
  4. 確定專戶      fm_account_setup_date      代收價款/存儲專戶確定
  5. 詢圈/競拍     fm_bid_start~fm_bid_end     bookbuilding / 競拍投標期
  6. 開標/得標     auctions.auction_date      [競拍] 開標定價日
  7. 訂定轉換價    fm_conv_price_set_date / conv_price
  8. 掛牌上市/櫃   listing_date

== dashboard 新案戰報分類 (renderNewCaseAlerts) ==
  - 🎯 可投標/即將 : 競拍 · 有 board · 未開標(auction_date 空或未來) · 未掛牌 · 非停滯
  - 🏁 已開標·待上市: auction_date ≤ 今天 且 尚未掛牌
  - (剃除) 停滯     : 生效>35天無任何後續 / 板>120天未生效無後續
  - (剃除) 已上市   : listing_date < 今天 或 fm_cb_first_close 有值 → 進「已發行」清單
  - (剃除) 詢圈/撤回/老案

執行: py -3.12 audit_cb_stages.py            (印報告)
      py -3.12 audit_cb_stages.py --errors-only
回傳 exit code = ERROR 級異常數 (0=乾淨),方便 pulse 判斷。
"""
import argparse
import sqlite3
import sys
import io
from datetime import datetime, date
from pathlib import Path

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

DB = Path(__file__).parent / 'cb_data.db'


def _d(s):
    s = (s or '')[:10]
    try:
        return datetime.strptime(s, '%Y-%m-%d').date()
    except ValueError:
        return None


def audit(conn, today=None):
    today = today or date.today()
    days = lambda s: (today - _d(s)).days if _d(s) else None
    auc = {r[0]: r[1] for r in conn.execute(
        "SELECT cb_code, auction_date FROM auctions WHERE auction_date IS NOT NULL")}
    rows = conn.execute('''
        SELECT cb_code, company, method, receipt_date, eff_date, fm_board_decision_date,
               fm_account_setup_date, fm_bid_start_date, fm_bid_end_date,
               fm_conv_price_set_date, conv_price, listing_date, fm_cb_first_close
        FROM issued
        WHERE (is_legacy IS NULL OR is_legacy!=1) AND (is_withdrawn IS NULL OR is_withdrawn!=1)
          AND (listing_date IS NULL OR listing_date='' OR listing_date='未定'
               OR substr(listing_date,1,10) >= date('now','-30 days'))
          AND ((fm_board_decision_date IS NOT NULL AND substr(fm_board_decision_date,1,10) >= date('now','-150 days'))
               OR (eff_date IS NOT NULL AND eff_date!='' AND substr(eff_date,1,10) >= date('now','-90 days')))
    ''').fetchall()

    errors, infos = [], []
    for r in rows:
        cb, nm = r['cb_code'], r['company']
        aucd = _d(auc.get(cb))
        listd = _d(r['listing_date'])
        m = r['method'] or ''
        # ── ERROR 級 (真邏輯/抓取問題,該修) ──
        # 開標已過 >2 天卻無轉換價 → conv_price 抓取漏了
        if aucd and (today - aucd).days >= 2 and not r['conv_price']:
            errors.append((cb, nm, f'開標 {aucd} 已過卻無轉換價 → 跑 fetch_mops_conv_price'))
        # 掛牌日已過 >3 天卻沒上市行情 → fetch_premium_rally 漏抓
        # 撥券日撞颱風/假日 → 首日順延到下一交易日 (62855 啟碁五 10-23 颱風,10-27 被誤報 9 次)
        try:
            import tw_calendar as _twc
            _eff_ld = _twc.effective_trading_day(listd) if listd else None
        except Exception:
            _eff_ld = listd
        if _eff_ld and (today - _eff_ld).days > 3 and not r['fm_cb_first_close']:
            errors.append((cb, nm, f'掛牌 {listd} 已過卻無上市行情 → 跑 fetch_premium_rally'))
        # 有專戶卻無董事會 → backfill 缺口,milestone 沒補
        if r['fm_account_setup_date'] and not r['fm_board_decision_date']:
            errors.append((cb, nm, '有專戶卻無董事會日 → 跑 fetch_mops_milestones --cb'))
        # 競拍 bid_end 過 >7 天卻無開標紀錄 → 開標結果沒抓進 auctions
        be = days(r['fm_bid_end_date'])
        if '競拍' in m and be is not None and be > 7 and not aucd:
            errors.append((cb, nm, f'競拍截止 {be} 天卻無開標紀錄 → 跑 fetch_twse_auction_results'))
        # ── INFO 級 (來源缺口,非 bug,有就好) ──
        # 有轉換價卻無生效日:conv_price 存在代表生效一定發生過,只是來源沒給日期 (開標後統一證清空)
        if r['conv_price'] and not (r['eff_date'] or '').strip():
            infos.append((cb, nm, '有轉換價但缺生效日 (來源缺口,timeline 少 1 節點)'))
        # 掛牌日已過但 1-3 天內,FinMind 行情通常 T+1~2 才有,屬正常延遲
        if listd and 0 <= (today - listd).days <= 3 and not r['fm_cb_first_close']:
            infos.append((cb, nm, f'掛牌 {listd} 行情待 FinMind (T+1~2 正常)'))
    return rows, errors, infos


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--errors-only', action='store_true')
    args = ap.parse_args()
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    rows, errors, infos = audit(conn)
    conn.close()
    print(f'[audit] 進行中 {len(rows)} 檔 · ERROR {len(errors)} · INFO {len(infos)}')
    for cb, nm, msg in errors:
        print(f'  ❌ {cb} {nm}: {msg}')
    if not args.errors_only:
        for cb, nm, msg in infos:
            print(f'  ℹ️ {cb} {nm}: {msg}')
    return len(errors)


if __name__ == '__main__':
    sys.exit(main())
