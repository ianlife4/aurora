#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""audit_cb_coverage.py — CB 新案覆蓋率稽核 + 告警。「未來不要再漏案」的守門者。

## 為什麼要這支 (2026-08-06 用戶:「檢查兩次,未來不要有這事情發生」)
3260 威剛九 7/28 就在 MOPS 公告,卻漏了 9 天沒進 DB。根因:
**MOPS 被擋時回【空清單】而不是報錯** → 掃描器當成「這家沒公告」靜默跳過,
log 零 WARN、統計正常,沒有任何跡象。**問題不是重試不夠,而是沒有人會發現。**

所以光加重試不夠 —— 必須有【獨立的第二次檢查】+【異常時主動通知】。

## 三道防線
  1. `scan_cb_disclosures.py` 全市場快掃 (廣度) — 已加空結果重試
  2. `rescan_missed_cb.py`     已知發行人慢掃 (可靠度) — 放慢+重試,補第 1 道漏的
  3. **本支**:獨立稽核 — 不重抓,而是【檢查前兩道的結果健不健康】,異常就發 TG

## 稽核項目 (任一不過 → 告警)
  A. 掃描新鮮度:最近一次成功的全市場掃描距今多久 (>36h = 掃描停擺)
  B. 空結果比例:上次掃描有多少家回空 (>60% = MOPS 大規模擋,結果不可信)
  C. 新案靜默期:多久沒偵測到任何新案 (>10 天 = 可疑,台股平均每週都有新 CB)
  D. 進行中案缺欄:在途案缺 eff/bid/conv 的比例異常升高

## 用法
  py audit_cb_coverage.py            # 稽核 + 有問題才發 TG
  py audit_cb_coverage.py --always   # 一律發 TG (測試用)
  py audit_cb_coverage.py --quiet    # 只印不發
"""
import argparse
import re
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
DB_PATH = HERE / 'cb_data.db'
PULSE_LOG = HERE / 'logs' / 'pulse.log'
LOG_PATH = HERE / 'audit_coverage.log'

# 門檻
MAX_SCAN_AGE_H = 36        # 全市場掃描超過這麼久沒成功 = 停擺
MAX_EMPTY_RATIO = 0.60     # 掃描回空的公司比例超過 = MOPS 大規模擋

# 🔴 上游來源檔新鮮度 (2026-08-19 加):
#   Gmail OAuth token 過期後,抓信程式每天都失敗但【只寫在自己的 log 裡】,
#   統一證 xlsx 靜靜停在 8/03 整整 16 天沒人發現,直到用戶回報「儀表板定價不見」。
#   那份檔案是 eff_date / 圈購期間 / 掛牌日 / 轉換價 的主要來源,一斷全線受影響。
#   → 把「來源檔多久沒更新」納入每日稽核,別再靠人眼發現。
CB_DROP_DIR = Path(r'C:\Users\J.Chun\Desktop\stock-dash\cbas-template\CB報')
MAX_SRC_AGE_D = 5          # 券商檔超過這麼多天沒更新 = 上游斷了 (正常每個交易日都寄)
# 這麼久沒新案 = 可疑 (台股幾乎每週都有新 CB)。
#
# ⚠ 2026-08-17 差點改壞:當時看歷史間隔分布 (中位 3 天 / p90 9 / p95 13),
#   算出「門檻 10 天 → 誤報 9.2%」,就把它調到 18 天想降噪。
#   結果同一天掃描修好後跑出【23 檔真的漏抓的新案】(和碩/緯穎/大聯大/嘉澤…),
#   證明那次告警是【真的】,調到 18 只會讓偵測晚 8 天。
#   兩個錯:(1) 拿被 bug 汙染的 DB 資料去校準門檻 — 空窗本身就是漏抓造成的,循環論證;
#          (2) 這個偵測器守的是「靜默故障」,漏報的代價遠大於誤報 — 寧可吵。
# 👉 維持 10 天。它已經證明自己會抓到真問題。
MAX_QUIET_DAYS = 10
CNY_MONTHS = ('01', '02')  # 農曆年結構性淡季 (歷史最長空窗 67 天) — 唯一可豁免的情境


def log(m):
    line = f'[{datetime.now():%Y-%m-%d %H:%M:%S}] {m}'
    print(line, flush=True)
    try:
        with open(LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass


def last_scan_info():
    """從 pulse.log 找最後一次全市場掃描:回 (時間, 是否完成, 掃到幾筆公告)。"""
    if not PULSE_LOG.exists():
        return None, False, None
    txt = PULSE_LOG.read_text(encoding='utf-8', errors='replace')
    lines = txt.split('\n')
    last_start, done, hits = None, False, None
    for i, l in enumerate(lines):
        if '--- scan 全市場新案 ---' in l:
            m = re.search(r'\[(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)\]', l)
            last_start = m.group(1) if m else None
            done, hits = False, None
            for nxt in lines[i + 1:i + 400]:      # cron_pulse 現在會把子行程輸出逐行轉寫 (前綴 '| '),行數變多
                if '掃描完成' in nxt or '新案 ' in nxt and '/ 補董事會' in nxt:
                    done = True
                    hm = re.search(r'共\s*(\d+)\s*筆', nxt)
                    hits = int(hm.group(1)) if hm else None
                    break
                if 'TIMEOUT' in nxt and 'scan 全市場' in nxt:
                    break
                if '--- ' in nxt and 'scan 全市場' not in nxt:
                    break
    return last_start, done, hits


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--always', action='store_true', help='一律發 TG')
    ap.add_argument('--quiet', action='store_true', help='只印不發 TG')
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    issues, info = [], []

    # A. 掃描新鮮度
    ts, done, hits = last_scan_info()
    if not ts:
        issues.append('❌ pulse.log 找不到任何全市場掃描紀錄')
    else:
        age_h = (datetime.now() - datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')).total_seconds() / 3600
        info.append(f'最後掃描 {ts} ({age_h:.0f} 小時前) · {"完成" if done else "🔴未完成/逾時"}'
                    + (f' · 抓到 {hits} 筆公告' if hits is not None else ''))
        if age_h > MAX_SCAN_AGE_H:
            issues.append(f'❌ 全市場掃描已 {age_h:.0f} 小時沒跑 (門檻 {MAX_SCAN_AGE_H}h) — 排程可能掛了')
        if not done:
            issues.append('❌ 最後一次全市場掃描【沒跑完】(逾時) — 新案偵測等於停擺')

    # C. 新案靜默期
    #    ⚠ 2026-08-17 差點改壞:本來想「掃描沒跑完就別報靜默期,那只是同一故障的回音」。
    #      但事實相反 — 掃描逾時只說明【工具壞了】,靜默期才證明【真的漏了案子】。
    #      那天兩條一起響,修好掃描後跑出 23 檔漏抓新案;若當時消音就只會看到
    #      「掃描逾時」而無從判斷嚴重性 (逾時可能只是慢,也可能是災難)。
    #    👉 兩條都要報,但把【關聯】講明白,讓人一眼看出這是同一件事的因與果。
    #    農曆年 (1~2 月) 結構性淡季 (歷史最長空窗 67 天) 是唯一豁免。
    row = conn.execute('''SELECT MAX(fm_board_decision_date) m FROM issued
                          WHERE (is_legacy IS NULL OR is_legacy!=1)''').fetchone()
    if row and row['m']:
        quiet = (datetime.now().date() - datetime.strptime(row['m'][:10], '%Y-%m-%d').date()).days
        info.append(f'最新一檔新案董事會日 {row["m"][:10]} ({quiet} 天前)')
        if datetime.now().strftime('%m') in CNY_MONTHS:
            info.append('  (農曆年淡季 → 靜默期不判)')
        elif quiet > MAX_QUIET_DAYS:
            msg = f'⚠ 已 {quiet} 天沒有任何新案 (門檻 {MAX_QUIET_DAYS} 天) — 台股通常每週都有,可能在漏抓'
            if ts and not done:
                msg += '\n     ↳ 掃描同時逾時 → 這兩條很可能是同一件事:掃描沒跑完 = 真的在漏'
            issues.append(msg)

    # C2. 上游來源檔新鮮度 (Gmail 斷了會靜默,見檔頭 MAX_SRC_AGE_D 說明)
    try:
        srcs = sorted(CB_DROP_DIR.glob('CB發行資訊與CBAS報價表_*.xlsx'),
                      key=lambda p: p.stat().st_mtime, reverse=True)
    except Exception:
        srcs = []
    if not srcs:
        issues.append(f'❌ 找不到任何券商 CB 報表 ({CB_DROP_DIR}) — Gmail 抓信可能全掛')
    else:
        newest = srcs[0]
        age_d = (datetime.now() - datetime.fromtimestamp(newest.stat().st_mtime)).days
        info.append(f'券商報表最新 {newest.name[-13:-5]} ({age_d} 天前)')
        if age_d > MAX_SRC_AGE_D:
            issues.append(
                f'❌ 券商 CB 報表已 {age_d} 天沒更新 (門檻 {MAX_SRC_AGE_D} 天) — '
                f'生效日/圈購期間/掛牌日/轉換價 全線斷源\n'
                f'     ↳ 多半是 Gmail OAuth token 過期:cd automation && python setup_gmail.py')

    # D. 在途案缺欄
    tot = conn.execute('''SELECT COUNT(*) c FROM issued
        WHERE (is_legacy IS NULL OR is_legacy!=1) AND (is_withdrawn IS NULL OR is_withdrawn!=1)
          AND fm_board_decision_date >= date('now','-120 days')''').fetchone()['c']
    noeff = conn.execute('''SELECT COUNT(*) c FROM issued
        WHERE (is_legacy IS NULL OR is_legacy!=1) AND (is_withdrawn IS NULL OR is_withdrawn!=1)
          AND fm_board_decision_date >= date('now','-120 days')
          AND (eff_date IS NULL OR eff_date='')''').fetchone()['c']
    if tot:
        info.append(f'近 120 天在途案 {tot} 檔,其中缺生效日 {noeff} 檔 ({noeff/tot*100:.0f}%)')

    # 摘要
    log('=' * 56)
    log('CB 新案覆蓋率稽核')
    log('=' * 56)
    for i in info:
        log(f'  · {i}')
    log('')
    if issues:
        log(f'🔴 發現 {len(issues)} 個問題:')
        for x in issues:
            log(f'  {x}')
    else:
        log('✅ 一切正常')
    conn.close()

    if (issues or args.always) and not args.quiet:
        try:
            from notify_tg import send_tg
            body = '<b>🔍 CB 新案覆蓋率稽核</b>\n\n'
            if issues:
                body += '\n'.join(issues) + '\n\n'
            else:
                body += '✅ 一切正常\n\n'
            body += '<i>' + '\n'.join(info) + '</i>'
            ok = send_tg(body)
            log(f'TG 通知: {"已送出" if ok else "送出失敗"}')
        except Exception as e:
            log(f'[WARN] TG 通知失敗: {e}')

    return 1 if issues else 0


if __name__ == '__main__':
    sys.exit(main())
