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
  B. MOPS 查詢未確認:上次掃描重試+補查後仍被擋/逾時的公司數 (>0 就報;2026-10-06 取代「空結果比例」,
     舊版分不出「查無資料」和「被擋」,所以那條從沒實作)
  C. 新案靜默期:多久沒偵測到任何新案 (>10 天 = 可疑,台股平均每週都有新 CB)
  C6. 方式/發行額衝突:公會詢圈公告、統一證、富邦初級檔跟 DB 打架的在途案 (logs/method_conflicts.json)
  D. 進行中案缺欄:在途案缺 eff/bid/conv 的比例異常升高

## 用法
  py audit_cb_coverage.py            # 稽核 + 有問題才發 TG
  py audit_cb_coverage.py --always   # 一律發 TG (測試用)
  py audit_cb_coverage.py --quiet    # 只印不發
"""
import argparse
import json
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
    """從 pulse.log 找最後一次全市場掃描:回 (時間, 是否完成, 掃到幾筆公告, 未確認家數, 未確認明細)。
    未確認 = 掃描印的「MOPS 查詢未確認 N 家」(2026-10-06 起才有;舊格式回 None)。"""
    if not PULSE_LOG.exists():
        return None, False, None, None, ''
    txt = PULSE_LOG.read_text(encoding='utf-8', errors='replace')
    lines = txt.split('\n')
    last_start, done, hits, unconf, unconf_txt = None, False, None, None, ''
    for i, l in enumerate(lines):
        if '--- scan 全市場新案 ---' in l:
            m = re.search(r'\[(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)\]', l)
            last_start = m.group(1) if m else None
            done, hits, unconf, unconf_txt = False, None, None, ''
            # cron_pulse 會把子行程輸出逐行轉寫 (前綴 '| ');2026-10-06 拿掉舊的 400 行上限 —
            #   掃描現在會印被擋事件/WARN,輸出超過 400 行就會把「跑完」誤判成「逾時」。遇到下一段 '--- ' 或 TIMEOUT 自然會停。
            for nxt in lines[i + 1:]:
                um = re.search(r'MOPS 查詢未確認 (\d+) 家(.*)', nxt)
                if um:
                    unconf, unconf_txt = int(um.group(1)), um.group(2).strip()
                if '掃描完成' in nxt or '新案 ' in nxt and '/ 補董事會' in nxt:
                    done = True
                    hm = re.search(r'共\s*(\d+)\s*筆', nxt)
                    hits = int(hm.group(1)) if hm else None
                    break
                if 'TIMEOUT' in nxt and 'scan 全市場' in nxt:
                    break
                if '--- ' in nxt and 'scan 全市場' not in nxt:
                    break
    return last_start, done, hits, unconf, unconf_txt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--always', action='store_true', help='一律發 TG')
    ap.add_argument('--quiet', action='store_true', help='只印不發 TG')
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    issues, info = [], []

    # A. 掃描新鮮度
    ts, done, hits, unconf, unconf_txt = last_scan_info()
    if not ts:
        issues.append('❌ pulse.log 找不到任何全市場掃描紀錄')
    else:
        age_h = (datetime.now() - datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')).total_seconds() / 3600
        info.append(f'最後掃描 {ts} ({age_h:.0f} 小時前) · {"完成" if done else "🔴未完成/逾時"}'
                    + (f' · 抓到 {hits} 筆公告' if hits is not None else '')
                    + (f' · MOPS 未確認 {unconf} 家' if unconf is not None else ''))
        if age_h > MAX_SCAN_AGE_H:
            issues.append(f'❌ 全市場掃描已 {age_h:.0f} 小時沒跑 (門檻 {MAX_SCAN_AGE_H}h) — 排程可能掛了')
        if not done:
            issues.append('❌ 最後一次全市場掃描【沒跑完】(逾時) — 新案偵測等於停擺')
        # B. MOPS 查詢未確認 (2026-10-06 取代從沒實作的「空結果比例」)
        #    掃描現在分得出「查無資料」(MOPS 明確回答) 和「被擋/逾時」(沒回答);重試 + 補查後仍沒答覆的公司,
        #    這段期間的新公告就是沒看到 — 即使只有 1 家也可能正好是新案 (威剛九就是 1 家),所以 >0 就報。
        if done and unconf:
            issues.append(f'⚠ 全市場掃描有 {unconf} 家 MOPS 查詢未確認 (重試+補查後仍被擋/逾時) — '
                          f'這些公司近期公告可能漏抓\n     ↳ {unconf_txt[:300]}\n'
                          f'     ↳ 已有 CB 的公司 rescan 會再查一次;其餘可手動 rescan_missed_cb.py --stock XXXX')

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

    # C3. 交易日曆涵蓋範圍 (2026-09-28 加)
    #     假日表過期 → tw_calendar / 前端 pwBizAdd 退化成「只跳週末」,T-5/T-3/T-1 會標到休市日,
    #     而且【不會報錯】,就跟 2026-09-28 教師節那次一樣要用戶自己看出來。
    try:
        import tw_calendar
        rng = tw_calendar.coverage()
        if not rng:
            issues.append('❌ holidays 表是空的 — 訂價窗口 T-5/T-3/T-1 會把國定假日當交易日\n'
                          '     ↳ cd automation && python tw_calendar.py --import-xlsx')
        else:
            info.append(f'交易日曆涵蓋 {rng[0]} ~ {rng[1]}')
            if not tw_calendar.coverage_ok():
                issues.append(
                    f'❌ 交易日曆只到 {rng[1]},不足 120 天 — 再過去 T-5/T-3/T-1 會退化成只跳週末\n'
                    f'     ↳ 等統一證 xlsx 的 holiday 分頁更新,或手動補 holidays 表')
            # C3b (2026-10-26 颱風稽核):只看涵蓋範圍抓不到【內容】錯 — 10-23 薔琵颱風休市 3 天沒進表、
            #   2027 統一證預估把中秋寫成 10-08。拿 TWSE 實際成交日 (FMTQIK) 與年度開休市表對帳,只報不改
            #   (cron_pulse 每輪 tw_calendar --sync 會自己補;這裡是它失效時的最後一道)。
            try:
                _rc = tw_calendar.reconcile_fmtqik(conn, months_back=2, apply=False)
                if _rc['added']:
                    issues.append('❌ 近 2 個月有休市日不在假日表 (平日無成交): ' + ', '.join(_rc['added'])
                                  + '\n     ↳ 訂價窗口 T-N 會踩到沒收盤的日子;cd automation && python tw_calendar.py --sync')
                if _rc['conflicts']:
                    issues.append('❌ 假日表標休市但 TWSE 當天有成交 (誤列,T-N 會少算一天): ' + ', '.join(_rc['conflicts']))
                if _rc['months'] == 0:
                    info.append('FMTQIK 對帳:TWSE 抓不到,略過')
                else:
                    info.append(f'FMTQIK 對帳 {_rc["months"]} 個月一致')
            except Exception as _e:
                info.append(f'FMTQIK 對帳失敗: {_e}')
            try:
                _ny = datetime.now().year + 1
                _sc = tw_calendar.sync_twse_schedule(conn, _ny, apply=False)
                if _sc['available'] and (_sc['added'] or _sc['removed']):
                    issues.append(f'❌ {_ny} 假日表與 TWSE 開休市表不符:缺 {_sc["added"] or "無"} / 多列 {_sc["removed"] or "無"}'
                                  f'\n     ↳ python tw_calendar.py --schedule {_ny}')
                elif _sc['available']:
                    info.append(f'{_ny} 假日表與 TWSE 開休市表一致')
            except Exception as _e:
                info.append(f'開休市表比對失敗: {_e}')
    except Exception as e:
        issues.append(f'❌ 交易日曆檢查失敗: {e}')

    # C4. 代號重用沒被處理 (2026-10-05 光譜三 53813 被併進 2008 合正三那列一個半月沒人發現)
    #     已掛牌列上出現晚一年以上的董事會/生效/投標日 = 同代號的新債。入口 (MOPS 掃描/補漏/券商檔/TWSE)
    #     都會先呼叫 db.retire_reused_code 封存舊債;這裡是最後一道,漏網就告警,不自動動資料。
    try:
        import db as _dbm
        _reuse = []
        for r in conn.execute('''SELECT cb_code, company, listing_date, fm_cb_first_date, fm_board_decision_date,
                eff_date, fm_bid_start_date FROM issued
                WHERE listing_date GLOB '[0-9][0-9][0-9][0-9]-*' OR fm_cb_first_date GLOB '[0-9][0-9][0-9][0-9]-*' ''').fetchall():
            ds = sorted(d for d in (_dbm._iso10(r['fm_board_decision_date']), _dbm._iso10(r['eff_date']),
                                    _dbm._iso10(r['fm_bid_start_date'])) if d)
            if ds and _dbm.reused_code_old_listing(conn, r['cb_code'], ds[-1]):
                _reuse.append(f"{r['cb_code']} {r['company']} (舊掛牌 {str(r['listing_date'] or r['fm_cb_first_date'])[:10]} / 新事件 {ds[-1]})")
        if _reuse:
            issues.append('❌ 新案被併進同代號的舊債那列 (在途清單/疊圖會漏掉它),多半是「第N次」推代號撞號:\n     '
                          + '\n     '.join(_reuse)
                          + '\n     ↳ 人工拆開:舊債欄位還原、新案移到 db.next_cb_code(股票) (見 memory project_cb_supervisor 光譜 53814)')
        # C5. 在途案代號撞到歷史上用過的代號 (cb168 全歷史,含 FinMind 沒有的 2005 前下櫃舊債)
        #     在途案還沒掛牌,不可能出現在 cb168 → 出現 = 推代號推錯 (光譜三 53813 = 2008 合正三)
        _hist = _dbm._hist_codes()
        if _hist:
            _col = []
            for r in conn.execute('''SELECT cb_code, company FROM issued WHERE COALESCE(is_withdrawn,0)=0
                    AND (listing_date IS NULL OR listing_date='' OR listing_date='未定' OR substr(listing_date,1,10) >= date('now'))
                    AND (fm_board_decision_date >= date('now','-400 days') OR substr(eff_date,1,10) >= date('now','-400 days'))''').fetchall():
                if r['cb_code'] in _hist:
                    _col.append(f"{r['cb_code']} {r['company']}")
            if _col:
                issues.append('❌ 在途案代號撞到歷史舊債代號 (櫃買代號=累計第幾檔,「第N次」推會撞):\n     '
                              + '\n     '.join(_col) + '\n     ↳ 改用 db.next_cb_code(股票),等 TWSE 即將開標的正式代號自動對正')
    except Exception as e:
        issues.append(f'❌ 代號撞號檢查失敗: {e}')

    # C6. 方式/發行額衝突 (2026-11-03 映興二 35972 / 華建五 25305 被記成競拍:富邦初級檔 09-04 就改詢圈、
    #     9 月稿本寫詢價圈購,DB 照統一證掛競拍兩個月 → 訂價窗口照「開標−7」算、公會圈購對不上)
    #     來源:fetch_twsa_bookbuilding (source=twsa,能確認的它已自動改寫,這裡只剩它不敢改的) +
    #     backfill_primary_dates (source=broker,統一證/富邦 vs DB) + auto_analyze_cb (source=prospectus,稿本
    #     「採…方式辦理公開承銷」vs DB)。只報【現在仍衝突】的在途案。
    #     券商檔過期 (DB 已被公會公告/TWSE 開標確認) 不報,不然統一證不改版就每晚吵同一檔。
    try:
        _cf_path = HERE / 'logs' / 'method_conflicts.json'
        _cf = json.loads(_cf_path.read_text(encoding='utf-8')) if _cf_path.exists() else {}
        _tw_ok = set((_cf.get('twsa') or {}).get('confirmed') or [])
        _tw_amt = set((_cf.get('twsa') or {}).get('amount_confirmed') or [])
        # 人工確認帳 (method_verified.json,2026-11-06 帆宣六:MOPS 董事會公告寫詢圈、統一證寫競拍 → 不然每晚報到統一證改版)
        try:
            _ver = json.loads((HERE / 'method_verified.json').read_text(encoding='utf-8'))
        except FileNotFoundError:
            _ver = {}
        _auc = {r[0] for r in conn.execute('SELECT cb_code FROM upcoming_auctions UNION SELECT cb_code FROM auctions')}
        _today = datetime.now().strftime('%Y-%m-%d')
        _d = lambda n: (datetime.now() - timedelta(days=n)).strftime('%Y-%m-%d')
        _lines, _seen = [], set()
        for _src, _sec in _cf.items():
            for it in (_sec or {}).get('items') or []:
                r = conn.execute('''SELECT method, amount, underwriter, term, listing_date, fm_board_decision_date b,
                                    eff_date e, fm_account_setup_date a FROM issued WHERE cb_code=?''', (it['cb'],)).fetchone()
                if not r:
                    continue
                ld = str(r['listing_date'] or '')[:10]
                if ld and ld != '未定' and ld < _today:
                    continue
                if not (str(r['b'] or '')[:10] >= _d(180) or str(r['e'] or '')[:10] >= _d(120)
                        or str(r['a'] or '')[:10] >= _d(60)):
                    continue                                    # 老案 (盛群一 2023 生效、掛牌欄空) 不算在途
                _f = it.get('field') or ('method' if it.get('kind') == 'method' else 'amount')
                if _f not in r.keys():
                    continue
                cur = r[_f]
                if str(cur if cur is not None else '').strip() != str(it.get('db') if it.get('db') is not None else '').strip():
                    continue                                    # DB 已改過 → 衝突已解
                if _src != 'twsa' and it.get('kind') == 'method':       # 券商檔/稿本比權威公告舊
                    if '詢圈' in str(cur) and it['cb'] in _tw_ok:
                        continue                                # 公會公告確認是詢圈,券商檔過期
                    if '競拍' in str(cur) and it['cb'] in _auc:
                        continue                                # TWSE 有競拍公告/開標,券商檔過期
                if _src == 'broker' and _f == 'amount' and it['cb'] in _tw_amt:
                    continue                                    # 公會承銷張數 = DB 發行額 (神達一 100 億,統一證還寫 80)
                _v = (_ver.get(it['cb']) or {}) if isinstance(_ver.get(it['cb']), dict) else {}
                if _src != 'twsa' and _f in _v and str(_v[_f]).strip() == str(cur if cur is not None else '').strip():
                    continue                                    # 人工確認過,DB 也還是那個值
                key = (it['cb'], it.get('msg'))
                if key in _seen:
                    continue
                _seen.add(key)
                _lines.append(f"{it['cb']} {it.get('company') or ''}: {it.get('msg')}")
        # C6b. 反方向:DB 寫詢圈,TWSE 卻有競拍公告/開標結果 (2026-11-03 抓到中華化二 17272、國統五 89365 都是這樣)。
        #     也是 fetch_twsa 自動改寫萬一改錯的最後一道 — 改成詢圈後 TWSE 公告開標,這裡會響。近 60 天掛牌的也看。
        for r in conn.execute('''SELECT cb_code, company FROM issued WHERE method LIKE '%詢圈%'
                AND COALESCE(is_withdrawn,0)=0 AND COALESCE(is_legacy,0)!=1
                AND (listing_date IS NULL OR listing_date='' OR listing_date='未定' OR substr(listing_date,1,10) >= ?)
                AND (cb_code IN (SELECT cb_code FROM auctions) OR cb_code IN (SELECT cb_code FROM upcoming_auctions))''',
                (_d(60),)).fetchall():
            _lines.append(f"{r['cb_code']} {r['company']}: DB 方式 詢圈,但 TWSE 有競拍公告/開標結果 → 多半該是競拍")
        if _lines:
            issues.append('⚠ 方式/發行額跟公會公告或券商檔不一致 (程式沒把握,未覆寫):\n     '
                          + '\n     '.join(_lines)
                          + '\n     ↳ 權威序:TWSE 競拍公告/公會詢圈公告 > 最新稿本 > 兩家券商一致 > 單家;確認後手動改 issued 並在 note 留痕')
    except Exception as e:
        issues.append(f'❌ 方式衝突檢查失敗: {e}')

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
