#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""30-min 本機自癒 pulse:對「所有進行中 CB」做完整補強,不漏。

設計原則 (用戶 2026-06-25 要求「全自動不要一直漏」):
  1. 進行中定義 *inclusive*:upcoming ∪ (未掛牌 ∧ (board≤150d ∨ eff≤90d))
     → 連「只有董事會決議、還沒生效」的早期案 (49675/聯亞/晟田) 都涵蓋,
       不再像舊版只認 eff_date 有值的。
  2. 每輪完整鏈:TWSE → xlsx 日期回填 → MOPS milestone(補 board/account 缺口)
     → conv_price(MOPS/FinMind/B05) → 個股走勢 → 有變才 build+publish。
  3. **永遠 exit 0**:單一 step 抓失敗 (TWSE/MOPS/FinMind 擋) 不讓整個 task 標紅,
     失敗寫進 log。schtask 不會再 LastTaskResult=1。
  4. 寫心跳 logs/pulse_last.txt (時間 + 結果),方便監督確認排程真的有在跑。

schtask: "CB Pulse 30min" → cron_pulse.bat → 本檔。
"""
import json
import sqlite3
import subprocess
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).parent
DB = HERE / 'cb_data.db'
PY = sys.executable
LOG_DIR = HERE / 'logs'
HEARTBEAT = LOG_DIR / 'pulse_last.txt'


def log(msg):
    print(f'[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}', flush=True)


def run(args, label, timeout=900):
    """跑 step,任何失敗都吞掉只記 log (never abort whole pulse)。

    🔴 2026-08-19:原本不接子行程輸出,讓它直接寫父行程的 stdout。父子各自帶緩衝、
       又同時寫同一個 pulse.log → 子行程的輸出常整段消失。
       後果不只是「看不到」:`audit_cb_coverage.last_scan_info()` 靠 log 裡的
       「掃描完成」字樣判斷掃描有沒有跑完,輸出不見就一律判成【逾時】→ 每天誤報,
       而誤報久了真的逾時也沒人信 (8/18 掃描其實 9 分鐘就跑完,稽核卻連報兩天)。
       → 改成 capture_output 後由 log() 逐行寫出,順序正確且不會被覆蓋。
    """
    log(f'--- {label} ---')
    try:
        r = subprocess.run([PY, *args], cwd=str(HERE), timeout=timeout,
                           capture_output=True, text=True,
                           encoding='utf-8', errors='replace')
        for line in (r.stdout or '').splitlines():
            if line.strip():
                log(f'  | {line.rstrip()}')
        for line in (r.stderr or '').splitlines()[-15:]:      # stderr 只留尾巴 (通常是 traceback)
            if line.strip():
                log(f'  ! {line.rstrip()}')
        if r.returncode != 0:
            log(f'  [{label}] returncode={r.returncode} (繼續)')
        return r.returncode == 0
    except subprocess.TimeoutExpired as e:
        # text=True 時 e.stdout 是 str,bytes 才要 decode — 寫死 .decode 會在這裡再炸一次
        # 讓整個 cron_pulse 中斷 (還好目前實測 e.stdout 都是 None 沒踩到)。
        _out = e.stdout or ''
        if isinstance(_out, bytes):
            _out = _out.decode('utf-8', 'replace')
        for line in _out.splitlines()[-20:]:
            if line.strip():
                log(f'  | {line.rstrip()}')                    # 逾時也把已產出的進度留下
        log(f'  [{label}] TIMEOUT {timeout}s (繼續)')
        return False
    except Exception as e:
        log(f'  [{label}] 例外: {e} (繼續)')
        return False


# ── 進行中 CB 定義 (inclusive) ────────────────────────────────
IN_PROGRESS_SQL = '''
  SELECT cb_code FROM upcoming_auctions WHERE is_cancelled=0
  UNION
  SELECT cb_code FROM issued
  WHERE (is_legacy IS NULL OR is_legacy != 1)
    AND (is_withdrawn IS NULL OR is_withdrawn != 1)
    AND (listing_date IS NULL OR listing_date='' OR listing_date='未定'
         OR substr(listing_date,1,10) >= date('now','-30 days'))
    AND (
      (fm_board_decision_date IS NOT NULL AND substr(fm_board_decision_date,1,10) >= date('now','-150 days'))
      OR (eff_date IS NOT NULL AND eff_date != '' AND substr(eff_date,1,10) >= date('now','-90 days'))
    )
'''


def get_inprogress():
    conn = sqlite3.connect(str(DB))
    cbs = sorted({r[0] for r in conn.execute(IN_PROGRESS_SQL) if r[0]})
    conn.close()
    return cbs


def get_milestone_gaps(inprogress):
    """進行中且 board 或 account 仍缺 → 需要 MOPS milestone 補 (65843 就是 board=NULL 漏掉)。"""
    if not inprogress:
        return []
    conn = sqlite3.connect(str(DB))
    ph = ','.join('?' for _ in inprogress)
    rows = conn.execute(f'''SELECT cb_code FROM issued WHERE cb_code IN ({ph})
        AND (fm_board_decision_date IS NULL OR fm_account_setup_date IS NULL)''', inprogress).fetchall()
    conn.close()
    return [r[0] for r in rows]


def get_pricing_due(inprogress):
    """已生效 (或詢圈/競拍已結束) 但 conv_price 仍空 = 真正該追訂價的小集合。
    早期案 (還沒生效) 不可能有 conv_price,不浪費時間去下載 B05 PDF。"""
    if not inprogress:
        return []
    conn = sqlite3.connect(str(DB))
    ph = ','.join('?' for _ in inprogress)
    rows = conn.execute(f'''SELECT cb_code FROM issued WHERE cb_code IN ({ph})
        AND (conv_price IS NULL OR conv_price=0)
        AND ((eff_date IS NOT NULL AND eff_date!='' AND substr(eff_date,1,10)<=date('now'))
             OR (fm_bid_end_date IS NOT NULL AND substr(fm_bid_end_date,1,10)<=date('now')))''', inprogress).fetchall()
    conn.close()
    return [r[0] for r in rows]


def daily_due(marker_name, hours=20):
    """回 True 若距上次跑 marker 超過 hours (用來把重活降成一天一次)。"""
    mk = LOG_DIR / marker_name
    try:
        if mk.exists():
            age_h = (datetime.now().timestamp() - mk.stat().st_mtime) / 3600
            if age_h < hours:
                return False
    except Exception:
        pass
    return True


def stamp_daily(marker_name):
    try:
        LOG_DIR.mkdir(exist_ok=True)
        (LOG_DIR / marker_name).write_text(f'{datetime.now():%Y-%m-%d %H:%M:%S}\n', encoding='utf-8')
    except Exception:
        pass


def snapshot(inprogress):
    """進行中 CB 關鍵欄位 hash → 決定要不要 publish。"""
    if not inprogress:
        return frozenset()
    conn = sqlite3.connect(str(DB))
    ph = ','.join('?' for _ in inprogress)
    rows = conn.execute(f'''SELECT cb_code, conv_price, eff_date, fm_account_setup_date,
        fm_board_decision_date, fm_bid_start_date, fm_bid_end_date, listing_date, last_status_note
        FROM issued WHERE cb_code IN ({ph}) ORDER BY cb_code''', inprogress).fetchall()
    twse = conn.execute('SELECT cb_code,auction_date,bid_start,bid_end,listing_date FROM upcoming_auctions ORDER BY cb_code').fetchall()
    # stocks 筆數 — fill_missing_stocks 補了新案母股就要上站 (決策助手/個股庫才看得到),不然要等下一個其他變更
    try:
        n_stk = conn.execute('SELECT COUNT(*) FROM stocks').fetchone()[0]
    except Exception:
        n_stk = ''
    conn.close()
    # 疊圖重算時間戳 — 不算進來的話,疊圖每天更新但進行中欄位沒變,這輪就不會 publish,線上一直是舊劇本
    try:
        ovl = json.loads((HERE.parent / 'overlay' / '_index.json').read_text(encoding='utf-8')).get('built', '')
    except Exception:
        ovl = ''
    return frozenset(tuple(str(c or '') for c in r) for r in rows) | \
           frozenset(('TWSE',) + tuple(str(c or '') for c in r) for r in twse) | \
           frozenset([('OVL', ovl), ('STK', str(n_stk))])


def write_heartbeat(result):
    try:
        LOG_DIR.mkdir(exist_ok=True)
        HEARTBEAT.write_text(f'{datetime.now():%Y-%m-%d %H:%M:%S} {result}\n', encoding='utf-8')
    except Exception:
        pass


def main():
    # deep = 夜間深掃 (強制全 milestone + 掃新發行人)。
    # 觸發: 明確 --deep,或「晚上 ≥21:00 且今天還沒 deep 過」→ 自動由 30 分 pulse 接手,
    #       不需獨立夜間排程 (獨立排程 CJK 路徑在 Windows scheduler 常出問題)。
    hr = datetime.now().hour
    # deep 時窗: 晚上 21:00 ~ 隔天 08:00 (含凌晨,防晚上沒開機、凌晨才開也能補跑當天 deep)
    in_deep_window = (hr >= 21 or hr <= 7)
    # 🔴 2026-09-28:光有時窗不夠 — deep 停了【6 天】沒人發現 (9/22 之後才靠 audit 的 36h 門檻報出來)。
    #    根因:deep = 時窗 AND 距上次>20h,兩個條件要【同時】成立。用戶 9/23~9/27 只有白天開機
    #    (09-25~09-28 中秋+教師節連假),電腦從沒在 21:00~07:00 開著 → 時窗永遠沒打開 →
    #    全市場掃描整整 6 天沒跑,新案偵測等於停擺,而 pulse 每輪都「正常結束」不會報錯。
    #    補救:超過 STALE_DEEP_H 沒 deep 就【不管幾點都補跑】。白天 MOPS 反而比夜間快
    #    (白天 ~10 分 vs 夜間 ~37 分),代價可接受;漏新案的代價高得多。
    STALE_DEEP_H = 30
    stale_deep = daily_due('pulse_deep.marker', hours=STALE_DEEP_H)
    deep = ('--deep' in sys.argv) or stale_deep or (
        in_deep_window and daily_due('pulse_deep.marker', hours=20))
    if stale_deep and not in_deep_window:
        log(f'⚠ 距上次 deep 已超過 {STALE_DEEP_H}h (多半是夜間沒開機) → 不等時窗,現在補跑')
    log(f'=== cron_pulse start {"(DEEP)" if deep else ""} ===')
    if deep:
        stamp_daily('pulse_deep.marker')
    try:
        inprogress = get_inprogress()
    except Exception as e:
        log(f'get_inprogress 失敗: {e}')
        write_heartbeat('FAIL get_inprogress')
        return 0
    before = snapshot(inprogress)
    log(f'進行中 CB: {len(inprogress)} 檔')
    n_inprogress_at_start = len(inprogress)   # 給 2.7 判斷「這輪有沒有新案進來」

    # 深掃 (夜間): 全市場掃 (不用 --only-unknown!)。
    #   --only-unknown 只掃「issued 沒有的股票」→ 會漏掉「已知發行人的新一檔」
    #   (如盟立 2464 已有 24642,新發 24643 盟立三 就被跳過)。全掃才不漏。
    if deep:
        # timeout 3600:1879 家 × 2 個月本來就要 ~900-1500s,加上「空結果重試」(防 MOPS 靜默擋)
        # 會更久。2026-08-04/05 連兩天卡在 2000s 逾時 → 整批新案沒掃到 (威剛九漏了 9 天)。
        # timeout 7200:實測掃描耗時波動很大 — 白天 ~10 分鐘,晚上 21:00 檔期 MOPS 降速
        # 常到 ~37 分鐘 (9/04=2234s、9/05=2205s),2026-09-06 甚至超過 3600s 被砍 →
        # 當天新案偵測直接空窗。3600 太貼著日常值,留一倍餘裕。
        # 🔴 2026-10-06:2402s(09-22) → 4794s(10-01) → 6092s(10-04) → 撞 7200s(10-05)。【不是】MOPS 自己變慢
        #   (單次請求一直 ~0.6s),是重試條件 raw==0 把「查無需求資料」(真的沒公告) 當成被擋 + 被擋的
        #   「Overrun」頁是 HTTP 200 沒被認出 + 4 workers ≈2.3 次/秒 超過 MOPS 容忍度 (~1.3 次/秒)
        #   → 越重試越被擋。改 mops_client (判讀 + 全域節流 + 全體冷卻 + 舊月份快取) 後:
        #   當月只需查 1 個月 ≈ 26 分;月初要多查上個月 (快取冷) ≈ 52 分。逾時維持 7200 不動。
        run(['scan_cb_disclosures.py', '--days', '14'], 'scan 全市場新案', timeout=7200)
        # 補漏:只掃「已有 CB 的 589 家」但放慢+重試,專門撈全市場快掃漏掉的
        # (兩者互補:全市場掃廣度、這支掃可靠度)
        # 600 家 × 約 3.2s = ~32 分鐘 (實測 2026-08-19:1800s 只跑到 150/600 就被砍,
        # 這層是全市場掃描之外的第二道防線,跑不完等於沒有) → 放寬到 45 分鐘留餘裕。
        # 2026-10-06:同上根因 (每家 3.6s → 14s,連三天逾時)。改走 mops_client 後舊月份共用 scan 的快取,
        #   607 家 × 當月 ≈ 9 分;months_back 改精確後月初會多包上上個月 (第一次查 ≈ 17 分)。
        run(['rescan_missed_cb.py', '--days', '45', '--fix'], 'rescan 補漏 (已知發行人)', timeout=2700)
        # 董事會公告【內文】屬性 (方式/承銷商/發行額/年期) 安全網 (2026-09-17 用戶:「未來請看資料也要進來看一下」)
        #   scan 抓到新案時已即時進內文填;這裡補「當時 detail 抓失敗」或舊案缺的,
        #   只對屬性有缺的在途案發請求 (通常 <10 檔)。順手把混進來的海外CB (ECB) 標 withdrawn。
        #   --verify:連「欄位都填滿」的在途案也拉公告比對,印出不一致但不覆寫。
        #   2026-09-21 由田:方式填滿了但值是錯的 (一則公告兩檔,舊版取先出現的方式套兩檔),
        #   早退式的檢查永遠看不到 → 這種「填滿但填錯」只能靠逐案比對抓。
        run(['board_attrs.py', '--verify'], '董事會內文屬性 + 比對', timeout=1800)
        # 同公司舊案疊圖 (2026-10-04 用戶選方案二+四接進 modal):每天用當日收盤重算在途案的劇本線、
        #   CB 價位卡、舊案 CB 結果。要抓全市場 ~340 檔還原股價 (有快取,約 1~3 分),所以只在 deep 跑。
        #   產出時間戳寫進 overlay/_index.json,snapshot() 有算進去 → 重算後這輪一定會 build+publish。
        run(['overlay_build.py', '--inflight'], '同公司舊案疊圖', timeout=1500)
        # 已掛牌案的疊圖歷史回補 (2026-10-04 用戶「每一檔都補上比較」):掛牌一年內的,已有且簽章沒變的直接跳過
        #   (幾秒),只有新掛牌滿 40 天的、或 DB 日期/轉換價/舊案清單變了的才重算。更早的年份用手動
        #   `overlay_build.py --history --since YYYY-MM-DD` 分批補,補過的檔每日模式不會刪。
        #   2026-10-05 全部年份已補完 (937 檔已掛牌);每天掃全部,只有簽章變了 (DB 事件日/轉換價/舊案清單改了) 才重算。
        run(['overlay_build.py', '--history', '--since', '2005-01-01'], '疊圖歷史回補 (有變才重算)', timeout=900)
        # 第三道:獨立稽核前兩道健不健康,異常主動發 TG。
        # 🔴 光有重試不夠 — 威剛九漏 9 天沒人發現,是因為【沒有任何機制會告訴你出事了】。
        #    掃描逾時/停擺/長期無新案 都會在這裡被抓出來並通知。
        run(['audit_cb_coverage.py'], '覆蓋率稽核 + 告警', timeout=180)

    # 1) TWSE 即將開標 (bid/listing → issued)
    run(['fetch_twse_upcoming.py'], 'fetch_twse_upcoming', timeout=180)
    # 1.5) TWSE 競拍開標結果回填 (剛開標案 → auctions)
    run(['fetch_twse_auction_results.py'], 'fetch_twse_auction_results', timeout=300)

    # 2) 統一證/富邦 xlsx 日期回填 (eff/bid/conv/listing,涵蓋所有 pipeline)
    if (HERE.parent.parent / 'stock-dash').exists() or (HERE / 'backfill_primary_dates.py').exists():
        run(['backfill_primary_dates.py'], 'backfill_primary_dates', timeout=120)

    # 2.5) 證券商公會 edoc 詢圈公告 → 圈購期間 (fm_bid_start/end)。權威來源,比 xlsx 即時。
    #      用戶 2026-07-13 加。只填空欄 (COALESCE,保護手填如聯電)。
    run(['fetch_twsa_bookbuilding.py'], 'twsa 詢圈圈購期間', timeout=120)

    # 進行中可能因前幾步新增 → 重撈
    inprogress = get_inprogress()

    # 2.7) 補 stocks 表缺漏母股 (FinMind TaiwanStockInfo + issued.capital)。
    #   🔴 2026-10-06 發現 pulse 從來沒跑這支 (只有雲端 GHA 的 mops_daily 有) → 7 檔新 CB 的母股
    #   (1623/6534/6597/6944/7631/7740/7743) 本機 stocks 表一直沒有,決策助手靠「從 issued 補」的 fallback;
    #   雲端補了又被本機下次發佈蓋掉。
    #   不每輪跑:FinMind 沒紀錄的 (太老下市,約 20 檔) 每次都會重查,一天一次就夠;
    #   但這輪有新案進來 (進行中數量變多) 就立刻補,新案母股當天上站。snapshot 有算 stocks 筆數,補到就會 publish。
    if daily_due('pulse_fill_stocks.marker', hours=20) or len(inprogress) > n_inprogress_at_start:
        run(['fill_missing_stocks.py'], 'fill_missing_stocks (母股補 stocks 表)', timeout=180)
        stamp_daily('pulse_fill_stocks.marker')

    # 3) MOPS milestone — 【每次 pulse 都批次抓】(用戶 2026-07-08:公開資訊當天公告就要當天進站,不要再自己看 MOPS)。
    #    fetch_mops_milestones 無參數 = 內部 4-worker 平行抓「進行中 + 近一年」案 (get_targets),
    #    一次呼叫 (取代舊「一檔一檔 --cb」41 次 python 啟動 + daily throttle)。
    #    → 下午 16:09 公告的專戶/董事會,下一輪 pulse (≤30 分) 就抓進 DB → snapshot 變 → 自動 build+publish 上站。
    gaps = get_milestone_gaps(inprogress)
    log(f'milestone 缺口: {len(gaps)} 檔 (--cbs 批次平行抓,只抓缺 board/account 的進行中案)')
    if gaps:
        run(['fetch_mops_milestones.py', '--cbs', ','.join(gaps)], 'milestone 批次', timeout=600)
    stamp_daily('pulse_milestone.marker')  # 保留 marker 供監督確認新鮮度

    # 4) conv_price — 只查「已生效但未訂價」的小集合 (pricing-due),每輪都查
    #    這是時間敏感的 (訂價當天就要抓到),B05 PDF 只對這幾檔下載
    due = get_pricing_due(inprogress)
    log(f'conv_price pricing-due: {len(due)} 檔 - {due}')
    if due:
        run(['fetch_mops_conv_price.py', *due], f'conv_price ({len(due)} 檔)', timeout=600)

    # 5) 個股走勢圖 (快,每輪)
    run(['fetch_premium_rally.py', '--in-progress'], 'rally', timeout=300)

    # 6) 監督者自檢:階段一致性稽核 (每輪都跑;ERROR 級異常寫進心跳 + log)
    nerr = 0
    try:
        import audit_cb_stages, sqlite3 as _sq
        _c = _sq.connect(str(DB)); _c.row_factory = _sq.Row
        _, _errs, _infos = audit_cb_stages.audit(_c)
        _c.close()
        nerr = len(_errs)
        log(f'audit: ERROR {nerr} / INFO {len(_infos)}')
        for cb, nm, msg in _errs:
            log(f'  [audit ERROR] {cb} {nm}: {msg}')
    except Exception as e:
        log(f'  [audit] 跑失敗: {e}')

    # 7) 有變才 build + publish
    after = snapshot(get_inprogress())
    if before == after:
        log('no change → skip publish')
        write_heartbeat(f'OK no-change · audit ERR={nerr}')
        log('=== cron_pulse done (no publish) ===')
        return 0

    log('CHANGE detected → build + publish')
    run(['build_html.py'], 'build_html', timeout=300)
    ok = run(['publish_cb.py'], 'publish_cb', timeout=300)
    base = 'OK published' if ok else 'OK built (publish issue)'
    write_heartbeat(f'{base} · audit ERR={nerr}')
    log(f'=== cron_pulse done (PUBLISHED, audit ERR={nerr}) ===')
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception:
        log('FATAL:\n' + traceback.format_exc())
        write_heartbeat('FATAL')
        sys.exit(0)  # 仍回 0,schtask 不標紅
