#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CB 每日 MOPS-only 更新流程 (daily orchestrator)

只跑跟 MOPS 重大訊息相關的步驟，**不抓 Gmail / 不解析元富 xlsx / 不動 auctions**。
適合每天 18:00 跑，補抓「董事會決議發行 CB」+「確定存儲專戶」這些隨時可能公告的資訊。

執行的步驟：
  1. discover_new_cbs.py --days 30   ← 從 MOPS 全市場關鍵字找新發 CB (INSERT issued)
  2. fetch_mops_milestones.py        ← 補抓既有 issued 的 board_decision/account_setup 日期
  3. build_html.py                   ← 重建 CB管理.html
  4. publish_cb.py                   ← 推到 ianlife4/aurora (GitHub Pages)

執行：
  py -3.12 mops_daily.py
  py -3.12 mops_daily.py --skip-publish    # 只本機更新，不推 GitHub
  py -3.12 mops_daily.py --no-tg           # 關 Telegram 通知

排程：Windows Task Scheduler 每天 18:00 跑 update_mops_daily.bat
"""
import argparse
import io
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

BASE_DIR = Path(__file__).parent
LOG_PATH = BASE_DIR / 'mops_daily.log'
PYTHON = sys.executable

# TG 通知（可選）
try:
    sys.path.insert(0, str(BASE_DIR))
    from notify_tg import send_tg
except ImportError:
    def send_tg(text, **kw): return False


def log(msg: str, also_print: bool = True):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f'[{ts}] {msg}'
    if also_print:
        print(line)
    try:
        with open(LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass


_STATE = {'steps': [], 'failures': [], 'fatal': None,
          'new_cbs': 0, 'board_added': 0, 'account_added': 0}


def run_step(name: str, cmd: list[str], required: bool = False) -> tuple[bool, str]:
    """跑一個子腳本，捕獲 stdout/stderr 用來解析 counters，回傳 (ok, stdout)。"""
    log(f'=== {name} ===')
    log(f'執行：{" ".join(cmd)}')
    t0 = time.time()
    env = os.environ.copy()
    env['PYTHONIOENCODING'] = 'utf-8'
    try:
        result = subprocess.run(
            cmd, cwd=str(BASE_DIR),
            capture_output=True, text=True,
            encoding='utf-8', errors='replace',
            env=env,
            timeout=1500  # 25 分鐘上限（MOPS 慢；166 筆預估 ~6 分鐘，但 MOPS 有時會卡）
        )
    except subprocess.TimeoutExpired:
        log(f'  [ERR] 逾時（>25 分鐘）')
        _STATE['steps'].append((name, False, 1500))
        _STATE['failures'].append(name)
        return False, ''
    except Exception as e:
        log(f'  [ERR] 執行錯誤：{e}')
        _STATE['steps'].append((name, False, 0))
        _STATE['failures'].append(name)
        return False, ''

    elapsed = time.time() - t0
    if result.stdout:
        for line in result.stdout.splitlines():
            print(f'  | {line}')
            try:
                with open(LOG_PATH, 'a', encoding='utf-8') as f:
                    f.write(f'  | {line}\n')
            except Exception:
                pass
    if result.stderr:
        # stderr 也寫進 log (之前只 print,publish_cb git push 失敗時看不到 git 錯誤訊息)
        for line in result.stderr.splitlines():
            print(f'  [WARN] {line}')
            try:
                with open(LOG_PATH, 'a', encoding='utf-8') as f:
                    f.write(f'  [stderr] {line}\n')
            except Exception:
                pass

    ok = result.returncode == 0
    status = '[OK]' if ok else '[ERR]'
    log(f'  {status} {name} 結束（{elapsed:.1f}s, exit={result.returncode}）')
    _STATE['steps'].append((name, ok, elapsed))
    if not ok:
        _STATE['failures'].append(name)
        if required:
            _STATE['fatal'] = name
            log(f'  -> 必要步驟失敗，中止')
    return ok, result.stdout or ''


def parse_counters(stdout: str, step: str):
    """從子腳本 stdout 抓統計數字（給 TG 通知用）。"""
    import re
    if step == 'discover':
        # discover_new_cbs.py 輸出形如 "INSERT 新案 N 筆" 或 "新增 N 筆"
        for pat in [r'INSERT.*?(\d+)\s*筆', r'新增\s*(\d+)\s*筆', r'新案\s*(\d+)']:
            m = re.search(pat, stdout)
            if m:
                _STATE['new_cbs'] = int(m.group(1))
                return
    elif step == 'milestones':
        # fetch_mops_milestones.py 輸出 "抓到董事會 : N" / "抓到專戶   : N"
        m = re.search(r'抓到董事會\s*[:：]\s*(\d+)', stdout)
        if m: _STATE['board_added'] = int(m.group(1))
        m = re.search(r'抓到專戶\s*[:：]\s*(\d+)', stdout)
        if m: _STATE['account_added'] = int(m.group(1))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=30, help='discover_new_cbs 搜尋天數（預設 30）')
    parser.add_argument('--skip-discover', action='store_true', help='跳過 discover_new_cbs')
    parser.add_argument('--skip-milestones', action='store_true', help='跳過 fetch_mops_milestones')
    parser.add_argument('--skip-html', action='store_true', help='跳過 HTML 重建')
    parser.add_argument('--skip-publish', action='store_true', help='跳過推 GitHub Pages')
    parser.add_argument('--skip-analyze', action='store_true', help='跳過 Claude API 自動分析')
    parser.add_argument('--skip-twse', action='store_true', help='跳過 TWSE 即將開標公告抓取')
    parser.add_argument('--skip-conv-price', action='store_true', help='跳過 MOPS conv_price 抓取')
    parser.add_argument('--skip-fill-stocks', action='store_true', help='跳過 fill_missing_stocks (從 FinMind 補 stocks)')
    parser.add_argument('--skip-rally', action='store_true', help='跳過 fetch_premium_rally --in-progress (個股走勢刷新)')
    parser.add_argument('--skip-scan', action='store_true', help='跳過 scan_cb_disclosures (全市場 CB 公開資訊掃描)')
    parser.add_argument('--analyze-limit', type=int, default=10, help='每日 auto_analyze 上限 (cost guard,預設 10 = 一天最多 ~$8 Opus / ~$2.5 Sonnet)')
    parser.add_argument('--no-tg', action='store_true', help='關閉 Telegram 通知')
    args = parser.parse_args()

    log('=' * 60)
    log('CB 每日 MOPS 更新開始')
    log('=' * 60)

    pipeline_start = time.time()

    # Step 0: 從 aurora repo 拉最新 DB 進本機 (跟 GHA cron 的 DB 同步)
    # 避免本機 DB 跟 GHA push 過的 DB 不一致
    if not args.skip_publish:
        try:
            aurora_db = Path(r'C:\Users\J.Chun\Desktop\stock-dash\ipo\CB\automation\cb_data.db')
            local_db = BASE_DIR / 'cb_data.db'
            if aurora_db.exists():
                # 先 git pull aurora 拿最新 DB
                pull = subprocess.run(['git', '-C', str(aurora_db.parent.parent.parent),
                                       'pull', '--rebase', '-X', 'ours', 'origin', 'main'],
                                      capture_output=True, text=True, encoding='utf-8', errors='replace',
                                      timeout=60)
                if pull.returncode == 0:
                    log('Step 0: git pull aurora ok')
                else:
                    log(f'Step 0: git pull aurora 失敗 (繼續): {pull.stderr[-200:]}')
                # 比對 aurora DB 跟本機 DB,若雲端較新則複製進來
                # (簡單方式:總是把 aurora DB 複製過來 — 因為 aurora 是 canonical)
                import shutil as _shutil
                a_mtime = aurora_db.stat().st_mtime
                l_mtime = local_db.stat().st_mtime if local_db.exists() else 0
                if a_mtime > l_mtime + 5:  # 雲端 DB 比本機新 (容忍 5 秒 mtime 誤差)
                    _shutil.copy2(str(aurora_db), str(local_db))
                    log(f'Step 0: copy aurora DB → local (aurora 較新 {a_mtime-l_mtime:.0f}s)')
        except Exception as e:
            log(f'Step 0: 同步失敗 (繼續): {e}')

    # Step 0.6: scan_cb_disclosures.py — 全市場掃 CB 公開資訊 (主要偵測來源,先於逐檔輪詢)
    # 一次抓全市場近 7 天 CB 公告 → 補新案/確定專戶,並標記 last_status_update (HTML 浮頂 + 🆕)。
    # 先跑這支才能把「新狀態」flag 起來;discover/milestones 留作補洞備援。
    if args.skip_scan:
        log('SKIP scan_cb_disclosures')
    else:
        cmd = [PYTHON, 'scan_cb_disclosures.py', '--days', '7']
        run_step('0.6 全市場 CB 公開資訊掃描 (新案/確定專戶 + 標記更新)', cmd, required=False)

    # Step 1: discover_new_cbs.py
    if args.skip_discover:
        log('SKIP discover_new_cbs')
    else:
        cmd = [PYTHON, 'discover_new_cbs.py', '--days', str(args.days)]
        ok, out = run_step('1. MOPS 全市場掃新發 CB', cmd, required=False)
        if ok:
            parse_counters(out, 'discover')

    # Step 2: fetch_mops_milestones.py
    if args.skip_milestones:
        log('SKIP fetch_mops_milestones')
    else:
        cmd = [PYTHON, 'fetch_mops_milestones.py']
        ok, out = run_step('2. MOPS 補抓 board_decision/account_setup 日', cmd, required=False)
        if ok:
            parse_counters(out, 'milestones')

    # Step 2.3: fetch_twse_upcoming.py (TWSE 即將開標公告 — 每天抓不只 weekly,
    # 因為「投標中/待開標/即將」分類靠 upcoming_auctions,1 週才更新會錯位)
    if args.skip_twse:
        log('SKIP fetch_twse_upcoming')
    else:
        cmd = [PYTHON, 'fetch_twse_upcoming.py']
        run_step('2.3 TWSE 即將開標公告抓取', cmd, required=False)

    # Step 2.35: fetch_twse_auction_results.py (TWSE 競拍結果回填)
    # 每日跑,把當天剛開標的 CB 結果 (min/avg/max 得標元價) 補進 auctions 表
    # 不跑就只能等用戶手動 query → 用戶會看不到剛開標 CB 的得標分析
    if args.skip_twse:
        log('SKIP fetch_twse_auction_results')
    else:
        cmd = [PYTHON, 'fetch_twse_auction_results.py']
        run_step('2.35 TWSE 競拍結果回填 (剛開標案 actual_price)', cmd, required=False)

    # Step 2.4: fetch_mops_conv_price.py (從 MOPS 重大訊息抓「訂定轉換價格」公告)
    # 用戶準備競拍時要看 conv_price,訂定公告通常在投標前 1 週 (5/8 訂定 → 5/13~15 投標)
    if args.skip_conv_price:
        log('SKIP fetch_mops_conv_price')
    else:
        cmd = [PYTHON, 'fetch_mops_conv_price.py']
        run_step('2.4 MOPS 訂定轉換價格抓取', cmd, required=False)

    # Step 2.45: fill_missing_stocks.py — 從 FinMind 補 stocks 表缺漏的個股
    # (新發 CB 母股不在用戶 Excel「個股」sheet 內時,決策助手會顯示「個股庫無此代號」)
    if args.skip_fill_stocks:
        log('SKIP fill_missing_stocks')
    else:
        cmd = [PYTHON, 'fill_missing_stocks.py']
        run_step('2.45 補 stocks 表缺漏個股 (FinMind)', cmd, required=False)

    # Step 2.5: auto_analyze_cb.py (用 Claude API 自動寫公開說明書分析 .md)
    if args.skip_analyze:
        log('SKIP auto_analyze_cb')
    else:
        cmd = [PYTHON, 'auto_analyze_cb.py', '--limit', str(args.analyze_limit)]
        ok, _ = run_step('2.5 自動產 CB 公開說明書分析 (Claude API)', cmd, required=False)
        if not ok:
            log('  -> auto_analyze_cb 失敗,繼續產 HTML')

    # Step 2.6: fetch_premium_rally.py --in-progress (個股走勢 + rally% 每日刷新)
    # 全量 rally 只在 weekly self_update 跑;但待上市/剛上市 CB 的個股走勢天天在動,
    # 競拍→掛牌這段最關鍵 (如 54642 霖宏二),不每日刷新 dashboard 圖表會落後到下個週一
    if args.skip_rally:
        log('SKIP fetch_premium_rally --in-progress')
    else:
        cmd = [PYTHON, 'fetch_premium_rally.py', '--in-progress']
        run_step('2.6 個股走勢刷新 (in-progress, FinMind)', cmd, required=False)

    # Step 3: build_html.py
    if args.skip_html:
        log('SKIP HTML 重建')
    else:
        cmd = [PYTHON, 'build_html.py']
        run_step('3. DB -> CB管理.html 重建', cmd, required=False)

    # Step 4: publish_cb.py
    if args.skip_publish or args.skip_html:
        log('SKIP GitHub Pages publish')
    else:
        cmd = [PYTHON, 'publish_cb.py']
        run_step('4. 推 CB管理.html 到 ianlife4/aurora', cmd, required=False)

    elapsed_total = time.time() - pipeline_start
    log('=' * 60)
    log(f'每日 MOPS 更新完成（耗時 {elapsed_total:.0f}s）')
    log(f'  新發 CB: {_STATE["new_cbs"]} 筆 · 新抓董事會日: {_STATE["board_added"]} · 新抓專戶日: {_STATE["account_added"]}')
    log('=' * 60)

    # TG 通知
    if not args.no_tg:
        ts = datetime.now().strftime('%H:%M')
        steps_n = len(_STATE['steps'])
        fails_n = len(_STATE['failures'])
        new_cbs = _STATE['new_cbs']
        board = _STATE['board_added']
        account = _STATE['account_added']

        if _STATE['fatal']:
            msg = (
                f'🚨 <b>MOPS Daily 中止</b> [{ts}]\n\n'
                f'必要步驟失敗：<code>{_STATE["fatal"]}</code>'
            )
            send_tg(msg, silent=False, parse_mode='HTML')
        elif new_cbs > 0 or board > 0 or account > 0:
            # 有新發現 → 主動通知
            msg = (
                f'🆕 <b>MOPS Daily</b> [{ts}]\n\n'
                f'• 新發 CB: <b>{new_cbs}</b> 筆\n'
                f'• 新抓董事會日: <b>{board}</b>\n'
                f'• 新抓專戶日: <b>{account}</b>\n'
                f'• {steps_n - fails_n}/{steps_n} steps OK ({elapsed_total:.0f}s)'
            )
            send_tg(msg, silent=False, parse_mode='HTML')
        elif fails_n > 0:
            msg = (
                f'⚠️ MOPS Daily 部分失敗 [{ts}]\n\n'
                f'失敗: {", ".join(_STATE["failures"])}'
            )
            send_tg(msg, silent=True, parse_mode='HTML')
        # 沒新增也沒失敗 → 完全靜默（每天都跑沒必要每天通知）


if __name__ == '__main__':
    main()
