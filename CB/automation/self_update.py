#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CB 自動更新主流程 (orchestrator)

完整鏈條：
  1a. Gmail 抓元富 (Masterlink) 新郵件 -> downloads/
  1b. Gmail 抓統一證 CBAS 新郵件 -> downloads/
  2.  解析 xlsx (元富+統一證) + TWSE 競拍 -> 更新 Excel + DB merge
  3.  Excel -> SQLite DB
  4.  TWSE/TPEx (或統一證 cache) 抓股票截標日收盤價 -> 算理論價 -> 寫回 DB
  5.  DB -> 重新產生 CB管理.html

執行：
  py -3.12 self_update.py                # 完整跑
  py -3.12 self_update.py --skip-gmail   # 跳過 Gmail 抓取（直接用 downloads/ 既有 xlsx）
  py -3.12 self_update.py --days 14      # Gmail 抓最近 14 天（預設 30）
  py -3.12 self_update.py --dry-run      # 全程乾跑，不改 Excel/DB

排程：建議用 Windows 工作排程器每週一 08:00 觸發。
"""
import argparse
import io
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# 強制 stdout/stderr UTF-8（避免 Windows cp950 編碼錯誤）
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

BASE_DIR = Path(__file__).parent
LOG_PATH = BASE_DIR / 'self_update.log'
PYTHON = sys.executable  # 用相同 Python interpreter

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


# 全域 pipeline 狀態（給 TG 通知用）
_PIPELINE_STATE = {'steps': [], 'failures': [], 'fatal': None}


def run_step(name: str, cmd: list[str], required: bool = True) -> bool:
    """跑一個子腳本，把輸出印出來 + 寫 log"""
    log(f'=== {name} ===')
    log(f'執行：{" ".join(cmd)}')
    t0 = time.time()
    # 強制子程式用 UTF-8 輸出（避免 cp950 亂碼）
    import os
    env = os.environ.copy()
    env['PYTHONIOENCODING'] = 'utf-8'
    try:
        result = subprocess.run(
            cmd, cwd=str(BASE_DIR),
            capture_output=True, text=True,
            encoding='utf-8', errors='replace',
            env=env,
            timeout=600  # 10 分鐘上限
        )
    except subprocess.TimeoutExpired:
        log(f'  [ERR] 逾時（>10 分鐘）')
        return False
    except Exception as e:
        log(f'  [ERR] 執行錯誤：{e}')
        return False

    elapsed = time.time() - t0
    # 把子程式輸出原樣印出（縮排）
    if result.stdout:
        for line in result.stdout.splitlines():
            print(f'  | {line}')
            try:
                with open(LOG_PATH, 'a', encoding='utf-8') as f:
                    f.write(f'  | {line}\n')
            except Exception:
                pass
    if result.stderr:
        # stderr 也寫進 log (publish_cb git push 失敗時的 git 錯誤訊息要看到)
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
    _PIPELINE_STATE['steps'].append((name, ok, elapsed))
    if not ok:
        _PIPELINE_STATE['failures'].append(name)
        if required:
            _PIPELINE_STATE['fatal'] = name
            log(f'  -> 必要步驟失敗，中止')
    return ok


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--skip-gmail', action='store_true', help='跳過 Gmail 抓取')
    parser.add_argument('--skip-twse',  action='store_true', help='跳過 TWSE 抓取（要改 run_update.py 才會生效，目前只是標記）')
    parser.add_argument('--skip-html',  action='store_true', help='跳過 HTML 重建')
    parser.add_argument('--skip-publish', action='store_true', help='跳過推到 GitHub Pages')
    parser.add_argument('--days',       type=int, default=30, help='Gmail 搜尋天數（預設 30）')
    parser.add_argument('--dry-run',    action='store_true', help='乾跑模式')
    parser.add_argument('--no-tg',      action='store_true', help='關閉 TG 通知')
    args = parser.parse_args()

    log('=' * 60)
    log(f'CB 自動更新開始 · mode={"DRY-RUN" if args.dry_run else "LIVE"}')
    log('=' * 60)

    # 收集每個 step 的成功/失敗狀態
    step_status = []
    failures = []

    def _record(name, ok):
        step_status.append((name, ok))
        if not ok:
            failures.append(name)

    pipeline_start = time.time()

    # -- Step 1a: Gmail Masterlink ------------------------------
    if args.skip_gmail:
        log('SKIP Gmail 抓取')
    else:
        cmd = [PYTHON, 'gmail_fetch_masterlink.py', '--days', str(args.days)]
        if args.dry_run:
            cmd.append('--dry-run')
        ok = run_step('1a. Gmail 抓取 元富 新附件', cmd, required=False)
        if not ok:
            log('  -> Masterlink Gmail 失敗，繼續後面')

        # Step 1b: Unisec
        cmd = [PYTHON, 'gmail_fetch_unisec.py', '--days', str(args.days)]
        if args.dry_run:
            cmd.append('--dry-run')
        ok = run_step('1b. Gmail 抓取 統一證 新附件', cmd, required=False)
        if not ok:
            log('  -> 統一證 Gmail 失敗，繼續後面')

    # -- Step 1c: 掃 MOPS 找元富 mail 還沒收到的新發 CB ----------
    if args.dry_run:
        log('SKIP MOPS 新案掃描（dry-run）')
    else:
        cmd = [PYTHON, 'discover_new_cbs.py', '--days', '60']
        ok = run_step('1c. 掃 MOPS 新發 CB（補元富 mail 沒收到的）', cmd, required=False)
        if not ok:
            log('  -> MOPS 新案掃描失敗，繼續後面')

    # -- Step 2: 解析 + TWSE -> Excel ----------------------------
    cmd = [PYTHON, 'run_update.py']
    if args.dry_run:
        cmd.append('--dry-run')
    ok = run_step('2. 解析 xlsx + TWSE 抓取 -> 寫 Excel', cmd, required=True)
    if not ok:
        log('=' * 60)
        log('終止：步驟 2 失敗')
        log('=' * 60)
        sys.exit(1)

    # -- Step 3: Excel -> DB -------------------------------------
    if args.dry_run:
        log('SKIP Excel->DB 同步（dry-run）')
    else:
        cmd = [PYTHON, 'migrate_excel.py']
        ok = run_step('3. Excel -> SQLite DB 同步', cmd, required=True)
        if not ok:
            log('=' * 60)
            log('終止：步驟 3 失敗')
            log('=' * 60)
            sys.exit(1)

    # -- Step 4: TWSE/TPEx 收盤價回填 -----------------------------
    if args.dry_run:
        log('SKIP 收盤價回填（dry-run）')
    else:
        cmd = [PYTHON, 'fetch_close_prices.py']
        ok = run_step('4. TWSE/TPEx 收盤價回填 + 算理論價', cmd, required=False)
        if not ok:
            log('  -> 收盤價回填失敗，繼續產 HTML（已存在資料不受影響）')

    # -- Step 4.5: FinMind CB 二級市場資料 ------------------------
    if args.dry_run:
        log('SKIP FinMind CB 二級市場（dry-run）')
    else:
        cmd = [PYTHON, 'fetch_cb_market.py']
        ok = run_step('4.5 FinMind CB 二級市場資料抓取', cmd, required=False)
        if not ok:
            log('  -> FinMind CB 抓取失敗，繼續產 HTML（既有 fm_* 資料不受影響）')

    # -- Step 4.6: 個股漲跌 + CB 首日溢價分析 ----------------------
    if args.dry_run:
        log('SKIP 個股漲跌+CB首日（dry-run）')
    else:
        cmd = [PYTHON, 'fetch_premium_rally.py']
        ok = run_step('4.6 個股漲跌 + CB 首日溢價分析', cmd, required=False)
        if not ok:
            log('  -> premium_rally 抓取失敗，繼續產 HTML（既有資料不受影響）')

    # -- Step 4.7: TWSE 即將開標官方公告 -----------------------------
    if args.dry_run:
        log('SKIP TWSE 即將開標 (dry-run)')
    else:
        cmd = [PYTHON, 'fetch_twse_upcoming.py']
        ok = run_step('4.7 TWSE 即將開標公告抓取', cmd, required=False)
        if not ok:
            log('  -> TWSE 即將開標抓取失敗，繼續產 HTML')

    # -- Step 4.8: auto_analyze_cb (Claude API 寫公開說明書分析) --
    if args.dry_run:
        log('SKIP auto_analyze_cb (dry-run)')
    else:
        cmd = [PYTHON, 'auto_analyze_cb.py', '--limit', '5']  # weekly 跑可多一點 (~$4)
        ok = run_step('4.8 自動產 CB 公開說明書分析 (Claude API)', cmd, required=False)
        if not ok:
            log('  -> auto_analyze_cb 失敗,繼續產 HTML')

    # -- Step 4.9: 即將開標但缺 conv_price 預警 (TG) --------------
    # 「沒競拍建議價」根因 sweep — 跟 mops_daily Step 2.7 同支腳本
    if not args.dry_run:
        cmd = [PYTHON, 'check_upcoming_conv_price.py', '--days', '5']
        run_step('4.9 即將開標缺 conv_price 預警 (TG)', cmd, required=False)

    # -- Step 5: DB -> HTML --------------------------------------
    if args.skip_html or args.dry_run:
        log('SKIP HTML 重建')
    else:
        cmd = [PYTHON, 'build_html.py']
        ok = run_step('5. DB -> CB管理.html 重建', cmd, required=False)
        if not ok:
            log('  -> HTML 重建失敗，但 DB 已更新，可手動重跑 build_html.py')

    # -- Step 6: Publish to GitHub Pages -------------------------
    if args.skip_html or args.dry_run or getattr(args, 'skip_publish', False):
        log('SKIP GitHub Pages publish')
    else:
        cmd = [PYTHON, 'publish_cb.py']
        ok = run_step('6. 推 CB管理.html 到 ianlife4/aurora', cmd, required=False)
        if not ok:
            log('  -> publish 失敗，本機 HTML 仍可用，dashboard 會顯示舊版')

    elapsed_total = time.time() - pipeline_start
    log('=' * 60)
    log(f'全部完成（耗時 {elapsed_total:.0f}s）')
    log('=' * 60)

    # TG 通知（如果 tg_config.txt 有設定）
    if not args.no_tg and not args.dry_run:
        from datetime import datetime as _dt
        ts = _dt.now().strftime('%H:%M')
        steps_n = len(_PIPELINE_STATE['steps'])
        fails_n = len(_PIPELINE_STATE['failures'])

        if _PIPELINE_STATE['fatal']:
            # 必要步驟失敗 → 紅色警報
            msg = (
                f'🚨 <b>CB Pipeline 中止</b> [{ts}]\n\n'
                f'必要步驟失敗：<code>{_PIPELINE_STATE["fatal"]}</code>\n\n'
                f'其他失敗 ({fails_n}):\n' +
                '\n'.join(f'• {n}' for n in _PIPELINE_STATE['failures']) + '\n\n'
                f'看 self_update.log debug'
            )
            send_tg(msg, silent=False, parse_mode='HTML')
        elif fails_n > 0:
            # 部分失敗 → 黃色警告
            msg = (
                f'⚠️ <b>CB Pipeline 部分失敗</b> [{ts}]\n\n'
                f'{steps_n - fails_n}/{steps_n} 成功，{fails_n} 失敗\n\n'
                f'失敗步驟:\n' + '\n'.join(f'• {n}' for n in _PIPELINE_STATE['failures']) + '\n\n'
                f'耗時 {elapsed_total:.0f}s · HTML 已 publish'
            )
            send_tg(msg, silent=False, parse_mode='HTML')
        else:
            # 全部成功 → 安靜成功通知（dashboard 自動更新摘要）
            try:
                import sqlite3
                conn = sqlite3.connect(str(BASE_DIR / 'cb_data.db'))
                n_issued = conn.execute('SELECT COUNT(*) FROM issued').fetchone()[0]
                n_auc = conn.execute('SELECT COUNT(*) FROM auctions').fetchone()[0]
                n_with_board = conn.execute('SELECT COUNT(*) FROM issued WHERE fm_board_decision_date IS NOT NULL').fetchone()[0]
                conn.close()
            except Exception:
                n_issued = n_auc = n_with_board = 0
            msg = (
                f'✅ CB Pipeline 完成 [{ts}]\n\n'
                f'• issued: {n_issued} 筆\n'
                f'• auctions: {n_auc} 筆\n'
                f'• 有董事會決議: {n_with_board} 筆\n'
                f'• {steps_n} steps 全部成功 ({elapsed_total:.0f}s)\n\n'
                f'Dashboard: stock-dash.ian-4k.workers.dev/cb'
            )
            send_tg(msg, silent=True, parse_mode='HTML')


if __name__ == '__main__':
    main()
