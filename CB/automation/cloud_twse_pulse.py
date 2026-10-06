#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cloud_twse_pulse.py — 雲端 TWSE-only 輕量備援 (只給 GitHub Actions `cb-twse-backup.yml` 跑)。

為什麼有這支 (2026-10-06):
  cb-daily.yml 跑的 mops_daily 會先掃全市場 MOPS,在 runner 上 25 分鐘跑不完,07-17 起全數逾時 →
  排程已停。但本機「CB Pulse 30min」變成唯一 writer 之後,電腦沒開 (假日/出差) 網站就不更新。
  這支只跑 runner 上實測幾秒~幾十秒、不碰 MOPS 的 step,當本機沒開機時的備援:
    1. fetch_twse_upcoming           TWSE 即將開標 (bid/listing 同步到 issued)        ~3s
    2. fetch_twse_auction_results    TWSE 競拍結果回填 + 剛開標新案 INSERT          ~5s
    3. fetch_twsa_bookbuilding       券商公會 edoc 詢圈圈購期間 (只填空欄)             ~幾秒
    4. fill_missing_stocks           FinMind 補新發 CB 的母股到 stocks 表             ~20s
    5. fetch_premium_rally --in-progress   進行中案的個股走勢 (FinMind)              ~10s
  → 內容有變才 build_html,再由 workflow commit (HTML + DB + charts + analysis)。

「有變」怎麼判 (不能用檔案 diff):
  - build_html 會寫 built_at,HTML 每次必變;fetch_twse_upcoming 整表 DELETE+INSERT 帶 updated_at,
    DB 位元組每次必變 → 改用【表內容快照】:每張表去掉 *updated_at 類欄位後取 sha256,跑前跑後比對。
  - stocks 整表排除:本機 cron_pulse 不跑 fill_missing_stocks,雲端每輪都會把本機沒有的那幾筆補回來、
    下次本機發佈又蓋掉 → 不排除就永遠「有變」,每輪都 commit。stocks 只跟著其他變更一起 commit。
  - fm_stock_chart_json (個股走勢) 有算進去:交易日收盤後走勢會變 → 本機沒開機時雲端照樣更新圖。

跟本機的關係:本機 DB 是正本。publish_cb.py 發佈時 rebase 用 -X theirs 保留本機版,只把雲端
  「本機沒有的 cb_code」補回本機 (_merge_missing_cbs)。這支 INSERT 的新案一律用 TWSE 正式代號,
  跟本機 fetch_twse_auction_results 抓到的一致,不會撞號。
本機不要跑這支:同一套腳本 cron_pulse 每 30 分鐘都在跑,沒必要再動本機 DB。

輸出:有設 GITHUB_OUTPUT 時寫 `changed=true|false` + `summary=...` 給 workflow 決定要不要 commit。
exit code:0 正常 (含「沒變」);1 = build_html 失敗、或兩支 TWSE step 都失敗 (要讓 job 標紅才看得到)。
"""
import hashlib
import io
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

HERE = Path(__file__).parent
DB = HERE / 'cb_data.db'
PY = sys.executable

# (指令, 標籤, 逾時秒, 是否算「關鍵」) — 關鍵 step 全失敗才讓 job 標紅
STEPS = [
    (['fetch_twse_upcoming.py'],              'TWSE 即將開標',               180, True),
    (['fetch_twse_auction_results.py'],       'TWSE 競拍結果回填',           300, True),
    (['fetch_twsa_bookbuilding.py'],          '券商公會 詢圈圈購期間',       120, False),
    (['fill_missing_stocks.py'],              '補 stocks 缺漏 (FinMind)',    180, False),
    (['fetch_premium_rally.py', '--in-progress'], '個股走勢刷新 (FinMind)',  300, False),
]

SKIP_TABLES = {'stocks'}                              # 見檔頭:每輪必變,排除
TS_SUFFIXES = ('updated_at', 'checked_at', 'archived_at')   # 時間戳欄位,每輪必變,排除


def log(msg):
    print(f'[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}', flush=True)


def run(args, label, timeout):
    """跑一個 step,失敗/逾時只記 log 回 False,不中斷整輪 (同 cron_pulse.run)。"""
    log(f'--- {label} ---')
    try:
        r = subprocess.run([PY, *args], cwd=str(HERE), timeout=timeout,
                           capture_output=True, text=True, encoding='utf-8', errors='replace')
        for line in (r.stdout or '').splitlines():
            if line.strip():
                log(f'  | {line.rstrip()}')
        for line in (r.stderr or '').splitlines()[-15:]:
            if line.strip():
                log(f'  ! {line.rstrip()}')
        if r.returncode != 0:
            log(f'  [{label}] returncode={r.returncode} (繼續)')
        return r.returncode == 0
    except subprocess.TimeoutExpired as e:
        out = e.stdout or ''
        if isinstance(out, bytes):
            out = out.decode('utf-8', 'replace')
        for line in out.splitlines()[-20:]:
            if line.strip():
                log(f'  | {line.rstrip()}')
        log(f'  [{label}] TIMEOUT {timeout}s (繼續)')
        return False
    except Exception as e:
        log(f'  [{label}] 例外: {e} (繼續)')
        return False


def snapshot():
    """每張表 (去掉時間戳欄位、排除 stocks) 的內容 sha256 → {table: hex}。"""
    conn = sqlite3.connect(f'file:{DB.as_posix()}?mode=ro', uri=True)
    out = {}
    try:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        for t in tables:
            if t in SKIP_TABLES:
                continue
            cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{t}")')]
            keep = [c for c in cols if not c.endswith(TS_SUFFIXES)]
            if not keep:
                continue
            sel = ', '.join(f'"{c}"' for c in keep)
            rows = sorted(repr(r) for r in conn.execute(f'SELECT {sel} FROM "{t}"'))
            h = hashlib.sha256()
            for r in rows:
                h.update(r.encode('utf-8', 'replace'))
                h.update(b'\x1e')
            out[t] = h.hexdigest()
    finally:
        conn.close()
    return out


def write_output(changed: bool, summary: str):
    p = os.environ.get('GITHUB_OUTPUT')
    if not p:
        return
    with open(p, 'a', encoding='utf-8') as f:
        f.write(f'changed={"true" if changed else "false"}\n')
        f.write(f'summary={summary}\n')


def main() -> int:
    if not DB.exists():
        log(f'[ERR] DB 不存在: {DB}')
        write_output(False, 'db missing')
        return 1
    log('=== cloud_twse_pulse start ===')
    before = snapshot()

    crit_ok = 0
    for args, label, timeout, critical in STEPS:
        ok = run(args, label, timeout)
        if critical and ok:
            crit_ok += 1

    after = snapshot()
    changed = sorted(t for t in set(before) | set(after) if before.get(t) != after.get(t))
    if crit_ok == 0:
        log('[ERR] 兩支 TWSE step 都失敗 → 不 build,job 標紅')
        write_output(False, 'twse steps failed')
        return 1
    if not changed:
        log('內容沒變 (時間戳/stocks 以外) → 不 build、不 commit')
        write_output(False, 'no change')
        log('=== cloud_twse_pulse done (no change) ===')
        return 0

    log(f'內容有變: {", ".join(changed)} → build_html')
    if not run(['build_html.py'], 'DB -> CB管理.html', 300):
        log('[ERR] build_html 失敗 → 不 commit (半成品不能上線)')
        write_output(False, 'build_html failed')
        return 1
    write_output(True, 'changed: ' + ', '.join(changed))
    log('=== cloud_twse_pulse done (changed) ===')
    return 0


if __name__ == '__main__':
    sys.exit(main())
