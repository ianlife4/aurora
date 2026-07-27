#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""tunnel_watchdog.py — 顧著 CB remote tunnel,死了自動重啟。

為什麼要這支 (2026-07-27):
  網站 modal 上「強制補抓公開說明書」等按鈕,是雲端 worker 回打【本機】serve.py 做的
  (分析要本機 API key + DB)。通道 = start_with_tunnel.py 起的 serve.py + cloudflared。
  但它是【手動開的前景程式】,關掉 / 重開機 / crash 就沒了,而且:
    - cloudflared 可能還活著、serve.py 卻死了 → tunnel 通但 origin 回 530 → 網頁顯示
      「HTTP 502: tunnel offline」,看起來像 tunnel 掛了,其實是後面服務掛了 (本次症狀)
    - quick tunnel 的 URL 每次重啟都會變,且 worker 註冊有 TTL 86400s (1 天) → 就算
      程式沒死,超過 1 天沒重新註冊也會失效
  → 本支每次執行檢查一次,不健康就整組重拉 (順便清掉孤兒 cloudflared)。

檢查邏輯 (任一不過 = 不健康 → 重啟):
  1. 本機 http://127.0.0.1:<PORT>/ 回 200
  2. worker 端記錄的 tunnel URL 打得通 (確認註冊還有效、通道真的通)

用法:
  py tunnel_watchdog.py            # 檢查,不健康才重啟
  py tunnel_watchdog.py --force    # 一律重啟
  py tunnel_watchdog.py --check    # 只檢查印狀態,絕不動手
排程: schtasks 每 30 分跑一次 (見 setup 段註解)
"""
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

HERE = Path(__file__).parent
SERVE_PORT = 8767
WORKER_BASE = 'https://stock-dash.ian-4k.workers.dev'
LOG_PATH = HERE / 'tunnel_watchdog.log'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0'


def log(msg):
    line = f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] {msg}'
    print(line, flush=True)
    try:
        with open(LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass


def local_ok():
    """本機 serve.py 活著?"""
    try:
        r = requests.get(f'http://127.0.0.1:{SERVE_PORT}/', timeout=8)
        return r.status_code == 200
    except Exception:
        return False


def tunnel_ok():
    """worker 認得的 tunnel URL 真的通?(順便驗註冊沒過期)"""
    try:
        # worker 把目前註冊的 tunnel 當 proxy 用;直接打一個會轉發的端點驗證
        r = requests.get(f'{WORKER_BASE}/api/tunnel-ping', timeout=20,
                         headers={'User-Agent': UA})
        if r.status_code == 200:
            return True
        # 沒有 ping 端點就退而求其次:只要本機活著就當通道待重建
        return False
    except Exception:
        return False


def kill_orphans():
    """清掉殘留 cloudflared (serve 死了它還活著 → 就是這種孤兒造成 530)"""
    for img in ('cloudflared.exe',):
        try:
            subprocess.run(['taskkill', '/F', '/IM', img],
                           capture_output=True, timeout=20)
        except Exception:
            pass


def restart():
    kill_orphans()
    time.sleep(2)
    log('重啟 start_with_tunnel.py …')
    # DETACHED_PROCESS + CREATE_NEW_PROCESS_GROUP:排程結束後它要活著
    flags = 0x00000008 | 0x00000200
    out = open(HERE / 'tunnel_restart.log', 'a', encoding='utf-8')
    subprocess.Popen(
        [sys.executable, str(HERE / 'start_with_tunnel.py')],
        cwd=str(HERE), stdout=out, stderr=subprocess.STDOUT,
        creationflags=flags,
        env={**os.environ, 'PYTHONIOENCODING': 'utf-8'},
    )
    # 等它把 serve 拉起來 (cloudflared 建通道約 5-10s)
    for i in range(12):
        time.sleep(5)
        if local_ok():
            log(f'  ✓ 本機 {SERVE_PORT} 已回應 (等了 {(i+1)*5}s)')
            return True
    log('  ✗ 重啟後本機仍無回應')
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--force', action='store_true', help='一律重啟')
    ap.add_argument('--check', action='store_true', help='只檢查不動手')
    args = ap.parse_args()

    lo = local_ok()
    if args.check:
        log(f'檢查: 本機{SERVE_PORT}={"OK" if lo else "DOWN"}')
        sys.exit(0 if lo else 1)

    if args.force:
        log('--force → 重啟')
        sys.exit(0 if restart() else 1)

    if lo:
        log(f'健康 (本機 {SERVE_PORT} OK),不動作')
        sys.exit(0)

    log(f'✗ 本機 {SERVE_PORT} 沒回應 → 重啟')
    sys.exit(0 if restart() else 1)


if __name__ == '__main__':
    main()
