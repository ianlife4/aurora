#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本機 server + cloudflared quick tunnel 一鍵啟動。

流程：
  1. 啟 serve.py (port 8765)
  2. 啟 cloudflared tunnel --url http://localhost:8765
  3. 解析 cloudflared 輸出抓 *.trycloudflare.com URL
  4. POST 到 stock-dash worker /api/tunnel-register?token=...&url=...
     之後 worker 的 /api/update 就會代轉到本機
  5. 開瀏覽器到 dashboard
  6. Ctrl-C 停掉兩個都殺

執行：py -3.12 start_with_tunnel.py
"""
import io
import os
import re
import signal
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path
from urllib.parse import quote

import requests

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

HERE = Path(__file__).parent
PYTHON = sys.executable
CLOUDFLARED = Path(r'C:\Users\J.Chun\AppData\Local\Microsoft\WinGet\Packages\Cloudflare.cloudflared_Microsoft.Winget.Source_8wekyb3d8bbwe\cloudflared.exe')
SERVE_PORT = 8767

WORKER_BASE = 'https://stock-dash.ian-4k.workers.dev'
REGISTER_TOKEN = 'xdZC4rryB3zwyl2gvEnu17DXuc7JcNOa'  # 對應 worker secret CB_TUNNEL_REGISTER_TOKEN

# 開啟對應的 dashboard URL 而不是本機（這樣可以驗證 tunnel 通了）
DASHBOARD_URL = f'{WORKER_BASE}/cb/'

procs = []


def _ts():
    return time.strftime('%H:%M:%S')


def log(msg, *, prefix='[main]'):
    print(f'{_ts()} {prefix} {msg}', flush=True)


def stream_reader(name: str, proc: subprocess.Popen, on_line=None):
    """背景把子程式 stdout 轉印出來，每行也丟給 callback。"""
    def _run():
        for raw in iter(proc.stdout.readline, b''):
            try:
                line = raw.decode('utf-8', errors='replace').rstrip()
            except Exception:
                line = repr(raw)
            print(f'{_ts()} [{name}] {line}', flush=True)
            if on_line:
                try:
                    on_line(line)
                except Exception as e:
                    log(f'on_line err: {e}')
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t


def register_tunnel(public_url: str) -> bool:
    target = (
        f'{WORKER_BASE}/api/tunnel-register'
        f'?token={quote(REGISTER_TOKEN)}'
        f'&url={quote(public_url)}'
    )
    try:
        # 帶瀏覽器 UA:Cloudflare bot 防護會擋 python-requests 預設 UA → 403 (2026-07 起)
        r = requests.post(target, timeout=15,
                          headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
        log(f'register -> HTTP {r.status_code}: {r.text[:200]}')
        return r.status_code == 200
    except Exception as e:
        log(f'register error: {e}')
        return False


def shutdown(*_):
    log('收到中止訊號，停掉子程式…')
    for p in procs:
        try:
            p.terminate()
        except Exception:
            pass
    time.sleep(1)
    for p in procs:
        if p.poll() is None:
            try:
                p.kill()
            except Exception:
                pass
    sys.exit(0)


def main():
    if not CLOUDFLARED.exists():
        sys.exit(f'[ERR] cloudflared 不在：{CLOUDFLARED}')

    signal.signal(signal.SIGINT, shutdown)
    if hasattr(signal, 'SIGBREAK'):
        signal.signal(signal.SIGBREAK, shutdown)

    log(f'1. 啟動 serve.py (port {SERVE_PORT})…')
    serve = subprocess.Popen(
        [PYTHON, str(HERE / 'serve.py'), '--port', str(SERVE_PORT), '--no-browser'],
        cwd=str(HERE),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        env={**os.environ, 'PYTHONIOENCODING': 'utf-8'},
    )
    procs.append(serve)
    stream_reader('serve', serve)
    time.sleep(2)
    if serve.poll() is not None:
        log('[ERR] serve.py 啟動失敗')
        return

    log(f'2. 啟動 cloudflared quick tunnel → http://localhost:{SERVE_PORT}…')
    cf = subprocess.Popen(
        [str(CLOUDFLARED), 'tunnel', '--url', f'http://localhost:{SERVE_PORT}'],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    procs.append(cf)

    captured_url = {'value': None}
    url_pat = re.compile(r'(https://[a-z0-9-]+\.trycloudflare\.com)', re.I)

    def on_cf_line(line: str):
        if captured_url['value']:
            return
        m = url_pat.search(line)
        if m:
            url = m.group(1)
            captured_url['value'] = url
            log(f'3. 抓到 tunnel URL：{url}')
            # 存檔給 tunnel_watchdog.py 續期用 (worker 註冊 TTL 只有 1 天,
            # 但 quick tunnel URL 每次重啟都會變 → 必須把當前 URL 落地)
            try:
                (HERE / 'tunnel_url.txt').write_text(url, encoding='utf-8')
            except Exception as e:
                log(f'   [WARN] 寫 tunnel_url.txt 失敗: {e}')
            # 註冊到 worker
            time.sleep(2)  # 等 tunnel 真的 up
            for attempt in range(1, 4):
                if register_tunnel(url):
                    log(f'4. ✓ 已通知 worker。dashboard 上「強制更新」現在可以從外面按了')
                    log(f'   開啟：{DASHBOARD_URL}')
                    try:
                        webbrowser.open(DASHBOARD_URL)
                    except Exception:
                        pass
                    return
                log(f'   register 重試 {attempt}/3…')
                time.sleep(2)
            log('[WARN] register 失敗，dashboard 上的更新按鈕會回 503')

    stream_reader('cloudflared', cf, on_line=on_cf_line)

    # block 等任一個子程式結束
    while True:
        time.sleep(2)
        for p in procs:
            if p.poll() is not None:
                log(f'子程式退出 (pid={p.pid}, exit={p.poll()})')
                shutdown()


if __name__ == '__main__':
    main()
