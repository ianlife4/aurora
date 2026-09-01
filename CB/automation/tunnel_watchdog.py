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
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import requests

HERE = Path(__file__).parent
SERVE_PORT = 8767
WORKER_BASE = 'https://stock-dash.ian-4k.workers.dev'
REGISTER_TOKEN = 'xdZC4rryB3zwyl2gvEnu17DXuc7JcNOa'   # 同 start_with_tunnel.py
LOG_PATH = HERE / 'tunnel_watchdog.log'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0'


def log(msg):
    line = f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] {msg}'
    try:
        print(line, flush=True)
    except Exception:
        pass  # cp950 console 印不出 ✓✗ 之類字元;log 檔才是正史,別讓 print 炸掉流程
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


def current_url():
    """start_with_tunnel.py 落地的當前 quick tunnel URL (每次重啟都會變)。"""
    p = HERE / 'tunnel_url.txt'
    if p.exists():
        u = p.read_text(encoding='utf-8').strip()
        if u.startswith('https://'):
            return u
    # 退路:從 tunnel_restart.log 撈最後一個
    lg = HERE / 'tunnel_restart.log'
    if lg.exists():
        m = re.findall(r'https://[a-z0-9-]+\.trycloudflare\.com',
                       lg.read_text(encoding='utf-8', errors='replace'))
        if m:
            return m[-1]
    return None


def tunnel_ok(url):
    """這條 tunnel 從【外網】真的通嗎 — 本機 200 不代表雲端連得到。

    🔴 2026-08-31 血案:路由器 DNS (192.168.0.1) 對 trycloudflare 新亂數子網域回
       NXDOMAIN,requests 解析失敗 → 永遠判「外網不通」→ 每 10 分鐘死循環重啟
       (其實 cloudflared 已 Registered、worker 端都連得到,只有本機解析不了)。
       改用 curl --doh-url 走 1.1.1.1 的 DNS-over-HTTPS,完全繞過本機 DNS。
    """
    if not url:
        return False
    try:
        r = subprocess.run(
            ['curl', '-s', '-o', os.devnull, '-w', '%{http_code}',
             '--max-time', '25', '--doh-url', 'https://1.1.1.1/dns-query',
             '-A', UA, url + '/'],
            capture_output=True, text=True, timeout=35)
        return r.stdout.strip() == '200'
    except Exception:
        return False


def reregister(url):
    """重新註冊到 worker 續期。

    🔴 為什麼每次都要做 (2026-07-30 血案):worker 端註冊 TTL 只有 86400s = 1 天,
       過期後網頁按鈕就回「HTTP 502 tunnel offline」,但【本機 serve.py 和 cloudflared
       都還活得好好的】→ 舊版 watchdog 只檢查本機 8767,連續三天回報「健康,不動作」,
       完全沒發現雲端那頭早就連不到。
       本支 30 分鐘跑一次 → 每次續期,TTL 永遠不會走到期。
    """
    if not url:
        return False
    try:
        target = (f'{WORKER_BASE}/api/tunnel-register'
                  f'?token={quote(REGISTER_TOKEN)}&url={quote(url)}')
        r = requests.post(target, timeout=15, headers={'User-Agent': UA})
        return r.status_code == 200
    except Exception as e:
        log(f'  [WARN] 續期失敗: {e}')
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
        env={**os.environ, 'PYTHONIOENCODING': 'utf-8', 'TUNNEL_NO_BROWSER': '1'},
    )
    # 🔴 必須等到【外網】通才算成功,不能只等本機。
    #    start_with_tunnel 拉起 serve 只要 ~2s,但 cloudflared 建通道 + 寫 tunnel_url.txt
    #    + 自行註冊到 worker 還要 ~15s。太早返回會讀到【舊的/死的】URL 再註冊上去,
    #    等於把壞掉的通道又寫回 worker (2026-07-30 測試時實際踩到)。
    #    註冊由 start_with_tunnel 自己做,這裡只負責「等到真的通」。
    prev = current_url()
    for i in range(20):
        time.sleep(5)
        u = current_url()
        if u and u != prev and local_ok() and tunnel_ok(u):
            log(f'  ✓ 外網已通 (等了 {(i+1)*5}s): {u}')
            # 🔴 2026-09-01:這裡必須自己補註冊,不能全指望 start_with_tunnel。
            #    實案:22:03 重啟後 start_with_tunnel 走到「3. 抓到 URL」就無聲中斷
            #    (watchdog 排程結束時行程樹被回收,register 那步沒跑到),
            #    Worker 掛著舊 URL → 用戶按強制更新一直 502,要等下一輪 10 分鐘後
            #    的健康檢查續期才自癒。此刻 u 已確認是【新 URL 且外網通】,
            #    再註冊沒有 2026-07-30 那種「把舊 URL 寫回去」的 race。
            ok = reregister(u)
            log(f'  {"✓" if ok else "✗"} 已{"" if ok else "嘗試"}註冊新 URL 到 worker')
            return True
    log(f'  ✗ 重啟後 {20*5}s 內外網仍不通')
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--force', action='store_true', help='一律重啟')
    ap.add_argument('--check', action='store_true', help='只檢查不動手')
    args = ap.parse_args()

    lo = local_ok()
    url = current_url()
    tk = tunnel_ok(url)

    if args.check:
        log(f'檢查: 本機{SERVE_PORT}={"OK" if lo else "DOWN"} · 外網tunnel={"OK" if tk else "DOWN"} · {url}')
        sys.exit(0 if (lo and tk) else 1)

    if args.force:
        log('--force → 重啟')
        sys.exit(0 if restart() else 1)

    # 兩段式:本機掛 或 外網不通 → 整組重拉;都通 → 只續期 (POST 很便宜)
    if not lo:
        log(f'✗ 本機 {SERVE_PORT} 沒回應 → 重啟')
        ok = restart()
    elif not tk:
        log(f'✗ 本機 OK 但外網 tunnel 不通 ({url}) → 重啟')
        ok = restart()
    else:
        # 健康:仍要重新註冊續期,否則 TTL 1 天一到雲端就連不進來 (本機卻看起來很正常)
        ok = reregister(current_url())
        log(f'健康 (本機+外網皆通) · 續期{"成功" if ok else "失敗"} · {url}')
        sys.exit(0 if ok else 1)

    # 重啟成功時 start_with_tunnel 已自行註冊新 URL,這裡不再 reregister
    # (會race:讀到尚未更新的舊 URL → 把死掉的通道寫回 worker)
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
