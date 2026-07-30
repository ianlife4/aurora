@echo off
REM 每日:檢查公開說明書有沒有出新版 (定價版/生效版),有的話重跑分析 + 上線
REM 便宜:列清單是純 HTTP 零 API 成本;只有真的出新版才會花錢重 analyze
REM --limit 5 = 每天最多重跑 5 檔 (跟既有 CB Weekly Analyze 同樣的日花費節奏,不會一次爆量)
cd /d "%~dp0"
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
set PY=C:\Users\J.Chun\AppData\Local\Python\pythoncore-3.14-64\python.exe

echo === %DATE% %TIME% 公開說明書新鮮度檢查 ===
"%PY%" check_prospectus_freshness.py --fix --limit 5
if errorlevel 1 echo [WARN] freshness check 非零結束

REM 重新分類發債動機 (分析內容變了 → 標籤/占比/償還利率要跟著更新才不會 stale)
echo === 發債動機重新分類 ===
"%PY%" classify_cb_motive.py --since 2026-01-30 --write

echo === rebuild + publish ===
"%PY%" build_html.py
"%PY%" publish_cb.py
