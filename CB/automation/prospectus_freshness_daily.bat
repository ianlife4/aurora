@echo off
REM Daily: check whether a newer prospectus (B021 revision / B022,B023 pricing / B05 effective) is on doc.twse;
REM if so, re-run the analysis and publish. Listing is plain HTTP (no API cost); --limit 5 caps API spend per day.
REM 2026-11-03: rewritten in pure ASCII + CRLF. The old UTF-8 (LF-only) version was mis-parsed by cmd (cp950):
REM   "set PY=" and the check/scan lines were swallowed, so since 2026-07-27 the scheduled task only ran
REM   build/publish with an empty interpreter path (exit 1). Keep this file ASCII-only.
cd /d "%~dp0"
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
set GIT_TERMINAL_PROMPT=0
set GCM_INTERACTIVE=never
set PY=C:\Users\J.Chun\AppData\Local\Python\pythoncore-3.14-64\python.exe
if not exist logs mkdir logs
set LOG=logs\prospectus_freshness_daily.log
echo === %DATE% %TIME% prospectus freshness start === >> %LOG%

"%PY%" -u check_prospectus_freshness.py --fix --limit 5 >> %LOG% 2>&1
if errorlevel 1 echo [WARN] check_prospectus_freshness exit %errorlevel% >> %LOG%

REM Mark which CBs without analysis already have a prospectus on doc.twse (dashboard list)
"%PY%" -u scan_prospectus_avail.py >> %LOG% 2>&1

REM Re-classify issuance motive (analysis text may have changed)
"%PY%" -u classify_cb_motive.py --since 2026-01-30 --write >> %LOG% 2>&1

"%PY%" -u build_html.py >> %LOG% 2>&1
"%PY%" -u publish_cb.py >> %LOG% 2>&1
echo === %DATE% %TIME% prospectus freshness done === >> %LOG%
exit /b 0
