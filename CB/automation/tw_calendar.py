#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""tw_calendar.py — 台股交易日曆 (唯一真相來源)。

為什麼要這支 (2026-09-28 用戶:「9/8 T-3 但是今天休市欸」):
  訂價窗口的 T-5 / T-3 / T-1 是「訂價基準日前 1/3/5 個【營業日】的收盤價」,
  但原本 HTML 的 pwBizAdd/pwBizDiff 只跳週末、不跳國定假日:
      2026-09-25 中秋 + 09-28 教師節補休 連在一起 →
      T=10/01 時,T-3 被算成 09/28 (休市,根本沒有收盤價),正確是 09/24。
  Python 端也有兩處同樣寫法 (cb_listing_advisor.next_trading_day、run_update 取前一交易日)。
  → 全部改用本模組,假日表集中一份。

資料來源 (2026-10-26 起三路,見用戶「cb定價連假跟颱風天有遞延處理嗎」稽核):
  1. 統一證 xlsx 的 `holiday` 分頁 (source='unisec-xlsx'):國定假日 + 他們手動補的颱風假。
     每次 backfill_primary_dates 跑就同步進 DB 的 holidays 表。⚠ 週報一週一份、颱風假要等好幾天,
     2026-07-10 颱風晚 11 天才進表;2026-10-23 薔琵颱風到 10-26 都還沒有 → 線上 T-1 落在沒收盤的日子。
  2. TWSE 新聞 (source='twse-news'):「臺灣證券交易所集中交易市場115年10月23日休市一天」這種公告
     在前一晚 20 點多就發 (07-09 20:11 / 10-22 20:10),每輪 pulse 抓一次 → 颱風假當晚就進表。
     ⚠ TWSE holidaySchedule API 只有【預排】假日、不含颱風,而且混了「開始交易日/最後交易日」這種有交易的列。
  3. TWSE 每月成交統計 FMTQIK (source='twse-fmtqik'):事後對帳 — 平日沒有成交列 = 休市。一天一次,
     補漏 (2023-08-03 卡努、2024-02-06/07 僅交割 統一證從來沒列) 兼稽核 (表裡標休市但其實有成交 → 告警)。
  另外 TWSE holidaySchedule 公布該年度開休市表後 (通常前一年 12 月),拿來校正統一證的【預估】
  (2027 統一證把中秋寫成 10-08,真中秋 09-15 沒列):缺的補進來 (source='twse-schedule'),
  未來的錯列刪掉並記進 holidays_suppressed,避免下一份 xlsx 又把它加回來。

兩套日曆 (2026-10-26):
  - holidays()          全部休市日 → 收盤價相關 (T-5/T-3/T-1、「前一交易日收盤」) 一律用這套。
  - planned_holidays()  只含【預排】假日 (去掉颱風/臨時休市) → 承銷時程的【回推】用這套:
    競拍 T = 投標起 −3 營業日,約定書是颱風宣布【之前】申報的,當時那天還算營業日;
    照實際日曆跳過颱風會把 T 推早一天 (光譜四 10-21 → 10-20)。往後推 (詢圈 圈購迄 +2) 仍用實際日曆。

⚠ 覆蓋範圍會過期:假日表只到次年年底,超出範圍就退化成「只跳週末」(會錯)。
  `coverage_ok()` 供稽核呼叫,快到期要提醒補表。
"""
import re
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

HERE = Path(__file__).parent
DB_PATH = HERE / 'cb_data.db'
CB_DROP = Path(r'C:\Users\J.Chun\Desktop\stock-dash\cbas-template\CB報')

# 臨時休市 (颱風那類,前一晚才宣布) 的來源 / 名稱特徵 → planned_holidays() 排除它們
ADHOC_SOURCES = ('twse-news', 'twse-fmtqik', 'manual')
ADHOC_NAME_RE = re.compile(r'颱風|臨時|停班|天災')

TWSE_NEWS_API = 'https://openapi.twse.com.tw/v1/news/newsList'
TWSE_NEWS_RSS = 'https://www.twse.com.tw/rwd/zh/news/feed?type=rss'
TWSE_FMTQIK = 'https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK'
TWSE_SCHEDULE = 'https://www.twse.com.tw/rwd/zh/holidaySchedule/holidaySchedule'
HTTP_TIMEOUT = 25

_CACHE = None        # set[str] 'YYYY-MM-DD' (全部)
_CACHE_ROWS = None   # dict date -> (name, source)
_CACHE_RANGE = None
_CACHE_PLANNED = None


def _reset_cache():
    global _CACHE, _CACHE_ROWS, _CACHE_RANGE, _CACHE_PLANNED
    _CACHE = _CACHE_ROWS = _CACHE_RANGE = _CACHE_PLANNED = None


def ensure_table(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS holidays (
        date TEXT PRIMARY KEY, name TEXT, source TEXT, updated_at TEXT)''')
    cols = {r[1] for r in conn.execute('PRAGMA table_info(holidays)')}
    if 'first_seen_at' not in cols:
        # 哪一天進表的 (只在 INSERT 時寫) — 稽核「颱風假晚幾天才進來」用;updated_at 每輪 upsert 都會變,看不出來
        conn.execute('ALTER TABLE holidays ADD COLUMN first_seen_at TEXT')
    conn.execute('''CREATE TABLE IF NOT EXISTS holidays_suppressed (
        date TEXT PRIMARY KEY, reason TEXT, created_at TEXT)''')


def _now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _is_adhoc(name, source):
    s = str(source or '')
    return s.startswith(ADHOC_SOURCES) or bool(ADHOC_NAME_RE.search(str(name or '')))


def _suppressed(conn):
    try:
        return {r[0] for r in conn.execute('SELECT date FROM holidays_suppressed')}
    except sqlite3.Error:
        return set()


def add_holiday(conn, iso, name, source, overwrite_name=False):
    """補一天休市。已存在 → 不動 (overwrite_name=True 才改名/來源)。回 True=新增。"""
    ensure_table(conn)
    iso = str(iso)[:10]
    if iso in _suppressed(conn) and not str(source).startswith('manual'):
        return False
    now = _now()
    before = conn.execute('SELECT 1 FROM holidays WHERE date=?', (iso,)).fetchone()
    if before:
        if overwrite_name:
            conn.execute('UPDATE holidays SET name=?, source=?, updated_at=? WHERE date=?', (name, source, now, iso))
        return False
    conn.execute('INSERT INTO holidays(date,name,source,updated_at,first_seen_at) VALUES(?,?,?,?,?)',
                 (iso, name, source, now, now))
    if str(source).startswith('manual'):
        conn.execute('DELETE FROM holidays_suppressed WHERE date=?', (iso,))
    _reset_cache()
    return True


def import_from_xlsx(conn, path=None, verbose=False):
    """把統一證 xlsx 的 holiday 分頁同步進 DB。回 (新增, 總筆數)。"""
    import glob
    import openpyxl
    if path is None:
        files = sorted(glob.glob(str(CB_DROP / 'CB發行資訊與CBAS報價表_統一證_*.xlsx')))
        if not files:
            return 0, 0
        path = files[-1]
    try:
        wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
        if 'holiday' not in wb.sheetnames:
            wb.close()
            return 0, 0
        ws = wb['holiday']
        rows = []
        for r in ws.iter_rows(values_only=True):
            if not r or r[0] is None:
                continue
            v = r[0]
            iso = v.strftime('%Y-%m-%d') if isinstance(v, (datetime, date)) else str(v)[:10]
            if len(iso) == 10 and iso[4] == '-':
                rows.append((iso, str(r[1] or '').strip()))
        wb.close()
    except Exception as e:
        if verbose:
            print(f'  [WARN] holiday 分頁讀取失敗: {e}')
        return 0, 0
    ensure_table(conn)
    sup = _suppressed(conn)
    before = conn.execute('SELECT COUNT(*) FROM holidays').fetchone()[0]
    now = _now()
    # 被 TWSE 官方開休市表否決過的日期 (holidays_suppressed) 不再從 xlsx 加回來
    conn.executemany(
        'INSERT INTO holidays(date,name,source,updated_at,first_seen_at) VALUES(?,?,?,?,?) '
        'ON CONFLICT(date) DO UPDATE SET name=excluded.name, updated_at=excluded.updated_at '
        "WHERE holidays.source='unisec-xlsx'",
        [(d, n, 'unisec-xlsx', now, now) for d, n in rows if d not in sup])
    after = conn.execute('SELECT COUNT(*) FROM holidays').fetchone()[0]
    _reset_cache()
    return after - before, after


# ── 讀取 ──────────────────────────────────────────────────────────────
def _load(conn=None):
    global _CACHE, _CACHE_ROWS, _CACHE_RANGE, _CACHE_PLANNED
    if _CACHE is not None:
        return
    own = conn is None
    if own:
        if not DB_PATH.exists():
            _CACHE, _CACHE_ROWS, _CACHE_RANGE, _CACHE_PLANNED = set(), {}, None, set()
            return
        conn = sqlite3.connect(str(DB_PATH))
    try:
        ensure_table(conn)
        rows = conn.execute('SELECT date, name, source FROM holidays').fetchall()
    except Exception:
        rows = []
    finally:
        if own:
            conn.close()
    _CACHE_ROWS = {r[0]: (r[1], r[2]) for r in rows}
    _CACHE = set(_CACHE_ROWS)
    _CACHE_PLANNED = {d for d, (n, s) in _CACHE_ROWS.items() if not _is_adhoc(n, s)}
    _CACHE_RANGE = (min(_CACHE), max(_CACHE)) if _CACHE else None


def holidays(conn=None):
    """回 set('YYYY-MM-DD') — 全部休市日 (含颱風/臨時)。第一次讀之後快取在記憶體。"""
    _load(conn)
    return _CACHE


def planned_holidays(conn=None):
    """只含【預排】假日 (國定假日、補休、僅交割日),不含颱風/臨時休市 → 承銷時程回推用。"""
    _load(conn)
    return _CACHE_PLANNED


def adhoc_holidays(conn=None):
    """颱風/臨時休市日 (全部 − 預排)。給 build_html 送前端分兩套日曆。"""
    _load(conn)
    return _CACHE - _CACHE_PLANNED


def holiday_rows(conn=None):
    """dict date -> (name, source)。"""
    _load(conn)
    return dict(_CACHE_ROWS)


def coverage():
    """回 (最早, 最晚) 假日日期;空表回 None。"""
    _load()
    return _CACHE_RANGE


def coverage_ok(through=None):
    """假日表是否涵蓋到指定日期 (預設:今天 +120 天)。稽核用。"""
    rng = coverage()
    if not rng:
        return False
    end = through or (date.today() + timedelta(days=120)).isoformat()
    return rng[1] >= end


def _d(x):
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    return date.fromisoformat(str(x)[:10])


def is_trading_day(d, hols=None):
    d = _d(d)
    hols = holidays() if hols is None else hols
    return d.weekday() < 5 and d.isoformat() not in hols


def biz_add(d, n, hols=None):
    """往後(正)/往前(負)推 n 個交易日。n=0 回原日 (不校正成交易日)。"""
    d = _d(d)
    if n == 0:
        return d
    hols = holidays() if hols is None else hols
    step = 1 if n > 0 else -1
    cnt = 0
    guard = 0
    while cnt != n:
        d += timedelta(days=step)
        guard += 1
        if guard > 3000:
            raise RuntimeError('biz_add 沒收斂 — 假日表可能異常')
        if is_trading_day(d, hols):
            cnt += step
    return d


def biz_add_planned(d, n):
    """同 biz_add,但只跳【預排】假日 (颱風/臨時休市算營業日)。承銷時程回推 (競拍 T = 投標起 −3) 用。"""
    return biz_add(d, n, hols=planned_holidays())


def biz_diff(a, b, hols=None):
    """a → b 之間有幾個交易日 (b 在 a 之後為正)。起點不計、終點計。"""
    a, b = _d(a), _d(b)
    if a == b:
        return 0
    hols = holidays() if hols is None else hols
    step = 1 if b > a else -1
    n = 0
    d = a
    guard = 0
    while d != b:
        d += timedelta(days=step)
        guard += 1
        if guard > 3000:
            raise RuntimeError('biz_diff 沒收斂')
        if is_trading_day(d, hols):
            n += step
    return n


def next_trading_day(d=None, hols=None):
    d = _d(d or date.today())
    hols = holidays() if hols is None else hols
    d += timedelta(days=1)
    while not is_trading_day(d, hols):
        d += timedelta(days=1)
    return d


def prev_trading_day(d=None, hols=None):
    d = _d(d or date.today())
    hols = holidays() if hols is None else hols
    d -= timedelta(days=1)
    while not is_trading_day(d, hols):
        d -= timedelta(days=1)
    return d


def effective_trading_day(d, hols=None):
    """排定日落在休市日 (颱風/假日) → 順延到下一個交易日;本來就是交易日就回原日。
    撥券/掛牌日用:啟碁五 62855 撥券 2026-10-23 撞颱風,TWSE 沒改日期,慣例首日 = 10-27。"""
    d = _d(d)
    hols = holidays() if hols is None else hols
    return d if is_trading_day(d, hols) else next_trading_day(d, hols)


# ── 即時來源:TWSE 新聞 ───────────────────────────────────────────────
_CLOSE_ONE = re.compile(r'(\d{2,3})年(\d{1,2})月(\d{1,2})日休市一天')
_CLOSE_RANGE = re.compile(r'(\d{2,3})年(\d{1,2})月(\d{1,2})日至(?:(\d{2,3})年)?(\d{1,2})月(\d{1,2})日證券市場休市')


def _roc(y):
    y = int(y)
    return y + 1911 if y < 1911 else y


def parse_twse_close_title(title):
    """「臺灣證券交易所集中交易市場115年10月23日休市一天」→ ['2026-10-23'];
    「115年2月16日至2月20日證券市場休市」→ 那幾天 (預排的春節,已在表裡,DO NOTHING)。"""
    out = []
    m = _CLOSE_ONE.search(title or '')
    if m:
        try:
            out.append(date(_roc(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat())
        except ValueError:
            pass
        return out
    m = _CLOSE_RANGE.search(title or '')
    if m:
        try:
            y1 = _roc(m.group(1))
            y2 = _roc(m.group(4)) if m.group(4) else y1
            a = date(y1, int(m.group(2)), int(m.group(3)))
            b = date(y2, int(m.group(5)), int(m.group(6)))
            d = a
            while d <= b and (d - a).days < 15:
                if d.weekday() < 5:
                    out.append(d.isoformat())
                d += timedelta(days=1)
        except ValueError:
            pass
    return out


def fetch_twse_news_titles():
    """回 [(title, date_iso_or_None)]。先 OpenAPI (677 則),失敗退 RSS (74 則)。"""
    import requests
    try:
        r = requests.get(TWSE_NEWS_API, timeout=HTTP_TIMEOUT, headers={'User-Agent': 'Mozilla/5.0'})
        r.raise_for_status()
        out = []
        for it in r.json():
            t = it.get('Title') or ''
            d = str(it.get('Date') or '')
            iso = None
            if len(d) == 7 and d.isdigit():
                iso = f'{int(d[:3]) + 1911}-{d[3:5]}-{d[5:7]}'
            out.append((t, iso))
        if out:
            return out
    except Exception:
        pass
    r = requests.get(TWSE_NEWS_RSS, timeout=HTTP_TIMEOUT, headers={'User-Agent': 'Mozilla/5.0'})
    r.raise_for_status()
    return [(t, None) for t in re.findall(r'<title>([^<]*)</title>', r.text)]


def import_from_twse_news(conn, verbose=False):
    """TWSE 新聞標題「…N年M月D日休市一天」→ 補進 holidays (source='twse-news')。回新增的日期 list。"""
    ensure_table(conn)
    try:
        titles = fetch_twse_news_titles()
    except Exception as e:
        if verbose:
            print(f'  [WARN] TWSE 新聞抓取失敗: {e}')
        return []
    added = []
    for title, _ in titles:
        for iso in parse_twse_close_title(title):
            if add_holiday(conn, iso, '臨時休市 (TWSE 公告)', 'twse-news'):
                added.append(iso)
                if verbose:
                    print(f'  + {iso} ← {title}')
    if added:
        conn.commit()
    return added


# ── 事後對帳:TWSE 每月成交統計 (FMTQIK) ──────────────────────────────
def fetch_fmtqik_days(year, month):
    """回該月有成交的日期 set('YYYY-MM-DD');抓不到回 None。"""
    import requests
    try:
        r = requests.get(TWSE_FMTQIK, params={'date': f'{year}{month:02d}01', 'response': 'json'},
                         timeout=HTTP_TIMEOUT, headers={'User-Agent': 'Mozilla/5.0'})
        r.raise_for_status()
        j = r.json()
    except Exception:
        return None
    if j.get('stat') not in ('OK', 'ok') and not j.get('data'):
        return None
    out = set()
    for row in j.get('data') or []:
        s = str(row[0]).strip()
        m = re.match(r'(\d{2,3})/(\d{1,2})/(\d{1,2})', s)
        if m:
            out.add(date(_roc(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat())
    return out


def reconcile_fmtqik(conn, months_back=2, apply=True, verbose=False, today=None):
    """平日沒成交列又不在表裡 → 補 (source='twse-fmtqik');表裡標休市但其實有成交 → 回報 (不刪)。
    回 {'added': [...], 'conflicts': [...], 'months': n}。只看「API 已經有資料走過」的日子 (≤ 該月最後一筆)。"""
    ensure_table(conn)
    today = _d(today or date.today())
    hols = holidays(conn)
    added, conflicts = [], []
    checked = 0
    sched_cache = {}     # year -> {iso: name} 或 None;補進來的日子若在官方開休市表裡 = 預排假日,不要標成臨時
    y, m = today.year, today.month
    for _ in range(months_back + 1):
        days = fetch_fmtqik_days(y, m)
        if days:
            checked += 1
            last = max(days)
            d = date(y, m, 1)
            while d.isoformat() <= last and d <= today:
                iso = d.isoformat()
                if d.weekday() < 5:
                    if iso not in days and iso not in hols:
                        if y not in sched_cache:
                            sched_cache[y] = fetch_twse_schedule(y) or {}
                        planned_name = sched_cache[y].get(iso)
                        name, src = ((planned_name, 'twse-schedule') if planned_name
                                     else ('休市 (無成交·FMTQIK 對帳)', 'twse-fmtqik'))
                        if apply and add_holiday(conn, iso, name, src):
                            added.append(iso)
                        elif not apply:
                            added.append(iso)
                    elif iso in days and iso in hols:
                        conflicts.append(iso)
                d += timedelta(days=1)
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    if added and apply:
        conn.commit()
        _reset_cache()
    if verbose:
        print(f'  FMTQIK 對帳 {checked} 個月:補 {added or "無"};誤列 {conflicts or "無"}')
    return {'added': added, 'conflicts': conflicts, 'months': checked}


# ── 預排假日校正:TWSE 年度開休市表 ─────────────────────────────────
def fetch_twse_schedule(year):
    """回 {iso: name} — 該年度【休市】的平日;TWSE 還沒公布回 None。
    ⚠ 表裡混了「國曆新年開始交易日」「農曆春節前最後交易日」這種【有交易】的列,要用名稱濾掉;週末也列著,不要。"""
    import requests
    try:
        r = requests.get(TWSE_SCHEDULE, params={'response': 'json', 'date': f'{year}0101'},
                         timeout=HTTP_TIMEOUT, headers={'User-Agent': 'Mozilla/5.0'})
        r.raise_for_status()
        j = r.json()
    except Exception:
        return None
    rows = j.get('data') or []
    if not rows:
        return None
    out = {}
    for row in rows:
        iso, name = str(row[0]).strip()[:10], str(row[1] if len(row) > 1 else '').strip()
        try:
            d = date.fromisoformat(iso)
        except ValueError:
            continue
        if d.year != year or d.weekday() >= 5:
            continue
        if '交易日' in name:          # 「開始交易日」「最後交易日」= 有交易
            continue
        out[iso] = name
    return out


def sync_twse_schedule(conn, year, apply=True, verbose=False):
    """用 TWSE 官方開休市表校正該年度的預排假日:
    缺的補 (source='twse-schedule');【未來】的錯列 (統一證預估錯,如 2027-10-08 中秋) 刪掉並記 suppressed。
    颱風/臨時休市不在官方表裡,不會被動到 (只刪 planned 且日期在未來的)。回 {'added','removed','year','available'}。"""
    ensure_table(conn)
    sched = fetch_twse_schedule(year)
    if sched is None:
        return {'year': year, 'available': False, 'added': [], 'removed': []}
    rows = holiday_rows(conn)
    today = date.today().isoformat()
    added, removed = [], []
    for iso, name in sched.items():
        if iso not in rows:
            if apply and add_holiday(conn, iso, name, 'twse-schedule'):
                added.append(iso)
            elif not apply:
                added.append(iso)
    for iso, (name, source) in rows.items():
        if not iso.startswith(str(year)) or iso in sched or iso <= today:
            continue
        if _is_adhoc(name, source):
            continue
        removed.append(iso)
        if apply:
            conn.execute('DELETE FROM holidays WHERE date=?', (iso,))
            conn.execute('INSERT OR REPLACE INTO holidays_suppressed(date,reason,created_at) VALUES(?,?,?)',
                         (iso, f'TWSE {year} 開休市表沒有這天 (原 {source}:{name})', _now()))
    if apply and (added or removed):
        conn.commit()
        _reset_cache()
    if verbose:
        print(f'  TWSE {year} 開休市表:補 {added or "無"};刪錯列 {removed or "無"}')
    return {'year': year, 'available': True, 'added': added, 'removed': removed}


def sync_all(conn, fmtqik_months=1, verbose=False):
    """每輪 pulse 的一鍵同步:TWSE 新聞 (颱風當晚) + FMTQIK 事後對帳 + 年度開休市表校正。回摘要 dict。"""
    out = {'news': import_from_twse_news(conn, verbose=verbose)}
    out['fmtqik'] = reconcile_fmtqik(conn, months_back=fmtqik_months, apply=True, verbose=verbose)
    y = date.today().year
    out['schedule'] = [sync_twse_schedule(conn, yy, apply=True, verbose=verbose) for yy in (y, y + 1)]
    return out


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser(description='台股交易日曆')
    ap.add_argument('--import-xlsx', action='store_true', help='從統一證 xlsx 同步假日表進 DB')
    ap.add_argument('--check', help='檢查某天是不是交易日 (YYYY-MM-DD)')
    ap.add_argument('--add', nargs='+', metavar=('DATE', 'NAME'), help='手動補一天休市:--add 2026-10-23 颱風假')
    ap.add_argument('--news', action='store_true', help='TWSE 新聞「…休市一天」→ 補颱風假')
    ap.add_argument('--fmtqik', type=int, metavar='N', help='用 TWSE 每月成交統計事後對帳近 N 個月 (0=只看本月)')
    ap.add_argument('--schedule', type=int, metavar='YEAR', help='用 TWSE 年度開休市表校正該年預排假日')
    ap.add_argument('--sync', action='store_true', help='= --news + --fmtqik 1 + --schedule 今年/明年 (pulse 每輪用)')
    ap.add_argument('--list', action='store_true', help='列出假日表 (今天起)')
    args = ap.parse_args()
    conn = sqlite3.connect(str(DB_PATH))
    ensure_table(conn)
    if args.import_xlsx:
        added, total = import_from_xlsx(conn, verbose=True)
        conn.commit()
        print(f'假日表同步: 新增 {added} / 共 {total} 筆')
    if args.add:
        iso = args.add[0]
        name = ' '.join(args.add[1:]) or '手動補登'
        date.fromisoformat(iso)
        ok = add_holiday(conn, iso, name, 'manual', overwrite_name=True)
        conn.commit()
        print(f'{iso} {name}: {"新增" if ok else "已存在 (改名/來源=manual)"}')
    if args.news or args.sync:
        a = import_from_twse_news(conn, verbose=True)
        print(f'TWSE 新聞: 新增 {a or "無"}')
    if args.fmtqik is not None or args.sync:
        r = reconcile_fmtqik(conn, months_back=(args.fmtqik if args.fmtqik is not None else 1), apply=True, verbose=True)
    if args.schedule or args.sync:
        years = [args.schedule] if args.schedule else [date.today().year, date.today().year + 1]
        for yy in years:
            r = sync_twse_schedule(conn, yy, apply=True, verbose=True)
            if not r['available']:
                print(f'  TWSE {yy} 開休市表尚未公布')
    rng = coverage()
    print(f'涵蓋範圍: {rng[0]} ~ {rng[1]}' if rng else '假日表是空的')
    print(f'涵蓋到今天+120天? {"是" if coverage_ok() else "否 — 該補表了"}')
    if args.check:
        d = date.fromisoformat(args.check)
        print(f'{d} → {"交易日" if is_trading_day(d) else "休市"}'
              f'{" (臨時休市,承銷時程回推時算營業日)" if d.isoformat() in adhoc_holidays() else ""}')
    if args.list:
        today = date.today().isoformat()
        for iso, (n, s) in sorted(holiday_rows(conn).items()):
            if iso >= today:
                print(f'  {iso}  {n:<24} {s}')
    conn.close()
