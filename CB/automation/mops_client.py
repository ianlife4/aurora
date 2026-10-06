#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""mops_client.py — MOPS (公開資訊觀測站 ajax_t05st01) 共用查詢層:
回應判讀 + 全域節流閥 + 已結束月份快取。scan / rescan / milestone / conv_price 共用。

## 為什麼要這支 (2026-10-06 全市場掃描 2402s(09-22) → 4794s(10-01) → 6092s(10-04) → 撞 7200s 逾時(10-05))
實測 (4 workers × 240 家,舊版 query_company):1010 次請求裡
  - 31% 是「Overrun - 查詢過於頻繁,請稍後再試!!」— 【HTTP 200】,舊 parser 只看到 0 列 → 當成「空」
  - 53% 是「資料庫中查無需求資料」— 這家這個月【真的】沒公告,MOPS 有正常回答
  - 2% 是「該 XXXX 公開發行公司不繼續公開發行!」(已下市,stocks 表還留著)
舊重試條件 `raw == 0` 把「有回答、答案是沒有」和「被擋」混為一談:
  ① 正常的「查無」每家白重試 3 輪 — 8 月有半年報幾乎每家都有公告,10-01 換成「9+10 月」後
     大半公司兩個月都沒公告 → 一換月掃描就暴增 (這就是 4794s 那個跳點)
  ② 真的被擋只等 1~2 秒就重打,4 個 worker 繼續猛打 → 越重試越被擋 (惡性循環,越跑越慢)
  ③ 更糟的覆蓋率洞:某家 9 月有公告、10 月那次被擋 → raw>0 不重試 → 10 月公告【靜默丟失】
同一 IP 容忍度實測 (各 3 分鐘):0.8/s 0%、1.0/s 0%、1.25/s 0%、1.5/s 8%、2.3/s 17% (峰值 43%)。
舊版 4 workers × sleep 0.4 ≈ 2.3 次/秒,一直在被擋的區間裡。

## 做法
1. 判讀看【MOPS 回了什麼】:ok(有公告)/nodata(查無需求資料)/gone(不繼續公開發行) = 明確答覆,不重試;
   overrun / error(逾時、502、認不得的頁面) = 沒回答 → 重試。認不得的頁面一律當沒回答 (寧可慢,不可漏)。
2. 節流閥【全部 worker 共用】:請求間隔 ≥0.85s (≈1.2 次/秒);任一 worker 撞到 Overrun → 全體冷卻
   (10s 起跳、連續被擋加倍、上限 120s) 並把間隔拉長 30%;連續 30 次正常縮回 10% (下限 0.8s)。
   持續 6 分鐘實測 (4 workers, 0.85s):1.12 次/秒,420 次只被擋 1 次 (冷卻 10s 後恢復)。
3. 已結束月份 (月底 +2 天後) 的查詢結果是定案的 → 快取到 mops_cache.db (只存明確答覆,20 天過期重抓)。
   milestone 每 30 分對 ~43 檔各查 13 個月 (~560 次),其中 12 個月是舊月份;scan 每月上半月也要多查上個月。
"""
import collections
import json
import re
import sqlite3
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from bs4 import BeautifulSoup

HERE = Path(__file__).parent
URL = 'https://mopsov.twse.com.tw/mops/web/ajax_t05st01'
TIMEOUT = 25

# Overrun 頁原文:「Overrun - 查詢過於頻繁,請稍後再試!! Too many query requests from your ip, please wait and try again later!!」
PAT_OVERRUN = re.compile(r'Overrun\s*-\s*查詢過於頻繁|Too many query requests')
NODATA_MARK = '查無需求資料'          # 「資料庫中查無需求資料」= 這個月真的沒公告
# 「該 1729 公開發行公司不繼續公開發行!」/「該 3598 上市公司已下市!」(奕力、奈普,2026-10-06 第一晚實跑抓到) = 公司已不在
PAT_GONE = re.compile(r'不繼續公開發行|已下市|已下櫃|已終止上市|已終止上櫃')
FINAL = ('ok', 'nodata', 'gone')     # MOPS 給了明確答覆 → 不必重試

# 節流參數 (實測見檔頭):預設 ≈1.18 次/秒,最快 1.25 次/秒
GAP_DEFAULT, GAP_MIN, GAP_MAX = 0.85, 0.80, 4.0
COOL_BASE, COOL_MAX = 10.0, 120.0

CACHE_PATH = HERE / 'mops_cache.db'
CACHE_TTL_DAYS = 20      # 舊月份理論上不會變;設過期只是防萬一存到不完整的頁 (最多錯 20 天就自癒)
CLOSE_LAG_DAYS = 2       # 月底後 2 天才當「已結束」(防最後一天深夜的公告還沒進 MOPS)


class Throttle:
    """全部 worker 共用的 MOPS 節流閥。wait() 在每次請求前呼叫,report(status) 在拿到回應後呼叫。"""

    def __init__(self, gap=GAP_DEFAULT, min_gap=GAP_MIN, max_gap=GAP_MAX):
        self.gap, self.min_gap, self.max_gap = gap, min_gap, max_gap
        self._lock = threading.Lock()
        self._next = 0.0           # 下一個請求最早可發的時間 (monotonic)
        self._cool_until = 0.0     # 被擋後全體冷卻到何時
        self._cool = 0.0           # 目前這一級冷卻秒數 (連續被擋就加倍)
        self._streak = 0           # 連續明確答覆次數
        self.stats = collections.Counter()
        self.cool_total = 0.0

    def wait(self):
        while True:
            with self._lock:
                now = time.monotonic()
                t = max(self._next, self._cool_until)
                if now >= t:
                    self._next = now + self.gap
                    return
                w = t - now
            time.sleep(min(w, 1.0))

    def report(self, status):
        msg = None
        with self._lock:
            self.stats[status] += 1
            now = time.monotonic()
            if status == 'overrun':
                self._streak = 0
                # 冷卻中其他 worker 陸續回來的 Overrun 屬於【同一波】,只算一次,不重複加倍
                if now >= self._cool_until:
                    self._cool = min(max(self._cool * 2, COOL_BASE), COOL_MAX)
                    self._cool_until = now + self._cool
                    self.cool_total += self._cool
                    self.stats['冷卻'] += 1
                    old, self.gap = self.gap, min(self.gap * 1.3, self.max_gap)
                    msg = (f'  [MOPS] 被擋 Overrun (第 {self.stats["冷卻"]} 波) → 全體暫停 {self._cool:.0f}s,'
                           f'間隔 {old:.2f}→{self.gap:.2f}s')
            elif status in FINAL:
                self._streak += 1
                # 恢復要夠快:實測 1.1~1.2 次/秒持續跑,約每 300 多次才偶發一次 Overrun。
                #   第一版「100 次才縮 5%」→ 被擋幾次後間隔卡在 1.4s+ 回不來,milestone 冷快取那輪只剩 0.7 次/秒撞 600s。
                if self._streak >= 30:
                    self._streak = 0
                    self.gap = max(self.gap * 0.9, self.min_gap)
                    self._cool = self._cool / 2 if self._cool > COOL_BASE else 0.0
        if msg:
            print(msg, flush=True)       # 寫進 pulse.log,事後查得到「哪時候被擋、擋了多久」

    def summary(self):
        s = self.stats
        n = sum(s[k] for k in ('ok', 'nodata', 'gone', 'overrun', 'error'))
        return (f'請求 {n} 次 · 有公告 {s["ok"]} / 查無 {s["nodata"]} / 不繼續公開發行 {s["gone"]}'
                f' / 被擋 Overrun {s["overrun"]} (全體冷卻 {s["冷卻"]} 次共 {self.cool_total:.0f}s)'
                f' / 錯誤 {s["error"]} · 目前間隔 {self.gap:.2f}s')


def month_closed(yr_roc, month, today=None):
    """(民國年, 月) 這個月是否已結束 (月底 +CLOSE_LAG_DAYS 天)。已結束的月份公告清單不會再變。"""
    today = today or date.today()
    y = int(yr_roc) + 1911
    nxt = date(y + 1, 1, 1) if month == 12 else date(y, month + 1, 1)
    return today >= nxt + timedelta(days=CLOSE_LAG_DAYS - 1)


class MonthCache:
    """已結束月份的 MOPS 清單快取 (獨立 sqlite,不進 cb_data.db — 那份會被 publish 到 aurora)。
    多 thread / 多行程安全:單一連線 + lock + WAL;快取壞了一律當沒命中,不影響查詢本身。"""

    def __init__(self, path):
        self.path = path
        self._conn = None
        self._lock = threading.Lock()
        self.hits = 0

    def _c(self):
        if self._conn is None:
            c = sqlite3.connect(str(self.path), timeout=30, check_same_thread=False)
            c.execute('PRAGMA journal_mode=WAL')
            c.execute('''CREATE TABLE IF NOT EXISTS month_cache (
                           co_id TEXT, yr INTEGER, mo INTEGER, status TEXT, items TEXT, fetched_at TEXT,
                           PRIMARY KEY (co_id, yr, mo))''')
            c.commit()
            self._conn = c
        return self._conn

    def get(self, co_id, yr_roc, month):
        if not month_closed(yr_roc, month):
            return None                          # 當月 (還會有新公告) 永遠查即時
        try:
            with self._lock:
                row = self._c().execute(
                    'SELECT status, items, fetched_at FROM month_cache WHERE co_id=? AND yr=? AND mo=?',
                    (str(co_id), int(yr_roc), int(month))).fetchone()
            if not row:
                return None
            st, items, fetched = row
            fdt = datetime.strptime(fetched, '%Y-%m-%d %H:%M:%S')
            # 抓的當下月份必須已結束 (清單才完整),且未過期
            if not month_closed(yr_roc, month, fdt.date()) or (datetime.now() - fdt).days > CACHE_TTL_DAYS:
                return None
            self.hits += 1
            return json.loads(items), st
        except Exception:
            return None

    def put(self, co_id, yr_roc, month, items, status):
        if status not in FINAL or not month_closed(yr_roc, month):
            return
        try:
            with self._lock:
                c = self._c()
                c.execute('INSERT OR REPLACE INTO month_cache VALUES (?,?,?,?,?,?)',
                          (str(co_id), int(yr_roc), int(month), status,
                           json.dumps(items, ensure_ascii=False), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                c.commit()
        except Exception:
            pass


DEFAULT = Throttle()                 # 同一行程內所有 MOPS 請求共用 (scan 的 4 workers + 內文/訂價查詢)
CACHE = MonthCache(CACHE_PATH)


def parse_list(html, co_id):
    """ajax_t05st01 月清單 → [{date, time, title}] (與舊 fetch_mops_milestones.query_mops 同一套規則)。"""
    soup = BeautifulSoup(html, 'html.parser')
    items = []
    for tr in soup.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) < 5:
            continue
        cells = [td.get_text(' ', strip=True) for td in tds[:5]]
        code, _name, d, t, title = cells
        if str(co_id) in code and re.match(r'\d{3}/\d{2}/\d{2}', d):
            items.append({'date': d, 'time': t, 'title': title})
    return items


def classify(status_code, text, n_items):
    """MOPS 回應 → ok / nodata / gone / overrun / error。認不得的頁面一律 error (= 沒回答,要重試)。"""
    if n_items:
        return 'ok'
    if PAT_OVERRUN.search(text or ''):
        return 'overrun'
    if status_code != 200:
        return 'error'
    if NODATA_MARK in text:
        return 'nodata'
    if PAT_GONE.search(text):
        return 'gone'
    return 'error'


def fetch_month(session, co_id, yr_roc, month, throttle=None, cache=True):
    """查某公司某月重大訊息 → (items, status)。單次,不重試。"""
    th = throttle or DEFAULT
    if cache:
        hit = CACHE.get(co_id, yr_roc, month)
        if hit is not None:
            return hit
    th.wait()
    try:
        r = session.post(URL, data={
            'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1',
            'queryName': 'co_id', 'inpuType': 'co_id', 'TYPEK': 'all', 'isnew': 'false',
            'co_id': str(co_id), 'year': str(yr_roc), 'month': f'{int(month):02d}',
        }, timeout=TIMEOUT)
        r.encoding = 'utf-8'
        code, text = r.status_code, r.text
    except Exception:
        th.report('error')
        return [], 'error'
    items = parse_list(text, co_id)
    st = classify(code, text, len(items))
    # 回來的公告日期必須都在所查的月份 — 防 MOPS 哪天忽略 year/month 參數 (TWSE API 有不理 yy 參數的前例),
    #   錯月的清單若被當定案存進快取,會把那個月的真公告藏 20 天。對不上 = 認不得的回應 → error (重試、不快取)。
    if st == 'ok':
        ym = f'{int(yr_roc)}/{int(month):02d}/'
        if not all(it['date'].startswith(ym) for it in items):
            st = 'error'
    th.report(st)
    if cache:
        CACHE.put(co_id, yr_roc, month, items, st)
    return items, st


def fetch_month_retry(session, co_id, yr_roc, month, tries=4, throttle=None, cache=True):
    """fetch_month + 沒拿到明確答覆就重試。Overrun 的等待由節流閥統一處理 (全體冷卻),
    這裡只對 error (逾時/502/認不得的頁) 額外退避。回 (items, 最後狀態)。"""
    items, st = [], 'error'
    for i in range(tries):
        items, st = fetch_month(session, co_id, yr_roc, month, throttle=throttle, cache=cache)
        if st in FINAL:
            break
        if st == 'error' and i < tries - 1:
            time.sleep(2.0 * (i + 1))
    return items, st


def post(session, data, tries=3, throttle=None, **kw):
    """低階 POST ajax_t05st01 (清單/內文都可) + Overrun 偵測重試。回 response text,全部失敗回 ''。
    給 fetch_mops_conv_price 的 query_mops_list / fetch_mops_detail 用 (它們自己 parse)。"""
    th = throttle or DEFAULT
    for i in range(tries):
        th.wait()
        try:
            r = session.post(URL, data=data, timeout=TIMEOUT, **kw)
            r.encoding = 'utf-8'
        except Exception:
            th.report('error')
            if i < tries - 1:
                time.sleep(2.0 * (i + 1))
            continue
        if PAT_OVERRUN.search(r.text or ''):
            th.report('overrun')
            continue
        if r.status_code != 200:
            th.report('error')
            if i < tries - 1:
                time.sleep(2.0 * (i + 1))
            continue
        th.report('ok')
        return r.text
    return ''
