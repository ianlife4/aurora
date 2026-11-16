"""從 TWSE 抓 CB 公開說明書 PDF.

兩步驟:
  1) GET doc.twse.com.tw/server-java/t57sb01?step=1&mtype=B&co_id={stock}
     → 解析 readfile2(...) 拿所有公開說明書檔名
  2) POST 同 endpoint step=9 模擬 readfile2 → 回傳含實際 PDF URL 的 HTML
     → GET 該 URL 下載到 MEMO/report/

用 doc.twse.com.tw 直接打 (mopsov 只是中介,最後也 redirect 到這).

Usage (CLI):
    python fetch_prospectus_pdf.py 4931
    python fetch_prospectus_pdf.py 4931 --type B021     # 只挑某類
    python fetch_prospectus_pdf.py 4931 --all-types     # 列出全部選項

Programmatic:
    from fetch_prospectus_pdf import fetch_latest_prospectus
    pdf_path = fetch_latest_prospectus('4931', kind='B021')

檔名類型 (B 系列常見):
    B021 各類公司債(稿本)         ← CB 申報用稿本 (最常用)
    B05  各類公司債生效              ← 定價確認版
    B07  初次申請上市/櫃             ← IPO
    B011/B012 增資發行(稿本)         ← 現金增資稿本
    B04  增資發行(生效)              ← 現金增資生效
    B1c  員工認股權憑證
    B1g/B1h 限制員工權利新股
"""
import os
import sys
import re
import argparse
import glob
import tempfile
import time
import requests
import urllib3
from datetime import datetime

urllib3.disable_warnings()

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

BASE = 'https://doc.twse.com.tw'
LIST_URL = BASE + '/server-java/t57sb01'

# 預設下載目的地: 本機用 _歸檔/大資料夾/MEMO/report/;雲端 (Linux,該路徑不存在) 用系統 temp
HERE = os.path.dirname(os.path.abspath(__file__))
_WIN_OUT = r'C:\Users\J.Chun\Desktop\_歸檔\大資料夾\MEMO\report'
DEFAULT_OUT_DIR = _WIN_OUT if os.path.isdir(os.path.dirname(_WIN_OUT)) else os.path.join(tempfile.gettempdir(), 'cb_pdf')


def _new_session():
    s = requests.Session()
    s.verify = False
    s.headers.update({'User-Agent': UA, 'Accept': 'text/html,*/*'})
    return s


def list_prospectuses(stock_code: str, sess=None):
    """回傳該 stock 所有公開說明書 list, 每筆 {filename, year_month, type, status, detail, size, upload_date}.
    最新 (上傳日期 desc) 排在最後。
    """
    sess = sess or _new_session()
    r = sess.get(LIST_URL, params={
        'step': '1', 'mtype': 'B', 'colorchg': '1', 'co_id': stock_code,
    }, timeout=20)
    r.raise_for_status()
    html = r.text
    # 用 readfile2 link 抓檔名,再用同 row 解析欄位
    # 每個 <tr> 包含: 證券代號 | 資料年度 | 資料類型 | 結案類型 | 性質 | 資料細節 | 備註 | <a>檔名</a> | 大小 | 上傳日期
    rows = re.findall(
        r'<tr>(.*?)</tr>', html, re.DOTALL,
    )
    out = []
    for row in rows:
        m = re.search(r'readfile2\([^)]+,\s*["\']([^"\']+\.(?:pdf|zip))["\']\)', row)
        if not m:
            continue
        filename = m.group(1)
        # 同 row 抓所有 td 純文字
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        cells = [re.sub(r'<[^>]+>', '', c).replace('&nbsp;', '').strip() for c in cells]
        if len(cells) < 9:
            continue
        if filename.split('_')[1] != stock_code:
            continue  # 跳掉舊代號重編的記錄
        out.append({
            'filename': filename,
            'year_month': cells[1],
            'kind': cells[2],
            'status': cells[3],
            'detail': cells[5] if len(cells) > 5 else '',
            'note': cells[6] if len(cells) > 6 else '',
            'size': cells[8] if len(cells) > 8 else '',
            'upload_date': cells[9] if len(cells) > 9 else '',
            'type_code': filename.split('_')[2].split('.')[0],  # B021, B05 etc
        })
    # 依 filename 開頭的 yyyymm 排序
    out.sort(key=lambda x: x['filename'][:6])
    return out


def resolve_pdf_url(stock_code: str, filename: str, sess=None) -> str:
    """模擬 readfile2 step=9 → 回傳完整 PDF download URL."""
    sess = sess or _new_session()
    r = sess.post(LIST_URL, data={
        'colorchg': '1', 'step': '9', 'kind': 'B',
        'co_id': stock_code, 'filename': filename, 'DEBUG': '',
    }, timeout=20)
    r.raise_for_status()
    m = re.search(r"href=['\"]([^'\"]*\.pdf)['\"]", r.text)
    if not m:
        raise RuntimeError(f'解析 step=9 response 沒抓到 PDF href:\n{r.text[:500]}')
    href = m.group(1)
    if href.startswith('/'):
        href = BASE + href
    return href


def pdf_complete(path: str, want_size: int = None) -> bool:
    """PDF 檔是不是完整的:有給清單大小就比大小,否則看檔尾有沒有 %%EOF。
    (2026-11-16 全快取 1,340 份實測:沒有 %%EOF 的 36 份全是 64KB 整數倍的半截檔,0 誤判。)"""
    try:
        size = os.path.getsize(path)
        if want_size:
            return size == want_size
        if size < 1024:
            return False
        with open(path, 'rb') as f:
            f.seek(max(0, size - 4096))
            return b'%%EOF' in f.read()
    except OSError:
        return False


def _msg(e, n: int = 160) -> str:
    """例外訊息 → 可以安全印出來的一行字。
    訊息裡可能夾 doc.twse 回的整頁 HTML (多行、含目前輸出編碼印不出來的字):在 except 裡直接印,
    遇到 cp950 印不出的字會再丟 UnicodeEncodeError,後面的重試就不跑了。壓成一行、截短、印不出的字換成 ?。"""
    s = ' '.join(str(e).split())[:n]
    enc = getattr(sys.stdout, 'encoding', None) or 'utf-8'
    try:
        return s.encode(enc, 'replace').decode(enc, 'replace')
    except Exception:
        return s.encode('ascii', 'replace').decode('ascii')


def _size_int(s) -> int:
    """清單「大小」欄 '20,254,619' → 20254619;解析不出回 0。"""
    try:
        return int(str(s or '').replace(',', '').strip())
    except ValueError:
        return 0


def download_pdf(url: str, out_path: str, sess=None) -> int:
    """串流下載 PDF, 回傳 bytes 數.
    🔴 2026-11-16:先寫 .part,確認完整 (Content-Length 對得上、檔尾有 %%EOF) 才改名成正式檔名。
       以前直接寫正式檔名 → 下載到一半斷線、或被上游逾時砍掉,就留下半截 .pdf;之後「拿最新一份」
       會挑到它 → 「PDF 抽不出文字」(正基二 65462 的 20MB 檔三次下載兩次半截,7 月起一直沒有分析;
       十銓五/威剛九/科嶠三 的重跑也是這樣失敗)。不完整就丟例外,不留檔。"""
    sess = sess or _new_session()
    headers = {'Referer': BASE + '/server-java/t57sb01'}
    part = out_path + '.part'
    r = sess.get(url, headers=headers, timeout=60, stream=True)
    r.raise_for_status()
    want = 0
    if (r.headers.get('Content-Encoding') or 'identity').lower() == 'identity':
        want = _size_int(r.headers.get('Content-Length'))
    total = 0
    try:
        with open(part, 'wb') as f:
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
        if want and total != want:
            raise IOError(f'下載不完整: {total:,} / {want:,} bytes')
        if out_path.lower().endswith('.pdf') and not pdf_complete(part):
            raise IOError(f'下載不完整: {total:,} bytes,檔尾沒有 %%EOF')
        os.replace(part, out_path)
    except BaseException:
        try:
            os.remove(part)
        except OSError:
            pass
        raise
    return total


# CB 相關的公開說明書類型,由「資訊完整度」低→高排序:
#   B021 = 申報稿本 (最早,轉換價只有預估)
#   B022/B023 = 定價版 (訂價後補件,含定案轉換價) ← 同一次申報多檔 CB 時會分 CB1/CB2/CB3 版
#   B05 = 生效版 (最終)
CB_KINDS = ('B021', 'B022', 'B023', 'B05')
_ZH_NUM = {'1': '一', '2': '二', '3': '三', '4': '四', '5': '五',
           '6': '六', '7': '七', '8': '八', '9': '九'}


def _upload_key(item):
    """'115/07/22 13:59:42' → 可排序 tuple (民國年轉西元)。抓不到就退 filename yyyymm。"""
    m = re.match(r'(\d{2,3})/(\d{2})/(\d{2})\s+(\d{2}):(\d{2}):(\d{2})', item.get('upload_date') or '')
    if m:
        return (int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)),
                int(m.group(4)), int(m.group(5)), int(m.group(6)))
    fn = item.get('filename') or ''
    return (int(fn[:4] or 0), int(fn[4:6] or 0), 0, 0, 0, 0)


def pick_best_cb_prospectus(stock_code: str, cb_code: str = None, board_ym: str = None,
                            sess=None, items=None):
    """挑「這檔 CB 最新最完整」的公開說明書。

    為什麼需要這支 (2026-07-27 辛耘三 35833 血案):
      舊邏輯寫死 kind='B021' 只看申報稿本,於是:
        (a) 看不到【定價版 B022/B023】和【生效版 B05】— 而定價版才有定案轉換價
        (b) 3583 同時申報第三+四次,定價版分成「CB3定價版」「CB4定價版」,要靠 note 認次數
      規則: 先濾出「跟本案同一次申報」(board 同期 ±6 月) 的 CB 類文件,
            再從中挑【上傳時間最新】者;若 note 明講次數,只收含本檔次數的。

    Args:
        cb_code: '35833' → 第三次,用來對 note 裡的「第三次」
        board_ym: 'YYYY-MM' 董事會月份,用來排除上一檔的舊說明書
    Returns: item dict (含 filename/note/upload_date...) 或 None
    """
    items = items if items is not None else list_prospectuses(stock_code, sess)
    cands = [x for x in items if x.get('type_code') in CB_KINDS]
    if not cands:
        return None

    # 1) 只留跟本案同期的 (排掉上一檔 CB 的舊說明書)
    if board_ym:
        try:
            bd = int(board_ym[:4]) * 12 + int(board_ym[5:7])
            same = []
            for x in cands:
                fn = x.get('filename') or ''
                if len(fn) >= 6 and fn[:6].isdigit():
                    ym = int(fn[:4]) * 12 + int(fn[4:6])
                    if abs(ym - bd) <= 6:
                        same.append(x)
            # 🔴 同期一份都沒有 = 本案 B021 還沒上稿 → 直接回 None,【不可】fallback 到舊文件。
            #    舊寫法 `if same: cands = same` 在無同期檔時默默放行全部 → 抓到上一檔 CB 的說明書
            #    (2026-08-03: 24643 盟立三 board 2026-06 卻配到 202409、30362 文曄二 配到 2019 年,差 7 年)。
            #    寧可顯示「TWSE 尚未上稿」也不要拿錯的檔去分析 — 用戶明確要求過這條規則。
            if not same:
                return None
            cands = same
        except (ValueError, TypeError):
            pass

    # 2) note 有講次數的,只收本檔次數 (「國內第三次無擔保轉換公司債(CB3定價版)」)
    if cb_code:
        zh = _ZH_NUM.get(cb_code[-1], '')
        seq_digit = cb_code[-1]
        typed = [x for x in cands if re.search(r'第[一二三四五六七八九]+次|CB\d', x.get('note') or '')]
        if zh and typed:
            mine = [x for x in typed
                    if f'第{zh}次' in (x.get('note') or '') or f'CB{seq_digit}' in (x.get('note') or '')]
            # 本檔專屬版本存在 → 只從中挑;否則保留「合併申報」那種通用版
            generic = [x for x in cands if x not in typed]
            cands = (mine + generic) if mine else cands

    return max(cands, key=_upload_key) if cands else None


def fetch_latest_prospectus(stock_code: str, kind: str = 'B021',
                            out_dir: str = None, only_status: str = None,
                            filename: str = None, items: list = None) -> str:
    """主入口: 抓某 stock 指定 kind 的公開說明書 (預設最新).
    Args:
        stock_code: '4931'
        kind: 'B021' (稿本) / 'B05' (生效) / None (任何 B*)
        out_dir: 下載資料夾 (default = MEMO/report/)
        only_status: '生效' / '尚未結案' / None
        filename: 指定 doc.twse 檔名 (例如 '200606_8096_B021.pdf') — 用於歷史 CB 精確配對
    Returns:
        下載到的 PDF 完整路徑 (絕對路徑)
    """
    out_dir = out_dir or DEFAULT_OUT_DIR
    os.makedirs(out_dir, exist_ok=True)
    sess = _new_session()
    # items:呼叫端剛列過清單就直接帶進來,不要馬上再列一次 (連打會被擋、回空清單)
    list_err = None
    items_given = items is not None     # 呼叫端自己帶清單進來 (排程抓轉換價那一步)
    if items is None:
        try:
            items = list_prospectuses(stock_code, sess)
        except requests.RequestException as e:
            if not filename:
                raise
            # 指定檔名時,清單逾時/斷線跟「回空清單」一樣走下面的退避重試
            # (2026-11-16 啟碁五 62855:doc.twse 很慢,清單 20 秒逾時一次就整檔放棄)
            print(f'  清單查詢失敗 ({_msg(e)}),退避重試')
            items, list_err = [], e
    if not items and filename:
        # 🔴 2026-11-16:指定檔名 = 呼叫端剛從清單看到這份 → 空清單是 doc.twse 連打被擋 (回空頁、不報錯),
        #    不是「沒有文件」。以前直接回 NOT_FOUND,上游記成「尚未申報」→ 重跑白白失敗
        #    (中砂二 15602:檢查程式 2 秒前才列過清單,子行程再列就拿到空的)。退避重試兩次。
        for wait in (8, 16):
            time.sleep(wait)
            sess = _new_session()
            try:
                items, list_err = list_prospectuses(stock_code, sess), None
            except requests.RequestException as e:
                items, list_err = [], e
            if items:
                break
        if not items:
            why = f'最後一次錯誤: {list_err}' if list_err is not None else '連打被擋?'
            raise RuntimeError(f'{stock_code}: doc.twse 清單連續 3 次回空或失敗 ({why}),'
                               f'沒辦法下載指定的 {filename}')
    if not items:
        raise LookupError(f'{stock_code}: 找不到任何公開說明書')

    # 過濾 type — 但【指定 filename 時不套 kind 濾網】:呼叫端已經明確知道要哪一份,
    # 若還用預設 kind='B021' 濾,會把定價版 B022/B023、生效版 B05 擋掉而報「不在可用清單中」
    # (2026-07-27: 指定 202607_3583_B022.pdf 卻被 B021 濾網擋下,4 檔重跑全失敗)
    if filename:
        cands = list(items)
    else:
        cands = [x for x in items if (kind is None or x['type_code'] == kind)]
    if only_status:
        cands = [x for x in cands if x['status'] == only_status]
    if not cands:
        avail = sorted({x['type_code'] for x in items})
        raise LookupError(
            f'{stock_code}: 找不到 type={kind} status={only_status} 的檔案. 可用類型: {avail}'
        )
    # 若指定 filename → 精確配對 (一家公司多版 B021 時用)
    if filename:
        matched = [x for x in cands if x['filename'] == filename]
        if not matched:
            avail_names = [x['filename'] for x in cands]
            raise LookupError(f'{stock_code}: 指定檔名 {filename} 不在可用清單中:\n  ' + '\n  '.join(avail_names))
        pick = matched[0]
    else:
        # 取最新 (filename yyyymm desc)
        pick = max(cands, key=lambda x: x['filename'][:6])
    print(f'[{stock_code}] 選擇: {pick["filename"]}  ({pick["year_month"]}, '
          f'{pick["status"]}, {pick["detail"]}, {pick["size"]} bytes, '
          f'上傳 {pick["upload_date"]})')

    # 本機已經有同一份 (檔名前綴相同、大小跟清單逐位元組一致) → 直接用,不重抓。
    # 每次下載 doc.twse 都給新的時間戳檔名,以前同一份 20MB 的說明書每跑一次就重抓一次,
    # 慢、容易半途斷,也更容易被擋。公司重傳更正版時大小會變 → 不相等就照常重抓。
    want_size = _size_int(pick.get('size'))
    if want_size:
        prefix = pick['filename'].rsplit('.', 1)[0]
        have = sorted(p for p in glob.glob(os.path.join(out_dir, prefix + '_*.pdf'))
                      if pdf_complete(p, want_size))
        if have:
            print(f'  本機已有完整檔 ({want_size:,} bytes): {have[-1]}')
            return have[-1]

    # 清掉放超過一小時的 .part (下載中被砍留下的,每份可能十幾 MB)
    try:
        for p in glob.glob(os.path.join(out_dir, '*.pdf.part')):
            if time.time() - os.path.getmtime(p) > 3600:
                os.remove(p)
    except OSError:
        pass

    try:
        pdf_url = resolve_pdf_url(stock_code, pick['filename'], sess)
    except (requests.RequestException, RuntimeError) as e:
        # 取下載網址那一步也只有 20 秒,逾時 / 回一頁沒有連結的空頁 (被擋) 就換連線再要一次。
        # 呼叫端自己帶清單進來的不重試、照舊直接丟:那是排程抓轉換價的步驟,整步 25 檔共用 600 秒上限,
        # 一檔還會試好幾份 B05,每份多等 28 秒會把整步拖到逾時。
        if items_given:
            raise
        print(f'  取得下載網址失敗 ({_msg(e)}),8 秒後重試一次')
        time.sleep(8)
        sess = _new_session()
        pdf_url = resolve_pdf_url(stock_code, pick['filename'], sess)
    out_name = os.path.basename(pdf_url)
    out_path = os.path.join(out_dir, out_name)
    if os.path.exists(out_path) and pdf_complete(out_path, want_size or None):
        print(f'  已存在: {out_path}')
        return out_path
    print(f'  下載: {pdf_url}')
    try:
        bytes_total = download_pdf(pdf_url, out_path, sess)
    except (IOError, requests.RequestException) as e:
        # 斷在半途 → 再抓一次 (歷史上同一檔隔幾分鐘重抓幾乎都成功)。
        # 換新連線、重新要一次下載網址 (網址帶時間戳,可能綁原本的連線)
        print(f'  下載失敗 ({_msg(e)}),5 秒後重試一次')
        time.sleep(5)
        sess = _new_session()
        try:
            pdf_url = resolve_pdf_url(stock_code, pick['filename'], sess)
            out_path = os.path.join(out_dir, os.path.basename(pdf_url))
        except Exception as e2:
            print(f'  重新取得下載網址失敗 ({_msg(e2)}),沿用原網址')
        bytes_total = download_pdf(pdf_url, out_path, sess)
    if want_size and bytes_total != want_size:
        print(f'  [WARN] 下載大小 {bytes_total:,} 跟清單 {want_size:,} 不同 (檔尾完整,照用)')
    print(f'  完成: {out_path}  ({bytes_total:,} bytes)')
    return out_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('stock_code', help='股票代號 e.g. 4931')
    ap.add_argument('--type', default='B021',
                    help='類型代碼 (default: B021 各類公司債稿本; 用 B05 抓生效版)')
    ap.add_argument('--all-types', action='store_true',
                    help='只列出全部公開說明書,不下載')
    ap.add_argument('--out-dir', default=None, help='下載目錄')
    ap.add_argument('--status', default=None,
                    help='只挑某結案類型 (生效/尚未結案)')
    ap.add_argument('--filename', default=None,
                    help='精確指定 doc.twse 檔名 (例如 200606_8096_B021.pdf),用於歷史多版 B021 配對')
    args = ap.parse_args()

    if args.all_types:
        items = list_prospectuses(args.stock_code)
        print(f'找到 {len(items)} 個檔案 (依時間 asc):')
        for x in items:
            print(f'  {x["filename"]:40} {x["year_month"]:10} '
                  f'{x["status"]:6} {x["detail"]:25} {x["size"]:>14}')
        return

    try:
        fetch_latest_prospectus(
            args.stock_code, kind=args.type,
            out_dir=args.out_dir, only_status=args.status,
            filename=args.filename,
        )
    except LookupError as e:
        # 2026-09-28 宸曜一:TWSE 根本沒有這類稿本 (還沒申報) 是【正常狀態】,不是程式錯。
        # 原本直接 raise → traceback + exit 1,上游 auto_analyze 只看到非 0 就記「網路抓失敗」,
        # UI 再標成紅色「✗ 失敗」— 三層把「尚未申報」講成「故障」。
        # 改成 exit 3 = 「查詢成功但沒有此類型稿本」,與網路/解析錯誤 (exit 1) 區分。
        print(f'[NOT_FOUND] {e}')
        sys.exit(3)


if __name__ == '__main__':
    main()
