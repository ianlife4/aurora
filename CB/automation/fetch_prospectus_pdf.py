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
import tempfile
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


def download_pdf(url: str, out_path: str, sess=None) -> int:
    """串流下載 PDF, 回傳 bytes 數."""
    sess = sess or _new_session()
    headers = {'Referer': BASE + '/server-java/t57sb01'}
    r = sess.get(url, headers=headers, timeout=60, stream=True)
    r.raise_for_status()
    total = 0
    with open(out_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=64 * 1024):
            if chunk:
                f.write(chunk)
                total += len(chunk)
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
            if same:
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
                            filename: str = None) -> str:
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
    items = list_prospectuses(stock_code, sess)
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

    pdf_url = resolve_pdf_url(stock_code, pick['filename'], sess)
    out_name = os.path.basename(pdf_url)
    out_path = os.path.join(out_dir, out_name)
    if os.path.exists(out_path):
        print(f'  已存在: {out_path}')
        return out_path
    print(f'  下載: {pdf_url}')
    bytes_total = download_pdf(pdf_url, out_path, sess)
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

    fetch_latest_prospectus(
        args.stock_code, kind=args.type,
        out_dir=args.out_dir, only_status=args.status,
        filename=args.filename,
    )


if __name__ == '__main__':
    main()
