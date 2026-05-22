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

    # 過濾 type
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
