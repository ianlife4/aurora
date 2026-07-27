#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
從 Gmail 一次抓 6 家券商 CBAS 報價 xlsx + 元富初級市場 xlsx。

GAS 死透後的本機替代,涵蓋:
  - 富邦      joyce.ycy.wang@fubon.com         (週五 14:14)
  - 永豐金    cbas@sinopac.com                 (週一上午)
  - 元大      SP.derivatives.brk@yuanta.com    (週四五 16:10)
  - 統一證    PSC.CBAS@uni-psg.com             (週一 14:xx)
  - 群益      cbas888@capital.com.tw           (週一 13:38)
  - 台新元富  wnhuang@masterlink.com.tw        (元富債券部黃婉濃 - 台新元富合併)
  - 元富初級  wnhuang@masterlink.com.tw        (1150430_CB初級市場資訊.xlsx 同寄件人)

附件直接落到 cbas-template/CB報/ → 跑 build_html → push aurora 整套自動。

執行:
  py -3.12 gmail_fetch_cbas_all.py              # 最近 14 天
  py -3.12 gmail_fetch_cbas_all.py --days 30    # 最近 30 天
  py -3.12 gmail_fetch_cbas_all.py --dry-run    # 看清單不下載
  py -3.12 gmail_fetch_cbas_all.py --build      # 抓完跑 build + push aurora
"""
import argparse
import base64
import io
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE_DIR = Path(__file__).parent
CBAS_DROP_DIR = Path(r'C:\Users\J.Chun\Desktop\stock-dash\cbas-template\CB報')
TOKEN_PATH = BASE_DIR / 'token.json'
CREDS_PATH = BASE_DIR / 'credentials.json'
LOG_PATH = BASE_DIR / 'gmail_fetch_cbas_all.log'

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# 寄件人 + 主旨關鍵字 + 附件檔名過濾 — 每家
BROKER_RULES = [
    {
        'name': '富邦',
        'sender': 'joyce.ycy.wang@fubon.com',
        'subject_kw': '',  # 富邦主旨變化大,不過濾
        'filename_re': re.compile(r'富邦.*\.xlsx?$', re.I),
    },
    {
        'name': '永豐金',
        'sender': 'cbas@sinopac.com',
        'subject_kw': '',
        'filename_re': re.compile(r'永豐.*OPTION.*\.xlsx?$|永豐.*CBAS.*\.xlsx?$', re.I),
    },
    {
        'name': '元大',
        'senders': ['SP.derivatives.brk@yuanta.com', 'MaychiLiao@yuanta.com'],
        'subject_kw': '',
        'filename_re': re.compile(r'CBAS報價表.*\.xlsx?$', re.I),
    },
    {
        'name': '統一證',
        'sender': 'PSC.CBAS@uni-psg.com',
        'subject_kw': 'CBAS報價',
        'filename_re': re.compile(r'CB發行.*統一.*\.xlsx?$|統一.*CBAS.*\.xlsx?$', re.I),
    },
    {
        'name': '群益',
        'sender': 'cbas888@capital.com.tw',
        'subject_kw': '',
        'filename_re': re.compile(r'群益.*CBAS.*\.xlsx?$|群益.*選擇權.*\.xlsx?$', re.I),
    },
    {
        # 台新元富: 寄件人已換人 → joychen@tssco.com.tw (台新綜合證券 債券處 陳巧英),每週一寄,
        #   主旨「台新證券 YYYYMMDD OPTION參考報價」,附件仍是 xls option 報價表。
        #   (舊 masterlink 黃婉濃 已停寄;2026 上半年一度只見 jpg/pdf 而誤判「停用」,
        #    害檔案卡在 2026-05-04 三個月沒更新 — 2026-07-27 用戶提供 mail 截圖後修正。)
        'name': '台新元富',
        'senders': ['joychen@tssco.com.tw', 'wnhuang@masterlink.com.tw'],
        'subject_kw': '',
        'filename_re': re.compile(r'台新.*option.*\.xlsx?$|台新.*OPTION.*\.xlsx?$', re.I),
    },
    {
        # 元富初級市場: 2026 起改夾在 富邦 王鈺媜 mail,檔名變 CB初級市場資訊YYYYMMDD.xlsx
        'name': '元富初級',
        'senders': ['joyce.ycy.wang@fubon.com', 'wnhuang@masterlink.com.tw'],
        'subject_kw': '',
        'filename_re': re.compile(r'CB初級市場資訊.*\.xlsx?$|\d+_CB初級市場.*\.xlsx?$', re.I),
    },
]


def log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f'[{ts}] {msg}'
    print(line, flush=True)
    try:
        with open(LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass


def get_gmail_service():
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
    except ImportError:
        sys.exit('缺 google API 套件:py -3.12 -m pip install -r requirements.txt')

    if not TOKEN_PATH.exists():
        sys.exit('找不到 token.json — 先跑 setup_gmail.py OAuth')

    creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            TOKEN_PATH.write_text(creds.to_json())
        except Exception as e:
            sys.exit(f'token 刷新失敗 (可能過期被吊銷):{e}\n→ 跑 setup_gmail.py 重新 OAuth')
    return build('gmail', 'v1', credentials=creds)


def search_messages(service, query):
    msgs = []
    page = None
    while True:
        resp = service.users().messages().list(
            userId='me', q=query, maxResults=100, pageToken=page
        ).execute()
        msgs.extend(resp.get('messages', []))
        page = resp.get('nextPageToken')
        if not page:
            break
    return msgs


def walk_parts(payload, out):
    fname = payload.get('filename', '')
    if fname:
        att_id = payload.get('body', {}).get('attachmentId')
        size = payload.get('body', {}).get('size', 0)
        if att_id:
            out.append({
                'filename': fname, 'attachment_id': att_id, 'size': size,
                'mime': payload.get('mimeType', ''),
            })
    for part in payload.get('parts', []):
        walk_parts(part, out)


def get_attachments(service, msg_id):
    msg = service.users().messages().get(userId='me', id=msg_id, format='full').execute()
    headers = {h['name']: h['value'] for h in msg['payload'].get('headers', [])}
    atts = []
    walk_parts(msg['payload'], atts)
    return atts, headers.get('Subject', ''), headers.get('Date', '')


def download_attachment(service, msg_id, attachment_id, dest):
    att = service.users().messages().attachments().get(
        userId='me', messageId=msg_id, id=attachment_id
    ).execute()
    data = base64.urlsafe_b64decode(att['data'])
    dest.write_bytes(data)
    return len(data)


def fetch_broker(service, rule, days, dry_run):
    """單家券商抓最近 N 天,回 (new_files, skipped)"""
    senders = rule.get('senders') or [rule['sender']]
    after = (datetime.now() - timedelta(days=days)).strftime('%Y/%m/%d')

    # 合併多寄件人到一個 OR query
    sender_q = ' OR '.join(f'from:{s}' for s in senders)
    sub_q = f' subject:"{rule["subject_kw"]}"' if rule.get('subject_kw') else ''
    query = f'({sender_q}){sub_q} has:attachment after:{after}'

    msgs = search_messages(service, query)
    log(f'  {rule["name"]:8s} · {len(msgs):>3d} 封 mail · q="{query[:80]}..."')

    new_files = []
    skipped = 0
    failed = 0

    # 按 mail 日期排序(新→舊),只挑「不存在的最新 xlsx 各一」
    msgs_with_meta = []
    for m in msgs:
        try:
            atts, subject, date = get_attachments(service, m['id'])
            msgs_with_meta.append({'id': m['id'], 'atts': atts, 'subject': subject, 'date': date})
        except Exception as e:
            failed += 1
            log(f'    [ERR] {m["id"]}: {e}')
    # Gmail date 字串可能不 sortable,改用 internalDate
    msgs_with_meta.sort(key=lambda x: x['date'], reverse=True)

    for meta in msgs_with_meta:
        for a in meta['atts']:
            fname = a['filename']
            if not rule['filename_re'].search(fname):
                continue
            dest = CBAS_DROP_DIR / fname
            if dest.exists():
                if dest.stat().st_size == a['size']:
                    skipped += 1
                    continue
                # 大小不同 → 重抓
            if dry_run:
                log(f'    DRY {fname} ({a["size"]:,}b)')
                new_files.append(fname)
                continue
            try:
                size = download_attachment(service, meta['id'], a['attachment_id'], dest)
                log(f'    [OK] {fname} ({size:,}b)')
                new_files.append(fname)
            except Exception as e:
                log(f'    [ERR] {fname}: {e}')
                failed += 1

    return new_files, skipped, failed


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--days', type=int, default=14)
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--build', action='store_true', help='抓完跑 build + push aurora')
    args = p.parse_args()

    CBAS_DROP_DIR.mkdir(parents=True, exist_ok=True)
    log('=' * 60)
    log(f'CBAS Gmail Fetch (all brokers) · 最近 {args.days} 天 · {"DRY-RUN" if args.dry_run else "DOWNLOAD"}')
    log('=' * 60)
    log(f'落地目錄: {CBAS_DROP_DIR}')

    service = get_gmail_service()

    total_new = 0
    total_skipped = 0
    for rule in BROKER_RULES:
        try:
            new, skipped, failed = fetch_broker(service, rule, args.days, args.dry_run)
            total_new += len(new)
            total_skipped += skipped
        except Exception as e:
            log(f'  {rule["name"]:8s} · FETCH FAILED: {e}')

    log('-' * 60)
    log(f'總結: 新下載 {total_new} · 已存在跳過 {total_skipped}')

    if args.build and total_new > 0 and not args.dry_run:
        log('-' * 60)
        log('跑 build + push aurora')
        cbas_dir = Path(r'C:\Users\J.Chun\Desktop\stock-dash\cbas-template')
        aurora_ipo = Path(r'C:\Users\J.Chun\Desktop\stock-dash\ipo')
        # 1) build
        r1 = subprocess.run([sys.executable, 'build.py'], cwd=str(cbas_dir))
        log(f'  build.py exit={r1.returncode}')
        if r1.returncode != 0:
            return
        # 2) cp cbas_data.json → aurora
        import shutil
        shutil.copy2(str(cbas_dir / 'cbas_data.json'), str(aurora_ipo / 'CB' / 'cbas_data.json'))
        log('  cbas_data.json copied to aurora')
        # 3) git pull --rebase → add → commit → push
        def _git(*args, check=False):
            return subprocess.run(['git', '-C', str(aurora_ipo)] + list(args),
                                  capture_output=True, text=True, encoding='utf-8', errors='replace')
        _git('pull', '--rebase', '-X', 'ours', 'origin', 'main')
        _git('add', 'CB/cbas_data.json')
        cr = _git('commit', '-m', f'Update CBAS via gmail auto-sync {datetime.now():%Y-%m-%d %H:%M}')
        if 'nothing to commit' in cr.stdout + cr.stderr:
            log('  (no changes to commit)')
            return
        pr = _git('push', 'origin', 'main')
        log(f'  push: {pr.stdout.strip()[-100:] or pr.stderr.strip()[-100:]}')


if __name__ == '__main__':
    main()
