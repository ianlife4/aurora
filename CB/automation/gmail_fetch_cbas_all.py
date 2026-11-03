#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
從 Gmail 一次抓 6 家券商 CBAS 報價 xlsx + 元富初級市場 xlsx。

GAS 死透後的本機替代,涵蓋:
  - 富邦      joyce.ycy.wang@fubon.com         (週五 14:14)
  - 永豐金    cbas@sinopac.com                 (週一上午)
  - 元大      SP.derivatives.brk@yuanta.com    (週四五 16:10,金融交易部)
  - 元大債券  MaychiLiao@yuanta.com            (週一早上,債券部 廖筱琪;2026-11-03 起)
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
        # 元大【金融交易部】:主旨「元大證券金融交易部CB資產交換選擇權報價表_YYYY/MM/DD」,附件 CBAS報價表YYYYMMDD.xlsx
        'name': '元大',
        'sender': 'SP.derivatives.brk@yuanta.com',
        'subject_kw': '',
        'filename_re': re.compile(r'CBAS報價表.*\.xlsx?$', re.I),
    },
    {
        # 元大【債券部】(廖筱琪 債券三組 商品行銷科):每週一早上寄「元大證券選擇權報價單YYYYMMDD-債券部」
        #   (也出現過「…人工報價單…」「…-債券部(電話下單)」),附件「元大證選擇權報價單YYYYMMDD.xlsx」
        #   或「元大證選擇權YYYYMMDD.xlsx」。跟金融交易部是兩個部門、兩份報價 (同檔債券部百元報價貴 ~0.7、
        #   折現率高 ~0.5 點,但多報 30 幾檔) → 獨立一家「元大債券」。
        #   2026-11-03 以前她被掛在上面「元大」的寄件人裡,但檔名只認 CBAS報價表 → 一封都沒抓到。
        #   同一個寄件人還會寄「競拍結果」「CB市場資訊週報」,靠檔名過濾。
        'name': '元大債券',
        'sender': 'MaychiLiao@yuanta.com',
        'subject_kw': '',
        'filename_re': re.compile(r'^元大證選擇權.*\d{8}.*\.xlsx?$', re.I),
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


CBAS_DIR = Path(r'C:\Users\J.Chun\Desktop\stock-dash\cbas-template')


def friend_sheet_changed():
    """朋友的 Option報價表 (Google 試算表) 內容有沒有更新 — 它跟券商寄信是兩回事,沒新信的日子也可能更新。
    比的是【解析後的報價+報價日】,不是 xlsx 位元組 (每次匯出 zip 內時間戳都不同,比位元組會天天誤判)。
    🔒 唯讀:只 GET 公開匯出網址到暫存檔,比完就刪。抓不到回 False (不觸發 build)。"""
    import json as _json
    try:
        if str(CBAS_DIR) not in sys.path:
            sys.path.insert(0, str(CBAS_DIR))
        import friend_sheet as FS
        cur = CBAS_DIR / 'CB報' / '_friend_option_sheet.xlsx'
        chk = CBAS_DIR / 'CB報' / '_friend_option_sheet.check.xlsx'
        ok, msg = FS.fetch(chk)
        if not ok:
            log(f'朋友 Option報價表: 抓不到 ({msg}),略過')
            return False
        def sig(p):
            return _json.dumps({b: (r.get('date'), r.get('quotes')) for b, r in FS.load(p).items()},
                               sort_keys=True, ensure_ascii=False, default=str)
        changed = (not cur.exists()) or sig(chk) != sig(cur)
        try:
            chk.unlink()
        except OSError:
            pass
        log(f'朋友 Option報價表: {"有更新 → 要重建" if changed else "沒變"}')
        return changed
    except Exception as e:
        log(f'朋友 Option報價表 檢查失敗: {e}')
        return False


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

    # 2026-11-03:朋友的 Option報價表 (13 家) 自己會更新,沒新信的日子也要重建
    friend_new = (args.build and not args.dry_run and total_new == 0 and friend_sheet_changed())
    if args.build and (total_new > 0 or friend_new) and not args.dry_run:
        log('-' * 60)
        log('跑 build + push aurora' + (' (朋友表更新)' if friend_new and total_new == 0 else ''))
        cbas_dir = CBAS_DIR
        # 1) build
        r1 = subprocess.run([sys.executable, 'build.py'], cwd=str(cbas_dir))
        log(f'  build.py exit={r1.returncode}')
        if r1.returncode != 0:
            return
        # 2) cbas_data.json → aurora → push
        publish_aurora(cbas_dir)


AURORA_IPO = Path(r'C:\Users\J.Chun\Desktop\stock-dash\ipo')


def publish_aurora(cbas_dir=CBAS_DIR, msg=None):
    """cbas_data.json → aurora repo → push。回 True = 已在 origin 上 (或本來就沒變)。
    順序:先在乾淨的工作樹 pull → 再 copy → 只 commit 這個檔 → push (被拒就 rebase 一次再推)。
    ⚠ 2026-11-03 審查抓到兩個老問題:
      ① 舊版先 copy 再 `pull --rebase` → 工作樹有沒 commit 的改動,git 直接拒絕 pull (rebase.autostash=false),
         錯誤又沒檢查 → commit 疊在舊 base 上 → push 被拒 (cbas_daily.log 最近 15 次 12 次),
         要等 CB管理/外資研究地圖 等別的排程推 aurora 才順便上線 (晚幾分鐘到近 2 小時)。
      ② 用的是 -X ours:rebase 時 ours = upstream,衝突會留遠端版、把本機剛 build 的 JSON 靜默丟掉
         (publish_cb.py 註解實測過);改 -X theirs = 保留正在重播的本機 commit。"""
    import shutil
    aurora_ipo = AURORA_IPO

    def _git(*args):
        return subprocess.run(['git', '-C', str(aurora_ipo)] + list(args),
                              capture_output=True, text=True, encoding='utf-8', errors='replace')

    def _tail(r):
        return ((r.stderr or '') + ' ' + (r.stdout or '')).strip()[-200:]

    def _pull(tag):
        r = _git('pull', '--rebase', '--autostash', '-X', 'theirs', 'origin', 'main')
        if r.returncode != 0:
            log(f'  ⚠ {tag} pull 失敗: {_tail(r)}')
            # 卡在 rebase 一半會害到同一個 repo 的其他排程 → 退回原狀
            if (aurora_ipo / '.git' / 'rebase-merge').exists() or (aurora_ipo / '.git' / 'rebase-apply').exists():
                _git('rebase', '--abort')
                log('    (rebase 中斷,已 abort 還原)')
        return r.returncode == 0

    _pull('先')
    shutil.copy2(str(Path(cbas_dir) / 'cbas_data.json'), str(aurora_ipo / 'CB' / 'cbas_data.json'))
    log('  cbas_data.json copied to aurora')
    _git('add', 'CB/cbas_data.json')
    if _git('diff', '--cached', '--quiet', '--', 'CB/cbas_data.json').returncode == 0:
        log('  (no changes to commit)')
        return True
    # 只 commit 這一個檔 —— 工作樹可能有別的排程還沒 commit 的東西,不要順手帶走
    cr = _git('commit', '-m', msg or f'Update CBAS via gmail auto-sync {datetime.now():%Y-%m-%d %H:%M}',
              '--', 'CB/cbas_data.json')
    if cr.returncode != 0:
        log(f'  🔴 commit 失敗: {_tail(cr)}')
        return False
    pr = _git('push', 'origin', 'main')
    if pr.returncode != 0:
        log(f'  push 被拒 (origin 在這期間又被推過) → rebase 再推: {_tail(pr)}')
        if _pull('重試'):
            pr = _git('push', 'origin', 'main')
    log(f'  push {"OK" if pr.returncode == 0 else "🔴 失敗"}: {_tail(pr)}')
    return pr.returncode == 0


if __name__ == '__main__':
    main()
