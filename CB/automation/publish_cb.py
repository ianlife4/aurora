#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
把 CB管理.html 推到 ianlife4/aurora repo (GitHub Pages)。
之後 https://ianlife4.github.io/aurora/CB/CB管理.html 會自動更新。
Worker (stock-dash) 再 proxy 過去當 dashboard 的 /cb tab。

執行：py -3.12 publish_cb.py
排程串接：self_update.py 在 build_html.py 之後呼叫本檔。
"""
import hashlib
import io
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

HERE = Path(__file__).parent
SRC = (HERE / '..' / 'CB管理.html').resolve()
AURORA_REPO = Path(r'C:\Users\J.Chun\Desktop\stock-dash\ipo')
TARGET_REL = Path('CB') / 'CB管理.html'
TARGET = AURORA_REPO / TARGET_REL
# DB sync: 本機 DB 也 push 到 aurora (cron + GHA 共用一份 canonical DB)
DB_SRC = HERE / 'cb_data.db'
DB_TARGET_REL = Path('CB') / 'automation' / 'cb_data.db'
DB_TARGET = AURORA_REPO / DB_TARGET_REL
# 個股走勢圖 per-CB 檔 (modal 懶載用) — build_html.py 產到 CB/charts/
CHARTS_SRC = (HERE / '..' / 'charts').resolve()
CHARTS_TARGET_REL = Path('CB') / 'charts'
CHARTS_TARGET = AURORA_REPO / CHARTS_TARGET_REL


def _hash(p: Path) -> str:
    if not p.exists():
        return ''
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _git(*args, check=True) -> subprocess.CompletedProcess:
    cmd = ['git', '-C', str(AURORA_REPO)] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace', check=check)


def main():
    if not SRC.exists():
        sys.exit(f'[ERR] 來源不存在：{SRC}')
    if not AURORA_REPO.exists():
        sys.exit(f'[ERR] aurora repo 不在：{AURORA_REPO}')
    if not (AURORA_REPO / '.git').exists():
        sys.exit(f'[ERR] {AURORA_REPO} 不是 git repo')

    TARGET.parent.mkdir(parents=True, exist_ok=True)

    # Autostash 任何 pending 改動,讓 pull --rebase 不會因 dirty 工作目錄 fail。
    # 典型情境: 用戶在跑 publish 前 cp _html_scaffold.html / .py / workflow 到 aurora
    # 工作目錄,以前要手動再 commit + push 一次;現在 stash → rebase → pop → 一起 commit。
    stashed = False
    st = _git('status', '--porcelain', check=False)
    if st.stdout.strip():
        sr = _git('stash', 'push', '-u', '-m', 'publish_cb autostash', check=False)
        if sr.returncode == 0 and 'No local changes' not in sr.stdout:
            stashed = True
            print('  (工作目錄 dirty → autostash 暫存)')

    # Pre-publish: 先 pull aurora 拿最新 DB (避免 GHA cron 剛 push 過的衝突)
    print('git pull origin main (拿 GHA push 過的最新 DB)...')
    pr = _git('pull', '--rebase', '-X', 'ours', 'origin', 'main', check=False)
    if pr.returncode != 0:
        print(f'  pull 失敗 (繼續): {pr.stderr.strip()[-200:]}')

    # 還原 stash → 改動回到工作目錄
    if stashed:
        sp = _git('stash', 'pop', check=False)
        if sp.returncode != 0:
            print(f'  [WARN] stash pop 衝突,需手動處理: {sp.stderr.strip()[-200:]}')
        else:
            print('  (autostash 已 pop 還原)')

    # 把 scaffold / .py / workflow 的修改一併納入這次 commit (省掉手動再 commit 一次)
    _git('add', '-u', '--', 'CB/automation', '.github/workflows', check=False)

    # 拷貝 HTML
    src_hash = _hash(SRC)
    tgt_hash = _hash(TARGET)
    html_changed = (src_hash != tgt_hash)
    if html_changed:
        print(f'拷貝 HTML: {SRC.name} -> {TARGET.relative_to(AURORA_REPO)}')
        shutil.copy2(str(SRC), str(TARGET))
    else:
        print(f'HTML 無變化,略過 (size={SRC.stat().st_size:,} bytes)')

    # === DB merge from aurora (避免覆寫雲端 cron 加的新 cb_code) ===
    # 問題:pull --rebase -X ours 對 binary DB 直接保留本機版 → GHA cron INSERT 的
    # 新案 (如 49675 十銓五 6/17) 我下次 push 就被洗掉。
    # 修法:copy 之前,從 aurora repo DB UNION 補回「本機沒的 cb_code」row 到本機 DB。
    if DB_TARGET.exists():
        try:
            import sqlite3 as _sq
            cs = _sq.connect(str(DB_SRC))
            ct = _sq.connect(str(DB_TARGET))
            local_cbs = {r[0] for r in cs.execute('SELECT cb_code FROM issued')}
            cloud_cbs = {r[0] for r in ct.execute('SELECT cb_code FROM issued')}
            missing = cloud_cbs - local_cbs
            if missing:
                print(f'  [merge] aurora 有 {len(missing)} 檔本機沒有 → 補回: {sorted(missing)[:10]}{"..." if len(missing)>10 else ""}')
                # 拿 aurora 的完整 row,INSERT 到本機 (用 sqlite_master 自動對齊欄位)
                cols = [r[1] for r in cs.execute('PRAGMA table_info(issued)').fetchall()]
                cs_cols = set(cols)
                ct_cols = {r[1] for r in ct.execute('PRAGMA table_info(issued)').fetchall()}
                shared = [c for c in cols if c in ct_cols]
                col_list = ','.join(shared)
                ph = ','.join('?' for _ in shared)
                inserted = 0
                for cb in missing:
                    row = ct.execute(f'SELECT {col_list} FROM issued WHERE cb_code=?', (cb,)).fetchone()
                    if row:
                        try:
                            cs.execute(f'INSERT INTO issued ({col_list}) VALUES ({ph})', row)
                            inserted += 1
                        except _sq.IntegrityError:
                            pass
                cs.commit()
                print(f'  [merge] {inserted} 筆 INSERT 完成')
            cs.close(); ct.close()
        except Exception as e:
            print(f'  [merge] 失敗 (繼續): {e}')

    # 拷貝 DB (本機跑完一定有更新,但檢查 hash 避免空 commit)
    db_src_hash = _hash(DB_SRC) if DB_SRC.exists() else ''
    db_tgt_hash = _hash(DB_TARGET) if DB_TARGET.exists() else ''
    db_changed = (db_src_hash != db_tgt_hash) and db_src_hash
    if db_changed:
        print(f'拷貝 DB: {DB_SRC.name} -> {DB_TARGET.relative_to(AURORA_REPO)} ({DB_SRC.stat().st_size/1024/1024:.1f} MB)')
        DB_TARGET.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(DB_SRC), str(DB_TARGET))
    else:
        print('DB 無變化,略過')

    # 同步 charts/ (per-CB 走勢圖,modal 懶載用) — always copy,交給 git status gate
    if CHARTS_SRC.exists():
        n = sum(1 for _ in CHARTS_SRC.glob('*.json'))
        print(f'同步 charts/: {n} 檔 -> {CHARTS_TARGET_REL}')
        shutil.copytree(str(CHARTS_SRC), str(CHARTS_TARGET), dirs_exist_ok=True)
        _git('add', str(CHARTS_TARGET_REL).replace('\\', '/'))

    # git add HTML + DB
    if html_changed:
        _git('add', str(TARGET_REL).replace('\\', '/'))
        print(f'  git add {TARGET_REL}')
    if db_changed:
        _git('add', str(DB_TARGET_REL).replace('\\', '/'))
        print(f'  git add {DB_TARGET_REL}')

    # git status 檢查 — 唯一的 push gate (HTML/DB/charts 任一有變就推)
    s = _git('status', '--porcelain', check=False)
    if not s.stdout.strip():
        print('git 沒看到變更（HTML/DB/charts 都沒變）')
        return

    msg = f'Auto-update CB 管理 {datetime.now().strftime("%Y-%m-%d %H:%M")}'
    print(f'git commit -m "{msg}"')
    cr = _git('commit', '-m', msg, check=False)
    if cr.returncode != 0:
        print(f'commit 失敗 (可能沒變更)：{cr.stdout}\n{cr.stderr}')
        return

    # 主動 fetch + rebase 預防 push 第一次就被 reject (5/11 mops_daily 0.8s 失敗就是這種情況)
    print('git fetch origin main (預先同步遠端 commit)...')
    fr = _git('fetch', 'origin', 'main', check=False)
    if fr.returncode == 0:
        rr = _git('pull', '--rebase', '-X', 'ours', 'origin', 'main', check=False)
        if rr.returncode != 0:
            print(f'  pre-push rebase 失敗 (繼續嘗試直 push): {rr.stderr.strip()[-200:]}')
    else:
        print(f'  fetch 失敗 (繼續嘗試直 push): {fr.stderr.strip()[-200:]}')

    # push (retry 3 次, 衝突時 rebase)
    for i in range(1, 4):
        print(f'git push (attempt {i})...')
        pr = _git('push', 'origin', 'main', check=False)
        if pr.returncode == 0:
            print(f'[OK] 推上去：https://github.com/ianlife4/aurora/blob/main/{TARGET_REL.as_posix()}')
            print(f'     公開網址：https://ianlife4.github.io/aurora/{TARGET_REL.as_posix()}')
            return
        print(f'  push 失敗：{pr.stderr.strip()[-200:]}')
        # rebase 重試
        rr = _git('pull', '--rebase', '-X', 'ours', 'origin', 'main', check=False)
        if rr.returncode != 0:
            print(f'  rebase 也失敗：{rr.stderr.strip()[-200:]}')
        time.sleep(2)

    sys.exit('[ERR] push 重試 3 次仍失敗，請手動處理 aurora repo')


if __name__ == '__main__':
    main()
