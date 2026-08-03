"""把 CB 公開說明書分析報告 (markdown 檔) 載入 issued.analysis_md。

Usage:
    python import_analysis.py <cb_code> <md_path>
    python import_analysis.py 47224 ../report/4722_CB4_analysis.md

也可在程式內 batch 載入,看 main() 範例。
"""
import os
import sys
import sqlite3
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, 'cb_data.db')


def upsert_analysis(cb_code: str, md_path: str) -> bool:
    """把 markdown 檔內容寫到 issued.analysis_md (對應 cb_code 那筆).
    回傳 True = 寫入成功; False = cb_code 不存在於 issued 表.
    """
    if not os.path.exists(md_path):
        raise FileNotFoundError(f'md 檔不存在: {md_path}')
    with open(md_path, 'r', encoding='utf-8') as f:
        md = f.read()
    if not md.strip():
        raise ValueError(f'md 檔是空的: {md_path}')

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()
    cur.execute('SELECT 1 FROM issued WHERE cb_code = ?', (cb_code,))
    if not cur.fetchone():
        conn.close()
        return False
    cur.execute(
        'UPDATE issued SET analysis_md = ?, analysis_updated_at = ? WHERE cb_code = ?',
        (md, now, cb_code),
    )
    conn.commit()
    n = cur.rowcount
    conn.close()
    return n > 0


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    cb_code = sys.argv[1]
    md_path = sys.argv[2]
    md_path_abs = md_path if os.path.isabs(md_path) else os.path.normpath(
        os.path.join(os.getcwd(), md_path)
    )

    ok = upsert_analysis(cb_code, md_path_abs)
    if ok:
        size = os.path.getsize(md_path_abs)
        print(f'OK  cb_code={cb_code}  md={md_path_abs}  ({size:,} bytes)')
    else:
        print(f'FAIL  cb_code={cb_code} 不存在於 issued 表')
        sys.exit(2)


if __name__ == '__main__':
    main()
