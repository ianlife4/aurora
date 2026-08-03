#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""auto_analyze_cb.py — 用 Claude API 自動產生 CB 公開說明書分析 .md

完整流程 (對每檔 CB):
  1. 從 DB 拿 CB 條件 (cb_code, stock_code, eff_date, board, account, listing 等)
  2. fetch_prospectus_pdf: 從 doc.twse.com.tw 抓 B021 公開說明書 PDF
  3. pdfplumber 抽 PDF 全文 (可能 200-400 頁,~50K-100K tokens)
  4. scan_prospectus_signals: 掃警示訊號 (提前拉貨/缺料/存貨/拉貨潮回落 等)
  5. 呼叫 Claude API (model=claude-opus-4-7, adaptive thinking)
     - System prompt: 七大段落寫作指示 (cached)
     - User msg 1: 4931_CB1 樣板分析 (cached, 重用降低成本)
     - User msg 2: 當前 CB 條件 + 警示結果 + PDF 全文 (不快取)
     - Streaming output (avoid SDK HTTP timeout)
  6. 寫 .md 到 MEMO/report/<stock>_CB<N>_analysis.md
  7. import_analysis.py 載入 DB analysis_md 欄位

排程:mops_daily.py Step 2.5 / self_update.py Step 5.5 自動跑

執行:
  py -3.12 auto_analyze_cb.py --cb 80961               # 單檔
  py -3.12 auto_analyze_cb.py --all                    # 所有 analysis_md IS NULL 的
  py -3.12 auto_analyze_cb.py --limit 3                # 限制筆數 (cost guard)
  py -3.12 auto_analyze_cb.py --cb 80961 --dry-run     # 不打 API,只顯示 prompt 大小
  py -3.12 auto_analyze_cb.py --cb 80961 --model claude-sonnet-4-6  # 改用 Sonnet (省錢)
"""
import argparse
import io
import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from glob import glob
from pathlib import Path

import anthropic
import pdfplumber

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE = Path(__file__).parent
DB_PATH = BASE / 'cb_data.db'
TOKEN_PATH = BASE / 'anthropic_token.txt'
REPORT_DIR = Path(r'C:\Users\J.Chun\Desktop\_歸檔\大資料夾\MEMO\report')
LOG_PATH = BASE / 'auto_analyze.log'
SAMPLE_ANALYSIS = REPORT_DIR / '4931_CB1_analysis.md'  # 樣板 (cached)

DEFAULT_MODEL = 'claude-opus-4-8'
MAX_TOKENS_OUTPUT = 16000  # 大約一份完整 .md 分析 (~300 行)
PDF_MAX_CHARS = 120000     # PDF 抽出文字上限 (~30-40K tokens) 避免超出常理


def log(msg: str):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f'[{ts}] {msg}'
    print(line)
    try:
        with open(LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass


# ── DB helpers ───────────────────────────────────────────────────────────────

def get_targets(cb_only: str | None, force_all: bool, limit: int | None) -> list[dict]:
    """挑要分析的 CB。
    預設條件: analysis_md IS NULL AND fm_board_decision_date IS NOT NULL
              (有 board 才能找到 B021,沒 board 的可能還太早)
    --all: 全掃 (含已有 analysis_md 的會 SKIP,除非 --force 之類)
    --cb: 指定一檔
    SELECT 含 prospectus_filename → 指定 doc.twse 檔名 (歷史多版 B021 用)"""
    # conv_price / fm_conv_price_set_date 一定要撈:公開說明書裡的轉換價是【申報時預估】,
    # 實際訂價是圈購/競拍後才定案 (常差很多),prompt 要拿 DB 實際值蓋掉 PDF 的預估值。
    # (2026-07-27 52912 邑昇二: 說明書寫 68.87,實際訂價 53.0 → 分析報告抄錯)
    cols = '''cb_code, stock_code, company, eff_date, listing_date, method,
              fm_board_decision_date, fm_account_setup_date, fm_eff_close_date,
              conv_price, fm_conv_price_set_date,
              analysis_md, is_legacy, prospectus_filename'''
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    if cb_only:
        rows = conn.execute(f'SELECT {cols} FROM issued WHERE cb_code = ?', (cb_only,)).fetchall()
    else:
        if force_all:
            rows = conn.execute(f'''
                SELECT {cols} FROM issued
                WHERE stock_code IS NOT NULL AND stock_code != ''
                  AND fm_board_decision_date IS NOT NULL
                ORDER BY fm_board_decision_date DESC
            ''').fetchall()
        else:
            # 只做「進行中(未上市 or 近 30 天剛掛牌)」的在途案 — 用戶做投標決策會看的就這些。
            # 🔴 一定要限定,否則 --無上限清 backlog 會連幾百檔老已上市案也分析 → API 成本爆炸。
            rows = conn.execute(f'''
                SELECT {cols} FROM issued
                WHERE stock_code IS NOT NULL AND stock_code != ''
                  AND fm_board_decision_date IS NOT NULL
                  AND (analysis_md IS NULL OR analysis_md = '')
                  AND (is_legacy IS NULL OR is_legacy != 1)
                  AND (is_withdrawn IS NULL OR is_withdrawn != 1)
                  AND (listing_date IS NULL OR listing_date='' OR listing_date='未定'
                       OR substr(listing_date,1,10) >= date('now','-30 days'))
                ORDER BY fm_board_decision_date DESC
            ''').fetchall()
    conn.close()
    out = [dict(r) for r in rows]
    if limit:
        out = out[:limit]
    return out


# ── PDF 取得 ──────────────────────────────────────────────────────────────────

def find_b021_pdf(stock_code: str, prospectus_filename: str | None = None) -> str | None:
    """在 MEMO/report 下找 stock_code 的 B021 PDF。
    若 prospectus_filename 指定 (例如 '200606_8096_B021.pdf'),會 glob 該前綴。
    沒指定就抓最新一份。"""
    if not REPORT_DIR.exists():
        return None
    if prospectus_filename:
        # 例如 200606_8096_B021.pdf → 找 200606_8096_B021_*.pdf
        prefix = prospectus_filename.replace('.pdf', '')
        candidates = sorted(glob(str(REPORT_DIR / f'{prefix}_*.pdf')))
        if candidates:
            return candidates[-1]
        return None  # 指定檔名沒找到 → 不要 fallback,讓 fetch_b021_pdf 去抓
    candidates = sorted(glob(str(REPORT_DIR / f'*_{stock_code}_B021_*.pdf')))
    return candidates[-1] if candidates else None


def best_prospectus_name(stock_code: str, cb_code: str, board_ym: str | None) -> str | None:
    """線上挑「這檔 CB 最新最完整」的說明書檔名 (B021稿本/B022,B023定價版/B05生效版)。
    只打一次 doc.twse 清單頁 (便宜、無 API 成本) → 拿來比對 DB 記錄的版本是否過期。"""
    try:
        import fetch_prospectus_pdf as fpp
        pick = fpp.pick_best_cb_prospectus(stock_code, cb_code=cb_code, board_ym=board_ym)
        return pick['filename'] if pick else None
    except Exception as e:
        log(f'    [WARN] 挑最佳說明書失敗: {e}')
        return None


def fetch_b021_pdf(stock_code: str, prospectus_filename: str | None = None) -> str | None:
    """呼叫 fetch_prospectus_pdf.py 抓 PDF。若 prospectus_filename 指定,精確配對。"""
    cmd = [sys.executable, 'fetch_prospectus_pdf.py', stock_code]
    if prospectus_filename:
        cmd += ['--filename', prospectus_filename]
        log(f'  → 抓 B021 PDF for {stock_code} (指定 {prospectus_filename})...')
    else:
        log(f'  → 抓 B021 PDF for {stock_code} (最新)...')
    try:
        result = subprocess.run(
            cmd, cwd=str(BASE),
            capture_output=True, text=True,
            encoding='utf-8', errors='replace',
            env={**os.environ, 'PYTHONIOENCODING': 'utf-8'},
            timeout=120,
        )
        if result.returncode != 0:
            log(f'    [ERR] fetch_prospectus_pdf exit={result.returncode}: {(result.stderr or "")[:200]}')
            return None
        # 抓完後再 glob 找
        return find_b021_pdf(stock_code, prospectus_filename)
    except Exception as e:
        log(f'    [ERR] fetch_prospectus_pdf: {e}')
        return None


def extract_pdf_text(pdf_path: str, max_chars: int = PDF_MAX_CHARS) -> str:
    """pdfplumber 抽全文字,限制 max_chars (避免 input 太大)。"""
    pages_text = []
    total = 0
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                txt = page.extract_text() or ''
                pages_text.append(f'--- Page {i+1} ---\n{txt}')
                total += len(txt)
                if total >= max_chars:
                    pages_text.append(f'\n[...PDF truncated at ~{max_chars} chars to control input size...]')
                    break
    except Exception as e:
        log(f'    [ERR] pdf extract: {e}')
        return ''
    return '\n'.join(pages_text)


# ── 警示掃描整合 ────────────────────────────────────────────────────────────

def run_signal_scan(pdf_path: str) -> str:
    """呼叫 scan_prospectus_signals.py 對單一 PDF 掃訊號,回傳純文字報告。"""
    try:
        result = subprocess.run(
            [sys.executable, 'scan_prospectus_signals.py', pdf_path],
            cwd=str(BASE),
            capture_output=True, text=True,
            encoding='utf-8', errors='replace',
            env={**os.environ, 'PYTHONIOENCODING': 'utf-8'},
            timeout=120,
        )
        return (result.stdout or '') + ('\n[scan stderr]\n' + result.stderr if result.stderr else '')
    except Exception as e:
        return f'[scan failed: {e}]'


# ── Claude API ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """你是專業的台股可轉換公司債 (CB) 投資分析師,負責閱讀公開說明書 (B021 稿本 / B022,B023 定價版 / B05 生效版) 並撰寫深度分析報告。

**報告的用途是「決策」不是「研究」**:讀者是要判斷這檔 CB 值不值得參與的投資人,
所以【結論與定位放最前面】,細節論證放後面。嚴格遵循以下段落結構 (順序固定):

# <公司名 (股票代號)> — <CB 序號中文,例: 第一次> 無擔保轉換公司債 分析

> 📄 來源:[<PDF 檔名>](<PDF 檔名>)
> 📅 公開說明書編印日:<民國日期>
> 🏛 主辦券商:<從 PDF 抓>
> 🏦 受託機構:<從 PDF 抓>
> 📝 董事會決議日:<DB 提供的 fm_board_decision_date,轉民國>
> ✅ 申報生效日:<DB 提供的 eff_date,轉民國>
> 📌 確定代收/存儲專戶:<DB 提供的 fm_account_setup_date,轉民國>

---

## 🎯 一眼判讀

**<GO / CAUTION / NO-GO>** — <一句話定位這家公司在做什麼 + 這筆錢要幹嘛。例:
「半導體設備商 + 晶圓再生廠,吃 AI 先進封裝資本支出。募 20.8 億全數買料備產能,不是還債。」>

| | 內容 |
|---|---|
| ✅ 最強看多理由 | <一條,要有數字> |
| ⚠️ 最強看空理由 | <一條,要有數字> |
| 👀 接下來要盯什麼 | <2-3 個具體可觀察的指標/事件> |

| 維度 | 評估 | 標籤 |
|---|---|:---:|
| 條件吸引力 | | 🟢/🟡/🔴 |
| 公司基本面 | | 🟢/🟡/🔴 |
| 警示訊號 | | 🟢/🟡/🔴 |
| 時點 | | 🟢/🟡/🔴 |
| 同業比較 | | 🟢/🟡/🔴 |

---

## 📋 發行條件總覽

| 項目 | 內容 |
|---|---|
| 發行種類 | |
| 發行總面額 | |
| 發行價格 | |
| 票面利率 | |
| 發行期間 | |
| 承銷方式 | |
| 轉換價 | |
| 信用評等 | |
| 承銷費用 | |

---

## 💰 發債動機分類

**類型:<從下列【五選一】,必須原字照用,不可自創>**
`純擴產備料型` / `純還債型` / `財務體質修復型` / `併購型` / `混合型`

判定規則 (依資金用途占比 + 借款成本):
- 購料/營運資金 ≥70% 且幾乎不還債 → 純擴產備料型
- 償還借款 ≥70% → 純還債型
- 還債為主 + 公司自承負債比/速動比惡化 + 募後財務比率大幅改善 → 財務體質修復型
- 主要用於取得股權/資產 → 併購型
- 都不占 70% → 混合型

| 資金用途 | 金額 | 占比 |
|---|---:|---:|
| <細項,從「壹、二、(八) 必要性合理性」段抓> | | |

**判定理由**: <為什麼歸這類。務必提到【借款利率水準】— 利率低卻大舉還債 vs 利率高才還債,
意義完全不同;錢拿去買料通常代表在押注需求,拿去還債通常代表財務壓力。>

---

## 🏭 產業與應用地圖

| 產品線 | 終端應用 | 營收占比 | 成長驅動 / 風險 |
|---|---|---:|---|
| <產品線名> | <終端應用,點出是不是 AI/車用/低軌衛星等熱門題材> | <%,從「營業概況」段抓;抓不到寫 n/a> | <驅動力,或該產品線的削價/產能過剩風險> |

**一句話總結產業位置**: <這家在產業鏈的哪一環、賣給誰、景氣看什麼指標>

---

## ⚖️ 同業對照

(公開說明書多半會列同業財務比較表 — 這是法定揭露,務必找出來抓。
 常見於「財務狀況及經營結果之檢討分析」或「必要性合理性」段,公司會自己列同業負債比/毛利率。)

| 項目 | 本公司 | 同業 A | 同業 B | 解讀 |
|---|---:|---:|---:|---|
| 負債比率 | | | | |
| 毛利率 | | | | |
| <其他公司有列的比較項目> | | | | |

**解讀**: <槓桿/獲利在同業中偏高或偏低?公司主動列出比較表本身就是一種訊號 —
若列出「我負債比比同業高」,等於自承槓桿壓力。若 PDF 沒有同業比較表,直接寫
「PDF 未揭露同業比較表」,不可自行編造同業數字。>

---

## 🚨 警示訊號 — 提前拉貨 / 缺料 / 存貨控管風險

> 一句話總結 (例:「🔴 這檔的訊號比一般 CB 更強烈:公司自己在風險事項段直接揭露『存貨控管風險增加』」)

### A. 直接從 PDF 抓到的關鍵語句 (重大警示)

| 訊號類型 | 公司原文 (公開說明書揭露) | 嚴重度 |
|---|---|:---:|
| <類型> | 「<原文引用 (一定要逐字)>」 | 🔴 高 / 🟡 中 / 🟢 低 |

### B. 為什麼能從 CB 公開說明書「測出」這些訊號

(列點解釋法定揭露機制 / 資金用途 / 流動比 vs 速動比 / 同業比對 / 客戶買回契約 / 上游引述)

### C. 解讀為「警示」的理由

(列點分析,為什麼這些訊號值得擔心)

### D. 對照觀察點 (追蹤指標)

| 指標 | 何時警示 |
|---|---|

### E. 樂觀情境 (為什麼還是值得布局)

(列點分析,為什麼即便有警示仍有 upside)

→ 結語一句話 (例:「邊看 AI 拉貨節奏邊持有」)

---

## ⏰ 為何是現在發

(分析時點:景氣循環 / 預期股價 / 利率環境 / 同業募資潮。
 若 DB 給了實際轉換價與訂價日,要點出「轉換價訂在股價的什麼位置」— 高檔訂價 vs 低檔訂價
 對後續轉換誘因差很多。)

## 🏢 公司展望補充

### (一) 產品結構 / 客戶結構 (產業應用地圖已列的不要重複,這裡補客戶集中度、大客戶占比)
### (二) TAM / CAGR
### (三) 主要成長引擎
### (四) 競爭格局
### (五) 風險事項 (從 PDF 「壹、二、風險事項」段)

## 💼 投資人解讀

(從散戶角度看這檔 CB:轉換價合不合理 / 套利空間 / 期間流動性 / 強制贖回觸發 / 賣回權設計)

## 🕰 前次 CB 執行追蹤 (如有同公司歷史 CB)

(如果 DB 有同公司前面的 CB,列出歷史結果作參考;首檔則寫「此為首檔 CB」)

---

寫作守則:
1. 引用 PDF 原文時必須**逐字精確**,放在引號內,不可改寫
2. 數字 (金額/比例/天數) 必須跟 PDF 一致,不可估算
3. 警示掃描提供的訊號要**全部列入** A 段表格 + 標註頁碼
4. 範本只供參考【語氣、表格密度、引文深度、論證細膩度】—— **段落結構一律以本 system prompt
   為準**。範本是舊版格式 (警示訊號在最前面、摘要在最後),**不要照它的順序**。
5. 不確定的事項要明確標 (不可推測),例如「PDF 未揭露,有待補件」。
   **同業數字、營收占比若 PDF 沒揭露就寫 n/a 或「PDF 未揭露」,絕對不可自行編造或用常識填補。**
6. 「一眼判讀」是全篇最重要的段落 — 要讓人只看它就能做決定;不可只是抄後面段落的句子,
   要真的做取捨判斷 (最強看多/看空各只准一條)
7. 整篇控制在 280-400 行 Markdown,避免冗長
8. 不要加任何 ```markdown 程式碼框 — 直接輸出 Markdown 原文
"""


def call_claude(client, model: str, sample_md: str, target_user_msg: str, max_tokens: int):
    """呼叫 Claude API 產 .md,用 streaming + prompt caching。

    cache 策略:
      - system prompt: 預設由 cache_control 自動快取最後一個 cacheable block
      - 樣板分析 (user msg 1): 顯式 cache_control,跨 CB 重用
      - 當前 CB context (user msg 2): NOT cached (每檔不同)
    """
    # 第一個 user msg = 樣板 (cached)
    sample_block = {
        "type": "text",
        # ⚠ 範本是【舊版格式】(警示訊號在最前、摘要在最後)。2026-07-30 改版後段落順序改由
        #   system prompt 決定,所以這裡只能叫模型學「語氣/表格密度/引文深度」,
        #   絕不能再說「按完全相同的格式輸出」— 否則舊順序會蓋掉新結構。
        "text": f"以下是『新盛力 (4931) 第一次 CB』的分析範本。**只參考它的語氣、表格密度、引文深度、論證細膩度** — "
                f"它的段落順序是舊版,已不適用;段落結構請一律依照 system prompt 的規定。\n\n"
                f"=== 範本開始 ===\n{sample_md}\n=== 範本結束 ===",
        "cache_control": {"type": "ephemeral"}  # 5-min TTL,跨 CB 重用
    }
    target_block = {
        "type": "text",
        "text": target_user_msg
    }
    messages = [{"role": "user", "content": [sample_block, target_block]}]

    log(f'  → 呼叫 Claude API: model={model}, max_tokens={max_tokens}')

    full_text_parts = []
    cache_read = 0
    cache_create = 0
    input_tokens = 0
    output_tokens = 0
    with client.messages.stream(
        model=model,
        max_tokens=max_tokens,
        system=SYSTEM_PROMPT,
        thinking={"type": "adaptive"},
        messages=messages,
    ) as stream:
        for text in stream.text_stream:
            full_text_parts.append(text)
        final = stream.get_final_message()
        usage = final.usage
        cache_read = getattr(usage, 'cache_read_input_tokens', 0) or 0
        cache_create = getattr(usage, 'cache_creation_input_tokens', 0) or 0
        input_tokens = getattr(usage, 'input_tokens', 0) or 0
        output_tokens = getattr(usage, 'output_tokens', 0) or 0

    log(f'  → tokens: in={input_tokens} cache_read={cache_read} cache_create={cache_create} out={output_tokens}')
    return ''.join(full_text_parts)


# ── Main 處理單檔 ────────────────────────────────────────────────────────

def cb_seq_chinese(cb_code: str) -> str:
    """80961 → '一', 80962 → '二', ..."""
    zh = {'1':'一','2':'二','3':'三','4':'四','5':'五','6':'六','7':'七','8':'八','9':'九'}
    return zh.get(cb_code[-1], cb_code[-1])


def build_target_msg(target: dict, pdf_text: str, signal_report: str) -> str:
    """組第二個 user message 給 Claude。"""
    db_info = f"""DB 內已知條件:
- CB 代號 (cb_code): {target['cb_code']}
- 公司: {target['company']}
- 股票代號: {target['stock_code']}
- 本檔次數: 第{cb_seq_chinese(target['cb_code'])}次
- 方式: {target.get('method') or '(未定)'}
- 董事會決議日 (fm_board_decision_date): {target.get('fm_board_decision_date') or '(未抓到)'}
- 申報生效日 (eff_date): {target.get('eff_date') or '(未生效)'}
- 確定專戶日 (fm_account_setup_date): {target.get('fm_account_setup_date') or '(未確定)'}
- 掛牌日 (listing_date): {target.get('listing_date') or '(未掛牌)'}
- **實際轉換價 (conv_price)**: {target.get('conv_price') if target.get('conv_price') is not None else '(尚未訂價)'}{'  ← 訂價日 ' + str(target.get('fm_conv_price_set_date'))[:10] if target.get('fm_conv_price_set_date') else ''}

🔴 **轉換價以上面 DB 的「實際轉換價」為準,不要用公開說明書裡的數字。**
公開說明書是【申報時】編印的,裡面的轉換價/溢價率只是**當時的預估值**;實際轉換價是後來
(詢圈圈購結束後 / 競拍訂價日) 才依訂價基準日前幾個交易日收盤價定案,兩者常差很多。
若 DB 顯示「尚未訂價」,才可引用說明書的預估價,但**必須註明是預估**。
"""
    return f"""現在請分析以下這檔 CB,完全按照剛才範本的結構/深度/表格密度,輸出七大段落 Markdown:

{db_info}

警示掃描結果 (scan_prospectus_signals.py 機器抓的訊號 — 你的 A 段必須**全部納入**這些訊號,並標頁碼):

{signal_report}

公開說明書 B021 原文 (pdfplumber 抽出,可能含表格錯位):

{pdf_text}

請直接開始輸出 Markdown (不要 ``` 包起來,不要 preamble,從第一行 H1 標題開始)。"""


def process_one(target: dict, client, model: str, dry_run: bool = False, force: bool = False) -> bool:
    """處理一檔 CB。回傳 True if .md 寫成功。"""
    cb = target['cb_code']
    stock = target['stock_code']
    log(f'')
    log(f'═══ {cb} {target["company"]} (stock {stock}) ═══')

    # --force:網頁「🔄 強制更新」走這條 — 已有分析也要重跑 (換最新版說明書 / 新版報告格式)。
    # 沒有 force 時維持 SKIP,避免每日排程重複燒 API。
    if target.get('analysis_md') and not force:
        log(f'  已有 analysis_md → SKIP (要重跑請加 --force)')
        return False
    if target.get('analysis_md') and force:
        log(f'  已有 analysis_md,但 --force → 重新分析 (將覆蓋舊報告)')

    # 1. 抓 / 找 PDF — 優先網路抓最新 (TWSE 可能剛上新版),失敗才退本機 cache
    # 為什麼網路優先: 用戶要「之後上傳隨時補」,若先吃本機 stale cache (如 6187 萬潤六本機留著
    # 202406 萬潤五的 PDF) 就永遠看不到 TWSE 新上的 202605 → 驗證會把舊的擋下,但永遠補不到新的
    prospectus_filename = target.get('prospectus_filename')
    if not prospectus_filename:
        # 沒指定 → 線上挑最新最完整版 (含定價版 B022/B023、生效版 B05,不只 B021 稿本)
        # 2026-07-27: 35833 辛耘三 用 202405 (2024 年、上一檔 CB 的) 稿本做分析,
        #   而 doc.twse 上早有 202607_B022「CB3定價版」→ 只認 B021 就永遠看不到。
        board_ym = (target.get('fm_board_decision_date') or '')[:7]
        best = best_prospectus_name(stock, cb, board_ym or None)
        if best:
            log(f'  → 線上最佳版本: {best}')
            prospectus_filename = best
    pdf = fetch_b021_pdf(stock, prospectus_filename)
    if not pdf:
        log(f'  網路抓失敗,試本機既有 cache...')
        pdf = find_b021_pdf(stock, prospectus_filename)
    if not pdf:
        log(f'  [SKIP] B021 PDF 不存在 (TWSE 可能還沒上稿)')
        return False
    log(f'  PDF: {Path(pdf).name}')

    # PDF 對應驗證: 沒指定 prospectus_filename 時, picked PDF 的 yyyymm 必須跟 board 同年 (差 ≤6 月)
    # 否則就是抓到「上一檔」的舊 B021 (本案還沒上),依規則「寧可空白不可硬拉前一檔」直接 skip
    # (用戶 2026-05-24 看到 80963 顯示 80961 第一次分析,明確說「請記住」這條規則)
    if not prospectus_filename:
        import re as _re
        m = _re.match(r'(\d{4})(\d{2})_', Path(pdf).name)
        board = (target.get('fm_board_decision_date') or '')[:10]
        if m and board:
            try:
                pdf_ym = int(m.group(1)) * 12 + int(m.group(2))
                bd_ym = int(board[:4]) * 12 + int(board[5:7])
                diff = abs(pdf_ym - bd_ym)
                if diff > 6:
                    log(f'  [SKIP] PDF {m.group(1)}/{m.group(2)} vs board {board[:7]} 差 {diff} 月 (>6) — 本案 B021 還沒上,不 fallback 舊版,等之後上傳再補')
                    return False
            except ValueError:
                pass

    # 2. 掃訊號
    log(f'  → 跑 scan_prospectus_signals')
    signal_report = run_signal_scan(pdf)
    log(f'    signal report: {len(signal_report)} chars')

    # 3. 抽 PDF 全文
    log(f'  → pdfplumber 抽 PDF...')
    t0 = time.time()
    pdf_text = extract_pdf_text(pdf)
    log(f'    {len(pdf_text)} chars in {time.time()-t0:.1f}s')
    if not pdf_text:
        log(f'  [SKIP] PDF 抽不出文字')
        return False

    # 4. 樣板
    sample_md = SAMPLE_ANALYSIS.read_text(encoding='utf-8') if SAMPLE_ANALYSIS.exists() else '(無樣板)'
    if not SAMPLE_ANALYSIS.exists():
        log(f'  [WARN] 樣板 {SAMPLE_ANALYSIS.name} 不存在,品質可能下降')

    # 5. 組 message + 呼叫 Claude
    target_msg = build_target_msg(target, pdf_text, signal_report)
    log(f'  → user msg 1 (樣板): {len(sample_md)} chars  / user msg 2 (current): {len(target_msg)} chars')

    if dry_run:
        log(f'  [DRY-RUN] 不打 API')
        return False

    try:
        t0 = time.time()
        md = call_claude(client, model, sample_md, target_msg, MAX_TOKENS_OUTPUT)
        log(f'  ✓ 生成完成 ({time.time()-t0:.0f}s,{len(md)} chars)')
    except anthropic.APIStatusError as e:
        log(f'  [ERR] Claude API: status={e.status_code} type={getattr(e,"type","?")} msg={e.message[:200]}')
        return False
    except Exception as e:
        log(f'  [ERR] Claude call: {e}')
        return False

    # 6. 寫 .md
    seq_zh = cb_seq_chinese(cb)
    out_name = f'{stock}_CB{cb[-1]}_analysis.md'
    out_path = REPORT_DIR / out_name
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md, encoding='utf-8')
    log(f'  → 寫 {out_path}')

    # 7. import 到 DB
    log(f'  → import_analysis.py {cb} {out_path}')
    try:
        result = subprocess.run(
            [sys.executable, 'import_analysis.py', cb, str(out_path)],
            cwd=str(BASE),
            capture_output=True, text=True,
            encoding='utf-8', errors='replace',
            env={**os.environ, 'PYTHONIOENCODING': 'utf-8'},
            timeout=30,
        )
        if result.returncode != 0:
            log(f'    [ERR] import_analysis exit={result.returncode}: {(result.stderr or "")[:200]}')
        else:
            log(f'    ✓ imported')
    except Exception as e:
        log(f'    [ERR] import_analysis: {e}')

    # 8. 記下「這份分析用了哪個版本的說明書」— 之後 check_prospectus_freshness.py
    #    只要比對它 vs 線上最新版,就知道要不要重跑 (便宜:一次 HTTP,不用打 API)
    used = prospectus_filename
    if not used:
        m = re.match(r'(\d{6}_\d{4}_B\w+)', Path(pdf).name)   # 202607_3583_B022_20260727_xxx.pdf
        used = m.group(1) + '.pdf' if m else None
    if used:
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.execute('UPDATE issued SET prospectus_filename=? WHERE cb_code=?', (used, cb))
            conn.commit()
            conn.close()
            log(f'    ✓ 記錄使用版本: {used}')
        except Exception as e:
            log(f'    [WARN] 記錄 prospectus_filename 失敗: {e}')

    return True


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cb', type=str, help='只處理指定 cb_code')
    ap.add_argument('--all', action='store_true', help='含已有 analysis_md 的也重抓 (謹慎用)')
    ap.add_argument('--force', action='store_true',
                    help='已有分析也強制重跑 (網頁「🔄 強制更新」用;會覆蓋舊報告並改用最新版說明書)')
    ap.add_argument('--limit', type=int, help='只跑前 N 筆 (cost guard)')
    ap.add_argument('--model', default=DEFAULT_MODEL, help=f'Claude model (default {DEFAULT_MODEL})')
    ap.add_argument('--dry-run', action='store_true', help='不打 API,只顯示資訊')
    args = ap.parse_args()

    log('═' * 60)
    log(f'auto_analyze_cb start · model={args.model} · dry-run={args.dry_run}')
    log('═' * 60)

    # API key
    if not TOKEN_PATH.exists():
        log(f'[FATAL] 找不到 {TOKEN_PATH}'); sys.exit(1)
    api_key = TOKEN_PATH.read_text(encoding='utf-8').strip()
    client = anthropic.Anthropic(api_key=api_key)

    # --limit 改為「成功數上限」(不在 get_targets 截斷):跳過沒 B021 的很便宜(只抓 PDF 清單比對日期,不打 API),
    # 這樣每天穩定產出 N 份,而非舊版「看前 N 筆,前面剛好都 B021 未上就 0 進度」。
    targets = get_targets(args.cb, args.all, None)
    log(f'候選筆數: {len(targets)}' + (f' · 成功上限 {args.limit}' if args.limit else ' · 無上限(清完為止)'))
    if not targets:
        log('沒有需要分析的 CB,結束')
        return 1 if args.cb else 0      # 指定單檔卻找不到 = 失敗,不能回報成功

    succeeded = 0
    skipped = 0
    for t in targets:
        ok = process_one(t, client, args.model, dry_run=args.dry_run, force=args.force)
        if ok:
            succeeded += 1
            if args.limit and succeeded >= args.limit:
                log(f'達成功上限 {args.limit} → 停止,剩餘留待下次')
                break
        else:
            skipped += 1

    log('')
    log('═' * 60)
    log(f'auto_analyze_cb done · 成功 {succeeded} · 跳過/失敗 {skipped}')
    log('═' * 60)
    # 🔴 指定單檔時,SKIP/失敗必須回非 0,否則 serve.py 會照樣跑 build+publish、
    #    前端顯示「✓ 完成」→ 用戶按了、等了、重整了,內容卻沒變也看不出哪錯
    #    (2026-08-03 用戶問「強制更新是怎麼強制」時發現的假成功)。
    if args.cb and succeeded == 0:
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main() or 0)
