# -*- coding: utf-8 -*-
"""
TWSA 開標統計表 PDF 批次解析工具

用法:
    1. 手動從 https://web.twsa.org.tw/edoc2/default.aspx
       切到「競拍公告/開標統計表」並下載 PDF 到 twsa_pdfs/ 資料夾
    2. python parse_twsa_pdfs.py
    3. 會輸出 _bid_details.json 給網頁使用

PDF 結構:
    P1: 總覽（合格投標筆數、得標筆數、金額）
    P2: 法人投標/得標統計（vs 散戶）
    P3+: 得標單價總表（逐筆得標價 × 張數）

增量更新：已解析過的 PDF（依檔名）會 skip
"""
import json, os, re, sys
from datetime import datetime

try:
    import pdfplumber
except ImportError:
    print("[ERROR] 請先安裝: pip install pdfplumber")
    sys.exit(1)

PDF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "twsa_pdfs")
OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_bid_details.json")


def parse_pdf(path):
    """Parse a single TWSA bid statistics PDF.

    Returns dict with: code, name, category, summary, institutional, winning_prices
    Or None if PDF doesn't match expected format.
    """
    try:
        with pdfplumber.open(path) as pdf:
            if len(pdf.pages) < 3:
                return None
            all_text = "\n".join((p.extract_text() or "") for p in pdf.pages)
    except Exception as e:
        print(f"  [WARN] PDF open error: {e}")
        return None

    # Header: 股名 (代號) 類別
    # 例: "奧義賽博-KY創 (7823) 創新板第一上市初上市"
    m = re.search(r"([^\s()]+)\s*\(\s*(\d{4,6})\s*\)\s*([^\n]+)", all_text[:500])
    if not m:
        return None
    name = m.group(1).strip()
    code = m.group(2).strip()
    category_raw = m.group(3).strip()

    # Simplify category
    category = category_raw
    if "創新板" in category_raw:
        category = "創新板"
    elif "初上市" in category_raw:
        category = "初上市"
    elif "初上櫃" in category_raw:
        category = "初上櫃"

    # PDF contains unicode variants (⼀⽅⾦⾴⼤⽇...), normalize to regular chars first
    variants = {
        "⼀":"一","⼆":"二","⽅":"方","⾴":"頁","⾦":"金","⼤":"大",
        "⼩":"小","⼯":"工","⼈":"人","⼭":"山","⽤":"用","⼒":"力",
        "⾼":"高","⾏":"行","⽬":"目","⼿":"手","⼝":"口","⽇":"日",
        "⽉":"月","⾞":"車","⽔":"水","⽩":"白","⾺":"馬","⾝":"身",
        "⾓":"角","⾕":"谷","⿓":"龍","⾯":"面","⿍":"鼎","⻑":"長",
    }
    norm = all_text
    for k, v in variants.items():
        norm = norm.replace(k, v)

    # Key prices/dates (using normalized text)
    def num(pattern):
        mm = re.search(pattern, norm)
        if mm:
            try:
                return float(mm.group(1).replace(",", ""))
            except:
                return None
        return None

    min_bid = num(r"最低承銷價格\s*[:：]?\s*([\d,]+\.?\d*)")
    public_price = num(r"公開承銷價格\s*[:：]?\s*([\d,]+\.?\d*)")
    min_win = num(r"最低得標價格\s*[:：]?\s*([\d,]+\.?\d*)")
    max_win = num(r"最高得標價格\s*[:：]?\s*([\d,]+\.?\d*)")
    avg_win = num(r"得標加權平均價格\s*[:：]?\s*([\d,]+\.?\d*)")

    open_date = None
    mm = re.search(r"開標日期\s*[:：]?\s*(\d{4}/\d{2}/\d{2})", norm)
    if mm:
        open_date = mm.group(1)

    # --- Page 1 總覽 ---
    # 合格投標筆數 合格投標數量(仟股) 得標筆數 得標數量(仟股) 得標總金額(仟元)
    # 1,064 9,727 167 1,580 229,564.47
    summary = {}
    # Use permissive matcher: find the header line then numbers on next line
    mm = re.search(
        r"合格投標筆數[^\n]*得標總金額[^\n]*\n\s*"
        r"([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+([\d,.]+)",
        norm,
    )
    if mm:
        summary = {
            "qualified_bids": int(mm.group(1).replace(",", "")),
            "qualified_lots": int(mm.group(2).replace(",", "")),  # 仟股
            "won_bids": int(mm.group(3).replace(",", "")),
            "won_lots": int(mm.group(4).replace(",", "")),  # 仟股
            "won_amount_k": float(mm.group(5).replace(",", "")),  # 仟元
        }

    # --- Page 2 法人統計 ---
    # 合格投標筆數 合格投標數量(仟股) 投標數量比率% 得標筆數 得標數量(仟股) 得標數量比率%
    # 40 1,872 19.25 5 213 13.48
    institutional = {}
    mm = re.search(r"法人投標[^\n]{0,50}得標統計表[\s\S]{0,2500}", norm)
    inst_section = mm.group(0) if mm else ""
    # Header is split across lines by PDF column wrapping; just look for the
    # data row pattern: 3 groups after 法人投標 section
    mm = re.search(
        r"^\s*([\d,]+)\s+([\d,]+)\s+(\d+\.\d+)\s+([\d,]+)\s+([\d,]+)\s+(\d+\.\d+)\s*$",
        inst_section,
        re.MULTILINE,
    )
    if mm:
        institutional = {
            "bids": int(mm.group(1).replace(",", "")),
            "lots": int(mm.group(2).replace(",", "")),
            "bid_ratio_pct": float(mm.group(3)),
            "won_bids": int(mm.group(4).replace(",", "")),
            "won_lots": int(mm.group(5).replace(",", "")),
            "won_ratio_pct": float(mm.group(6)),
        }

    # --- Page 3+ 得標單價總表 ---
    # 序號 得標單價(元) 得標數量(仟股) 得標總金額(仟元)
    # 1 200.0000 20 4,000.00
    # Use MULTILINE so ^ matches each line start (avoids newline-consumption bug)
    winning_prices = []
    for mm in re.finditer(
        r"^\s*(\d+)\s+(\d+\.\d+)\s+([\d,]+)\s+([\d,.]+)\s*$",
        norm,
        re.MULTILINE,
    ):
        try:
            seq = int(mm.group(1))
            price = float(mm.group(2))
            lots = int(mm.group(3).replace(",", ""))
            amount = float(mm.group(4).replace(",", ""))
            # Sanity: price * lots(仟股) ≈ amount(仟元)
            if amount > 0 and abs(price * lots - amount) / amount < 0.01:
                winning_prices.append(
                    {"seq": seq, "price": price, "lots": lots, "amount": amount}
                )
        except (ValueError, ZeroDivisionError):
            continue

    # Dedupe by seq + sort
    seen = set()
    unique_prices = []
    for p in winning_prices:
        if p["seq"] not in seen:
            seen.add(p["seq"])
            unique_prices.append(p)
    winning_prices = sorted(unique_prices, key=lambda x: x["seq"])

    return {
        "code": code,
        "name": name,
        "category": category,
        "open_date": open_date,
        "min_bid_price": min_bid,
        "public_price": public_price,
        "min_win_price": min_win,
        "max_win_price": max_win,
        "avg_win_price": avg_win,
        "summary": summary,
        "institutional": institutional,
        "winning_prices": winning_prices,
        "_source_pdf": os.path.basename(path),
    }


def main():
    if not os.path.isdir(PDF_DIR):
        os.makedirs(PDF_DIR, exist_ok=True)
        print(f"[INFO] Created {PDF_DIR}")

    # Load existing data (incremental)
    existing = {}
    if os.path.exists(OUTPUT):
        try:
            with open(OUTPUT, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception as e:
            print(f"[WARN] Could not load existing {OUTPUT}: {e}")
            existing = {}

    processed_pdfs = {v.get("_source_pdf") for v in existing.values() if v}

    pdfs = sorted(f for f in os.listdir(PDF_DIR) if f.lower().endswith(".pdf"))
    print(f"[INFO] Found {len(pdfs)} PDFs in {PDF_DIR}")
    print(f"[INFO] Already parsed: {len(processed_pdfs)}")

    added = 0
    skipped = 0
    errors = 0
    for fname in pdfs:
        if fname in processed_pdfs:
            skipped += 1
            continue
        path = os.path.join(PDF_DIR, fname)
        print(f"  Parsing {fname}...")
        data = parse_pdf(path)
        if not data:
            print(f"    [WARN] Could not parse (format unrecognized)")
            errors += 1
            continue
        code = data["code"]
        # Support multiple auctions per code by keying on (code, open_date)
        key = f"{code}_{data.get('open_date','')}".rstrip("_")
        existing[key] = data
        added += 1
        wp_count = len(data.get("winning_prices", []))
        print(
            f"    OK: {data['name']} ({code}) 得標價 {wp_count} 筆, "
            f"法人得標率 {data.get('institutional', {}).get('won_ratio_pct', '-')}%"
        )

    # Write output
    out = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total": len(existing),
        "stocks": existing,
    }
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print()
    print(f"[DONE] Parsed: +{added}, Skipped (already done): {skipped}, Errors: {errors}")
    print(f"[DONE] Total stocks in {OUTPUT}: {len(existing)}")


if __name__ == "__main__":
    main()
