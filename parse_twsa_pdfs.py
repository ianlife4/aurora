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
MONITOR_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_monitor_data.json")


def load_name_to_code():
    """Load _monitor_data.json → build name-prefix → code mapping for fuzzy lookup."""
    m = {}
    if not os.path.exists(MONITOR_DATA):
        return m
    try:
        with open(MONITOR_DATA, "r", encoding="utf-8") as f:
            data = json.load(f)
        for a in data.get("auctions", []):
            name = (a.get("name") or "").strip()
            code = (a.get("code") or "").strip()
            if name and code:
                m[name] = code
    except Exception as e:
        print(f"[WARN] Could not load monitor data for name lookup: {e}")
    return m


def lookup_code_by_name(name_map, filename, pdf_name_fragment):
    """Try to find stock code by matching names from filename / PDF text against _monitor_data."""
    # Strip "股份有限公司" / "-KY" / "控股" variants
    def normalize(s):
        return (
            s.replace("股份有限公司", "").replace("有限公司", "")
            .replace("-KY", "").replace("控股", "").replace("公司", "")
            .strip()
        )
    # Try exact filename-company match first
    fn_co = re.sub(r"^\d+_", "", filename.replace(".pdf", "")).strip()
    candidates = [normalize(fn_co), normalize(pdf_name_fragment)]

    # Try exact / prefix match in monitor_data
    for cand in candidates:
        if not cand:
            continue
        # Try full match, then startswith, then substring
        for mn_name, code in name_map.items():
            n_mn = normalize(mn_name)
            if cand == n_mn:
                return code
        for mn_name, code in name_map.items():
            n_mn = normalize(mn_name)
            if cand.startswith(n_mn) or n_mn.startswith(cand):
                return code
        for mn_name, code in name_map.items():
            n_mn = normalize(mn_name)
            if n_mn and (n_mn in cand or cand in n_mn):
                return code
    return None


def parse_pdf(path, name_map=None):
    """Parse a single TWSA bid statistics PDF.

    Returns dict with: code, name, category, summary, institutional, winning_prices
    Or None if PDF can't be parsed.
    """
    try:
        with pdfplumber.open(path) as pdf:
            if len(pdf.pages) < 1:
                return None
            all_text = "\n".join((p.extract_text() or "") for p in pdf.pages)
    except Exception as e:
        print(f"  [WARN] PDF open error: {e}")
        return None

    # Try header patterns in order of specificity:
    # 1. "奧義賽博-KY創 (7823) 創新板第一上市初上市"
    # 2. "微矽電子 創新板初上市" (no code)
    name = None
    code = None
    category_raw = ""
    m = re.search(r"([^\s()]+)\s*\(\s*(\d{4,6})\s*\)\s*([^\n]+)", all_text[:600])
    if m:
        name = m.group(1).strip()
        code = m.group(2).strip()
        category_raw = m.group(3).strip()
    else:
        # Try "<name> 創新板..." pattern, skip URL-like first lines
        lines = [l.strip() for l in all_text[:1500].split("\n") if l.strip()]
        for line in lines:
            if line.startswith("http") or "://" in line:
                continue
            # Skip lines that are date/time stamps (e.g., "2024/2/26 上午10:05...")
            if re.match(r"^\d{4}/\d{1,2}/\d{1,2}", line):
                continue
            # Match: name + category (cover all variations)
            m2 = re.match(r"^(\S+)\s+(創新板[^\s]*|初次上市|初次上櫃|初上市|初上櫃|第一上市|第一上櫃)", line)
            if m2:
                name = m2.group(1).strip()
                category_raw = line[len(name):].strip()
                break
            # Also try inline "有價證券名稱： 代號 名稱" pattern (T004 table header)
            m3 = re.match(r"有價證券名稱\s*[:：]\s*(\d{4,6})\s+(\S+)", line)
            if m3:
                code = m3.group(1)
                name = m3.group(2).strip()
                break

    if not name:
        return None

    # Try to recover code from _monitor_data.json by name matching
    if not code and name_map:
        filename = os.path.basename(path)
        code = lookup_code_by_name(name_map, filename, name)

    if not code:
        print(f"    [WARN] {path}: could not determine stock code for '{name}'")
        return None

    # Simplify category
    category = category_raw
    if "創新板" in category_raw:
        category = "創新板"
    elif "初次上市" in category_raw or "初上市" in category_raw or "第一上市" in category_raw:
        category = "初上市"
    elif "初次上櫃" in category_raw or "初上櫃" in category_raw or "第一上櫃" in category_raw:
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
    # 1 200.0000 20 4,000.00  OR  1 1,479.0000 2 2,958.00
    # Use MULTILINE so ^ matches each line start (avoids newline-consumption bug)
    # Price can have commas (e.g. "1,479.0000") so allow [\d,]+\.\d+
    winning_prices = []
    for mm in re.finditer(
        r"^\s*(\d+)\s+([\d,]+\.\d+)\s+([\d,]+)\s+([\d,.]+)\s*$",
        norm,
        re.MULTILINE,
    ):
        try:
            seq = int(mm.group(1))
            price = float(mm.group(2).replace(",", ""))
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
                raw = json.load(f)
            # Support both formats: {updated_at, total, stocks:{...}} and flat {...}
            if isinstance(raw, dict) and "stocks" in raw and isinstance(raw["stocks"], dict):
                existing = raw["stocks"]
            else:
                existing = raw
        except Exception as e:
            print(f"[WARN] Could not load existing {OUTPUT}: {e}")
            existing = {}

    processed_pdfs = {v.get("_source_pdf") for v in existing.values() if isinstance(v, dict)}

    # Load monitor_data for fuzzy code lookup
    name_map = load_name_to_code()
    print(f"[INFO] Name-code map from monitor_data: {len(name_map)} entries")

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
        data = parse_pdf(path, name_map)
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
