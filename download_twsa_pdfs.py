# -*- coding: utf-8 -*-
"""
TWSA 開標統計表 PDF 批次下載 (Playwright 自動化)

用法:
    python download_twsa_pdfs.py          # 下載近 5 年全部
    python download_twsa_pdfs.py 2026     # 只下載指定年度（民國/西元都可）
    python download_twsa_pdfs.py 2026 2025  # 多個年度

流程:
    1. 開啟 https://web.twsa.org.tw/edoc2/default.aspx
    2. 切到「競拍公告/開標統計表」radio
    3. 每個年度下，依序點每列第 2 個 icon（開標統計表）
    4. 捕獲下載，存到 twsa_pdfs/ (檔名格式: {案號}_{code}_{name}.pdf)
    5. 已存在的檔案 skip（增量更新）

注意:
    - TWSA 只對 IPO 競拍有此 PDF（CB/公司債沒有）
    - 有些舊案可能 PDF 不存在，會被 skip
"""
import os, sys, re, time, json
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

ROOT = os.path.dirname(os.path.abspath(__file__))
PDF_DIR = os.path.join(ROOT, "twsa_pdfs")
os.makedirs(PDF_DIR, exist_ok=True)

URL = "https://web.twsa.org.tw/edoc2/default.aspx"


def safe_filename(s):
    """Sanitize string for filename."""
    if not s:
        return "unknown"
    s = re.sub(r'[\\/:*?"<>|]', "_", s)
    s = s.replace("\n", "_").replace("\r", "_").strip()
    return s[:80]


def existing_case_numbers():
    """Return set of already-downloaded case numbers (e.g., '114012')."""
    existing = set()
    for f in os.listdir(PDF_DIR):
        m = re.match(r"^(\d{6})_", f)
        if m:
            existing.add(m.group(1))
    return existing


def switch_mode(page, radio_id):
    """Switch report type radio. radio_id: '1'=Auction, '8'=AuctionInquiring."""
    try:
        already = page.evaluate(
            f"document.querySelector('#ctl00_cphMain_rblReportType_{radio_id}')?.checked"
        )
        if not already:
            page.click(f"#ctl00_cphMain_rblReportType_{radio_id}")
            page.wait_for_load_state("networkidle", timeout=15000)
            time.sleep(1.5)
        return True
    except Exception as e:
        print(f"[ERROR] Could not switch to radio {radio_id}: {e}")
        return False


def download_year_mode(page, year_roc, existing_cases, mode_radio):
    """Download for a given year under a specific radio mode.

    mode_radio: '1'=Auction (舊, 2024-), '8'=AuctionInquiring (新, 2025+)
    Returns: (downloaded_count, skipped_count, failed_count)
    """
    mode_name = "競拍公告(舊)" if mode_radio == "1" else "競拍申購公告(新)"
    print(f"\n--- [{year_roc}] {mode_name} ---")

    if not switch_mode(page, mode_radio):
        return 0, 0, 0

    try:
        page.select_option("#ctl00_cphMain_ddlYear", str(year_roc))
        page.wait_for_load_state("networkidle", timeout=20000)
        time.sleep(2.5)
    except Exception as e:
        print(f"[ERROR] Could not select year {year_roc}: {e}")
        return 0, 0, 0

    try:
        page.wait_for_selector("#ctl00_cphMain_gvResult", timeout=15000)
    except PWTimeoutError:
        print("[WARN] No result grid")
        return 0, 0, 0

    # Collect all row info first.
    # Columns: 0:序號 1:發行公司 2:主辦承銷商 3:發行性質 4:承銷股數 5:競拍股數
    #          6:投標期間 7:最低承銷價格 8:公告檔 9:開標統計表
    # Button names: imgbtnAuctionFileName (公告檔) / imgbtnReportFileName (開標統計表)
    rows_data = page.evaluate("""() => {
        const grid = document.querySelector('#ctl00_cphMain_gvResult');
        if (!grid) return [];
        const trs = grid.querySelectorAll('tr');
        const result = [];
        trs.forEach(tr => {
            const tds = tr.querySelectorAll('td');
            if (tds.length < 8) return;
            const caseNo = (tds[0]?.innerText || '').trim();
            if (!/^\\d{6}$/.test(caseNo)) return;
            // 開標統計表 button (imgbtnReportFileName)
            let reportBtn = null;
            tr.querySelectorAll('input[type="image"]').forEach(b => {
                if (b.name && b.name.includes('imgbtnReportFileName')) reportBtn = b.name;
            });
            result.push({
                caseNo,
                nameCell: (tds[1]?.innerText || '').trim(),
                underwriter: (tds[2]?.innerText || '').trim(),
                natureCell: (tds[3]?.innerText || '').trim(),
                qty: (tds[5]?.innerText || '').trim(),
                period: (tds[6]?.innerText || '').trim(),
                minBid: (tds[7]?.innerText || '').trim(),
                reportBtn,
            });
        });
        return result;
    }""")

    print(f"[INFO] Found {len(rows_data)} rows on {year_roc}")
    downloaded = skipped = failed = 0

    # Pre-filter to IPO only for count
    ipo_rows = [r for r in rows_data if any(k in r["natureCell"] for k in ["初上市", "初上櫃", "初次上市", "初次上櫃", "創新板", "第一上市", "第一上櫃"])]
    print(f"[INFO] IPO rows (excluding CB/bonds): {len(ipo_rows)}")

    for i, row in enumerate(rows_data):
        case_no = row["caseNo"]
        name = row["nameCell"]
        nature = row["natureCell"]

        # Skip if already downloaded
        if case_no in existing_cases:
            skipped += 1
            continue

        # Skip non-IPO (CB/公司債 don't have bid auction details)
        if not any(k in nature for k in ["初上市", "初上櫃", "初次上市", "初次上櫃", "創新板", "第一上市", "第一上櫃"]):
            skipped += 1
            continue

        if not row["reportBtn"]:
            print(f"  [{case_no}] {name[:20]} → no imgbtnReportFileName, skip")
            skipped += 1
            continue

        btn_name = row["reportBtn"]
        print(f"  [{case_no}] {name[:25]} ({nature[:15]}) ...", end=" ", flush=True)

        # Construct CSS selector (escape $ for :has syntax)
        btn_selector = f'input[type="image"][name="{btn_name}"]'

        try:
            # Trigger download by clicking. PDF download triggers in new navigation.
            with page.expect_download(timeout=30000) as dl_info:
                page.click(btn_selector)
            download = dl_info.value
            # Suggested filename from server
            src_name = download.suggested_filename or "download.pdf"
            # Our filename: caseNo_name.pdf
            stem = safe_filename(name).replace(" ", "")
            dest_name = f"{case_no}_{stem}.pdf"
            dest_path = os.path.join(PDF_DIR, dest_name)
            download.save_as(dest_path)
            if os.path.getsize(dest_path) < 1000:
                # Likely empty/error PDF
                os.remove(dest_path)
                print("EMPTY, skipped")
                failed += 1
            else:
                size_kb = os.path.getsize(dest_path) // 1024
                print(f"OK {size_kb}KB")
                downloaded += 1
                existing_cases.add(case_no)
            time.sleep(1.0)  # politeness delay
        except PWTimeoutError:
            print("TIMEOUT")
            failed += 1
            time.sleep(2)
        except Exception as e:
            print(f"ERR {type(e).__name__}")
            failed += 1
            time.sleep(2)

    print(f"  [{mode_name}] ✅ {downloaded} / ⏭ {skipped} / ❌ {failed}")
    return downloaded, skipped, failed


def download_year(page, year_roc, existing_cases):
    """Try both Auction (radio 1) and AuctionInquiring (radio 8) for a year."""
    print(f"\n{'='*60}")
    print(f"[Year {year_roc}（西元 {int(year_roc)+1911}）]")
    print(f"{'='*60}")
    d1, s1, f1 = download_year_mode(page, year_roc, existing_cases, "8")
    d2, s2, f2 = download_year_mode(page, year_roc, existing_cases, "1")
    d, s, f = d1 + d2, s1 + s2, f1 + f2
    print(f"[{year_roc}] TOTAL ✅ {d} downloaded, ⏭ {s} skipped, ❌ {f} failed")
    return d, s, f


def main():
    # Parse year args
    args = sys.argv[1:]
    if args:
        years = []
        for a in args:
            y = int(a)
            if y > 1911:
                y -= 1911  # convert to ROC
            years.append(str(y))
    else:
        # Default: last 5 years (ROC)
        now_roc = datetime.now().year - 1911
        years = [str(now_roc - i) for i in range(6)]  # current year + 5 previous
    print(f"[INFO] Target years (ROC): {years}")

    existing = existing_case_numbers()
    print(f"[INFO] Already have {len(existing)} PDFs in {PDF_DIR}")

    total_d = total_s = total_f = 0
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--ignore-certificate-errors", "--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(
            ignore_https_errors=True,
            accept_downloads=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        page = ctx.new_page()

        try:
            print(f"[INFO] Opening {URL}")
            page.goto(URL, wait_until="domcontentloaded", timeout=30000)
            time.sleep(1)

            print("[INFO] Will try both 競拍申購公告 (2025+) + 競拍公告 (≤2024) per year")
            # Process each year with both modes
            for yr in years:
                d, s, f = download_year(page, yr, existing)
                total_d += d
                total_s += s
                total_f += f
        finally:
            ctx.close()
            browser.close()

    print(f"\n{'='*60}")
    print(f"[DONE] Total: ✅ {total_d} downloaded, ⏭ {total_s} skipped, ❌ {total_f} failed")
    print(f"[DONE] PDFs in {PDF_DIR}: {len([f for f in os.listdir(PDF_DIR) if f.endswith('.pdf')])}")
    print(f"{'='*60}")
    print("\n下一步: python parse_twsa_pdfs.py")


if __name__ == "__main__":
    main()
