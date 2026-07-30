#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""classify_cb_motive.py — 把手上的 CB 分析報告依「發債動機」分類彙總。

用戶問「目前手上的 PDF 分析資料可以分幾類整理給我」→ 本支產出分類統計 + 明細。

分類來源兩條路:
  1. v2 報告 → 直接讀 `**類型:xxx**` 標籤 (權威)
  2. v1 舊報告 → 從「發債理由與目的」段的表格抽 (計劃項目 | 金額 | 比重),
     依項目關鍵字歸到 還債/擴產購料/資本支出/併購,再套 ≥70% 規則推類型

分類定義 (同 auto_analyze_cb.py SYSTEM_PROMPT):
  純還債型      償還借款 ≥70%   → 錢用來降槓桿,股價想像空間通常較小
  純擴產備料型  購料/營運/設備 ≥70% → 押注需求,通常較有故事
  混合型        兩者都不到 70%
  併購型        主要取得股權/資產

用法:
  py classify_cb_motive.py                    # 近半年
  py classify_cb_motive.py --since 2025-07-01 # 指定範圍
  py classify_cb_motive.py --csv out.csv      # 另存 CSV
"""
import argparse
import csv
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).parent
DB_PATH = HERE / 'cb_data.db'

REPAY = ('償還', '清償', '還款')
EXPAND = ('營運資金', '購料', '進料', '備料', '機器設備', '購置設備', '廠房', '擴建',
          '擴充', '產能', '研發', '設備款', '建廠')
MERGE = ('取得股權', '併購', '收購', '轉投資', '投資子公司')


def bucket(item):
    """把計劃項目名歸類。

    🔴 否定詞陷阱 (2026-07-30 踩到):項目常寫成「充實營運資金(**無**償還借款)」,
       只要看到「償還」就歸還債 → 整批 100% 擴產案被誤判成 100% 還債,結論完全相反
       (49311 新盛力一是純 AI BBU 備料案,卻被分到純還債型)。
       所以先把「無/未/不 償還…」這種否定片語剔掉,再判還債。
    """
    s = re.sub(r'[*\s]', '', item or '')
    s_repay = re.sub(r'[無未不非]償還[^、,，)）/]*', '', s)   # 去掉「無償還借款」等否定
    if any(k in s_repay for k in REPAY):
        return 'repay'
    if any(k in s for k in MERGE):
        return 'merge'
    if any(k in s for k in EXPAND):
        return 'expand'
    return 'other'


def extract_v1(md):
    """從 v1 報告的資金用途表格抽 {bucket: 占比%}。回 (dict, 是否抽到)"""
    if not md:
        return {}, False
    # 找「發債理由/資金運用/資金用途」段落
    m = re.search(r'##[^\n]*(?:發債理由|資金運用|資金用途)[^\n]*\n(.*?)(?=\n## |\Z)', md, re.S)
    seg = m.group(1) if m else md
    agg = defaultdict(float)
    got = False
    for line in seg.split('\n'):
        if not line.strip().startswith('|'):
            continue
        cells = [c.strip() for c in line.strip().strip('|').split('|')]
        if len(cells) < 2:
            continue
        name = cells[0]
        if any(k in name for k in ('計劃項目', '合計', '---', '項目')) or set(name) <= {'-', ':'}:
            continue
        pct = None
        for c in cells[1:]:
            pm = re.search(r'(\d+(?:\.\d+)?)\s*%', c)
            if pm:
                pct = float(pm.group(1))
                break
        if pct is None or pct <= 0 or pct > 100:
            continue
        b = bucket(name)
        if b == 'other':
            continue
        agg[b] += pct
        got = True
    return dict(agg), got


def label_from_pct(agg):
    if not agg:
        return None
    total = sum(agg.values()) or 1
    r = agg.get('repay', 0) / total * 100
    e = agg.get('expand', 0) / total * 100
    m = agg.get('merge', 0) / total * 100
    if m >= 70:
        return '併購型'
    if r >= 70:
        return '純還債型'
    if e >= 70:
        return '純擴產備料型'
    return '混合型'


def repay_rate_lo(md):
    """抓「擬償還借款」的最低利率 (%)。

    🔴 為什麼要抓這個:同樣掛「純還債型」意義差很多 —
       52892 宜鼎二 還的是 4.32%~4.55% 高息 → 用 0% CB 換掉真的省錢
       12951 生合一 還的是 0.925% 土銀貸款 → 幾乎省不到息,真正目的多半是
                     美化負債比 / 騰出銀行額度 (更接近財務體質修復型)
    只在「償還明細」表附近找,避免抓到毛利率、成長率等其他百分比。
    """
    if not md:
        return None
    # 🔴「償還銀行借款」字樣在警示訊號/解讀段也會出現,不能只取第一個命中
    #   (12951 生合一:第一個命中在 B 段散文裡 → 抓不到表格 → None)
    #   改成:找出所有候選錨點,只認【真的有銀行/機構 + 利率欄的表格列】
    # 銀行名不一定含「銀行」二字 (52892 宜鼎二表格寫「國泰世華」「玉山(USD)」「花旗」「匯豐」)
    BANKS = ('銀行', '銀', '農會', '信合社', '壽險', '票券', '產險', '合庫', '土銀',
             '國泰', '富邦', '中信', '中國信託', '玉山', '台新', '永豐', '兆豐', '華南',
             '第一', '彰化', '上海商銀', '花旗', '匯豐', '渣打', '星展', '安泰', '王道',
             '凱基', '日盛', '聯邦', '遠東', '京城', '高雄', '新光', '陽信', '板信', '三信')
    rates = []
    for m in re.finditer(r'償還|擬償還|清償', md):
        seg = md[m.start(): m.start() + 2000]
        for line in seg.split('\n'):
            s = line.strip()
            if not s.startswith('|'):
                continue
            if not any(b in s for b in BANKS):        # 沒銀行名 → 不是償還明細表
                continue
            for v in re.findall(r'(\d{1,2}\.\d{1,3})\s*%', s):
                f = float(v)
                if 0.3 <= f <= 9:                     # 台灣借款利率合理區間
                    rates.append(f)
    return min(rates) if rates else None


def label_from_v2(md):
    m = re.search(r'\*\*類型[:：]\s*`?([^`*\n]+?)`?\*\*', md)
    if m:
        return m.group(1).strip()
    m = re.search(r'類型[:：]\s*`([^`]+)`', md)
    return m.group(1).strip() if m else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--since', default='2026-01-30')
    ap.add_argument('--csv')
    ap.add_argument('--write', action='store_true',
                    help='把分類結果寫回 issued (motive_type/repay_pct/expand_pct/repay_rate_lo)')
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute('''
        SELECT cb_code, company, stock_code, method, conv_price, listing_date,
               fm_board_decision_date bd, analysis_md md
        FROM issued
        WHERE (is_legacy IS NULL OR is_legacy != 1)
          AND (is_withdrawn IS NULL OR is_withdrawn != 1)
          AND fm_board_decision_date >= ?
          AND analysis_md IS NOT NULL AND analysis_md != ''
        ORDER BY fm_board_decision_date DESC
    ''', (args.since,)).fetchall()
    conn.close()

    out = []
    for r in rows:
        md = r['md']
        v2 = '一眼判讀' in md
        if v2:
            lab, src, agg = label_from_v2(md), 'v2標籤', {}
        else:
            agg, got = extract_v1(md)
            if got:
                lab, src = label_from_pct(agg), 'v1推算'
            elif re.search(r'未揭露|有待補件|本稿本未含', md[:6000]):
                # 大型股申報稿本常還沒附「資金運用計畫」(如 23031 聯電一 p.108 起未含)
                # → 這是【公司還沒揭露】不是我們抽取失敗,要分開統計
                lab, src = 'PDF未揭露用途', '報告自述未揭露'
            else:
                lab, src = None, '抽不到'
        total = sum(agg.values()) or 1
        out.append({
            'cb': r['cb_code'], 'company': (r['company'] or '').strip(),
            'method': r['method'] or '', 'bd': (r['bd'] or '')[:10],
            'listing': (r['listing_date'] or '')[:10] or '未定',
            'label': lab or '(未判定)', 'src': src,
            'repay_pct': round(agg.get('repay', 0) / total * 100, 1) if agg else '',
            'expand_pct': round(agg.get('expand', 0) / total * 100, 1) if agg else '',
            'repay_rate_lo': repay_rate_lo(md) if (lab or '').startswith(('純還債', '財務體質', '混合')) else None,
        })

    cnt = Counter(x['label'] for x in out)
    print(f'=== 發債動機分類 · board >= {args.since} · 共 {len(out)} 檔 ===')
    print()
    for lab, n in cnt.most_common():
        print(f'  {lab:<14} {n:>3} 檔  ({n/len(out)*100:.0f}%)')
    print()
    for lab, _ in cnt.most_common():
        print(f'── {lab} ──')
        for x in [y for y in out if y['label'] == lab]:
            pc = f" 還債{x['repay_pct']}%/擴產{x['expand_pct']}%" if x['repay_pct'] != '' else ''
            print(f"   {x['cb']} {x['company'][:11]:<12} {x['method']:<4} 掛牌 {x['listing']:<11}{pc}  [{x['src']}]")
        print()

    if args.write:
        conn = sqlite3.connect(str(DB_PATH))
        n = 0
        for x in out:
            if x['label'] in ('(未判定)',):
                continue          # 判不出來的不寫,保持 NULL (前端顯示「—」)
            note = (f"還債 {x['repay_pct']}% / 擴產 {x['expand_pct']}%"
                    if x['repay_pct'] != '' else x['src'])
            if x['repay_rate_lo']:
                note += f" · 擬償還借款最低利率 {x['repay_rate_lo']}%"
            conn.execute('''UPDATE issued SET motive_type=?, repay_pct=?, expand_pct=?,
                            repay_rate_lo=?, motive_note=? WHERE cb_code=?''',
                         (x['label'],
                          x['repay_pct'] if x['repay_pct'] != '' else None,
                          x['expand_pct'] if x['expand_pct'] != '' else None,
                          x['repay_rate_lo'], note, x['cb']))
            n += 1
        conn.commit()
        conn.close()
        print(f'[OK] 寫入 issued: {n} 檔')

    if args.csv:
        with open(args.csv, 'w', newline='', encoding='utf-8-sig') as f:
            w = csv.DictWriter(f, fieldnames=list(out[0].keys()))
            w.writeheader()
            w.writerows(out)
        print(f'[OK] CSV → {args.csv}')


if __name__ == '__main__':
    main()
