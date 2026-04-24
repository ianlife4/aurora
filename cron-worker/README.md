# Aurora Cron Worker

用 Cloudflare Workers Cron Triggers 取代不穩的 GitHub Actions schedule cron。
Worker 只做一件事:定時呼叫 GitHub API 觸發 `update-auction-data.yml` workflow_dispatch。

## 為什麼需要?

GHA 免費帳號的 `schedule` cron 在高峰時段可能延遲或**完全跳過**。
CF Cron Triggers 是按時觸發的(即使是免費版),靠它 kick 才穩定。

## 架構

```
CF Cron (UTC 01:30/05:00/07:30/10:00)
    ↓
aurora-cron worker
    ↓ POST /workflows/update-auction-data.yml/dispatches
GitHub Actions
    ↓ python fetch_auction_monitor.py + git push
_monitor_data.json 更新 → GitHub Pages → dashboard
```

## 首次部署

### 1. 建立 GitHub PAT

至 https://github.com/settings/tokens → Fine-grained tokens → **Generate new token**
- Repository access: Only select repositories → `ianlife4/aurora`
- Permissions → Repository permissions:
  - **Actions**: Read and write
  - **Metadata**: Read-only (自動)
- Expiration: 依需求(建議 1 年)

複製產生的 token(`github_pat_xxx...`)。

### 2. 部署 Worker

```bash
cd C:\Users\J.Chun\Desktop\競拍資歷\cron-worker
npm install -g wrangler  # 若還沒裝
wrangler login           # 瀏覽器登入 CF 帳號(若還沒登入)
wrangler deploy
wrangler secret put GITHUB_TOKEN
# 貼上剛剛複製的 PAT → Enter
wrangler secret put TRIGGER_KEY
# 貼上一組你自訂的密碼(手動觸發 /trigger 用,隨便設) → Enter
```

### 3. 測試

瀏覽器開啟 `https://aurora-cron.ian-4k.workers.dev/trigger?key=<你剛設的 TRIGGER_KEY>`
預期回應:
```json
{"ok": true, "status": 204, "log": "[...] dispatch update-auction-data.yml → 204 OK"}
```

然後到 https://github.com/ianlife4/aurora/actions 看應該立刻有一筆 workflow_dispatch 事件進來。

## 觀察排程是否正常

```bash
wrangler tail  # 即時看 console.log
```

或在 CF Dashboard → Workers → aurora-cron → Logs。

## 修改排程

編輯 `wrangler.toml` 的 `[triggers].crons`,然後再 `wrangler deploy`。
