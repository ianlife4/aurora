// Aurora Cron Worker
// 排程觸發 GitHub Actions workflow_dispatch,用來取代不穩的 GHA schedule cron
// CF Cron Triggers 比 GHA cron 更準時(免費帳號也會如期觸發)

const OWNER = 'ianlife4';
const REPO = 'aurora';
const WORKFLOW = 'update-auction-data.yml';

async function dispatchWorkflow(env, ctx) {
  const url = `https://api.github.com/repos/${OWNER}/${REPO}/actions/workflows/${WORKFLOW}/dispatches`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'Accept': 'application/vnd.github+json',
      'Authorization': `Bearer ${env.GITHUB_TOKEN}`,
      'X-GitHub-Api-Version': '2022-11-28',
      'User-Agent': 'aurora-cron-worker',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ ref: 'main' }),
  });
  const ok = resp.ok;
  const status = resp.status;
  const body = ok ? '' : await resp.text();
  const log = `[${new Date().toISOString()}] dispatch ${WORKFLOW} → ${status} ${ok ? 'OK' : body}`;
  console.log(log);
  return { ok, status, log };
}

export default {
  // 排程觸發(CF cron)
  async scheduled(event, env, ctx) {
    ctx.waitUntil(dispatchWorkflow(env, ctx));
  },

  // 手動觸發 + 健康檢查
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === '/trigger') {
      // 簡易保護:需要正確的 ?key= 才能手動觸發
      const key = url.searchParams.get('key');
      if (!env.TRIGGER_KEY || key !== env.TRIGGER_KEY) {
        return new Response('forbidden', { status: 403 });
      }
      const result = await dispatchWorkflow(env, null);
      return new Response(JSON.stringify(result, null, 2), {
        status: result.ok ? 200 : 500,
        headers: { 'content-type': 'application/json; charset=utf-8' },
      });
    }

    return new Response(
      'aurora-cron worker\n' +
      'Schedules: TW 09:30 / 13:00 / 15:30 weekdays, 18:00 daily\n' +
      'Manual trigger: GET /trigger?key=<TRIGGER_KEY>\n',
      { headers: { 'content-type': 'text/plain; charset=utf-8' } }
    );
  },
};
