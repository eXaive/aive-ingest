/**
 * scripts/verify-watch-evaluation-trigger.ts -- the Watch monitor-tick trigger's contract, with fetch replaced by a spy.
 *
 *   npm run verify:watch-evaluation-trigger
 *
 * Checks: the token goes only into the Authorization header; the body is exactly { trigger: 'MONITOR_TICK' } (the route
 * rejects any other key, and the caller never asserts an ingest state); logs never contain the token, ids or free text
 * from the response; exit codes follow the header of workers/mcp-watch-evaluation-trigger.ts. Also checks the wiring in
 * collect-mcp-tools.yml: the step sits directly after "Collect tools/list", is gated by the repo variable, and the gate
 * step fails the job when it fails.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { triggerWatchEvaluation } from '../workers/mcp-watch-evaluation-trigger';

const TOKEN = 'tok_' + 'x'.repeat(60), BASE = 'https://app.example';
const EVENT_ID = '0b8f3c1e-6f2a-4d7b-9a51-2c4e8d0f9a13';
const env = { AIVE_BASE_URL: BASE + '/', MCP_WATCH_EVALUATOR_TOKEN: TOKEN, GITHUB_RUN_ID: '36730916985', GITHUB_RUN_ATTEMPT: '2' };
const summary = (over: Record<string, unknown>) => ({ evaluated: 4, events: 1, failed: 0, uncertainWrites: 0, skipped: 0, durationMs: 1234,
  evaluationLatencyMs: [300, 310], peakHeapBytes: 1, backlog: { due: 0, leased: 0, oldest_due: '2026-10-15T08:00:00Z', failed: 0, unseeded: 0, watch: EVENT_ID },
  notifications: { sent: 1, failed: 0, unknown: 0, skipped: 0, failures: [] }, ...over });

async function run(response: { status: number; body: unknown } | 'network', e: Record<string, string | undefined> = env) {
  const calls: { url: string; init: RequestInit }[] = [], lines: string[] = [];
  const fake = (async (url: string, init: RequestInit) => {
    calls.push({ url, init });
    if (response === 'network') throw new Error('ECONNRESET');
    return new Response(JSON.stringify(response.body), { status: response.status });
  }) as unknown as typeof fetch;
  const code = await triggerWatchEvaluation(e, fake, l => lines.push(l));
  return { code, calls, log: lines.join('\n') };
}

(async () => {
  const ok = await run({ status: 200, body: summary({}) });
  assert.equal(ok.code, 0);
  assert.equal(ok.calls.length, 1);
  assert.equal(ok.calls[0].url, `${BASE}/api/internal/mcp/watch-evaluation`);
  assert.equal(ok.calls[0].init.method, 'POST');
  assert.equal((ok.calls[0].init.headers as Record<string, string>).Authorization, `Bearer ${TOKEN}`);
  assert.deepEqual(JSON.parse(String(ok.calls[0].init.body)), { trigger: 'MONITOR_TICK' }, 'exactly the trigger; the run id is never forwarded');
  assert.ok(!ok.log.includes(TOKEN) && !ok.log.includes(EVENT_ID) && !ok.log.includes('2026-10-15'), 'no token, ids or timestamps in public logs');
  assert.match(ok.log, /HTTP 200 evaluated=4 events=1 failed=0 uncertain=0 skipped=0 durationMs=1234 backlog=due=0,leased=0,failed=0,unseeded=0 notifications=sent:1,failed:0,unknown:0,skipped:0/);
  assert.ok(!ok.log.includes('::warning::'));

  assert.equal((await run({ status: 200, body: summary({ evaluated: 0, events: 0 }) })).code, 0, 'nothing due is green');
  assert.equal((await run({ status: 200, body: summary({ skipped: 2 }) })).code, 0, 'stale lease / authority change is not a failure');
  assert.equal((await run({ status: 200, body: summary({ failed: 1 }) })).code, 1, 'an evaluation failure is a coverage gap: red');
  assert.equal((await run({ status: 200, body: summary({ uncertainWrites: 1 }) })).code, 1, 'uncertain writes are red even on 200');
  assert.equal((await run({ status: 503, body: summary({ uncertainWrites: 1 }) })).code, 1);
  for (const status of ['MONITORING_DISABLED', 'SETUP_REQUIRED', 'MONITORING_UNAVAILABLE']) {
    const r = await run({ status: 503, body: { status } });
    assert.equal(r.code, 1, status);
    assert.match(r.log, new RegExp(`HTTP 503 status=${status} `));
  }
  assert.equal((await run({ status: 401, body: { status: 'UNAUTHORIZED' } })).code, 1);
  assert.equal((await run({ status: 400, body: { status: 'INVALID_REQUEST' } })).code, 1);
  assert.equal((await run({ status: 200, body: { version: 'MCP_WATCH_EVALUATION_BATCH_V1', status: 'COMPLETE' } })).code, 1, 'a batch summary is not a tick summary');
  assert.equal((await run({ status: 200, body: 'not json' })).code, 1);
  assert.equal((await run('network')).code, 1);
  const leak = await run({ status: 503, body: { status: `owner ${EVENT_ID} leaked` } });
  assert.ok(!leak.log.includes(EVENT_ID) && !leak.log.includes('status='), 'free-text status never reaches the log');

  const notify = await run({ status: 200, body: summary({ notifications: { sent: 0, failed: 1, unknown: 1, skipped: 0, failures: ['HTTP_422', `bad ${EVENT_ID}`] } }) });
  assert.equal(notify.code, 0, 'delivery outcomes never fail the tick');
  assert.match(notify.log, /::warning::watch notifications not confirmed: HTTP_422=1,OTHER=1/);
  assert.ok(!notify.log.includes(EVENT_ID));

  const unset = await run({ status: 200, body: summary({}) }, { ...env, MCP_WATCH_EVALUATOR_TOKEN: '' });
  assert.deepEqual([unset.code, unset.calls.length], [1, 0], 'fails closed without calling anything');
  const noBase = await run({ status: 200, body: summary({}) }, { ...env, AIVE_BASE_URL: ' ' });
  assert.deepEqual([noBase.code, noBase.calls.length], [1, 0]);

  // Wiring: directly after "Collect tools/list", variable-gated, secrets via env, and counted by the gate step.
  const wf = readFileSync('.github/workflows/collect-mcp-tools.yml', 'utf8').replace(/\r\n/g, '\n');
  const names = [...wf.matchAll(/^ {6}- name: (.+)$/gm)].map(m => m[1].trim());
  assert.equal(names[names.indexOf('Collect tools/list') + 1], 'Evaluate MCP Watches', 'step order');
  const step = wf.slice(wf.indexOf('- name: Evaluate MCP Watches'), wf.indexOf('- name: Collect resources/list'));
  assert.match(step, /id: watch\n/);
  assert.match(step, /if: always\(\) && steps\.tools\.outcome != 'skipped' && vars\.MCP_WATCH_MONITOR_TICK_ENABLED == 'true'\n/);
  assert.match(step, /continue-on-error: true\n/);
  assert.match(step, /AIVE_BASE_URL: \$\{\{ secrets\.AIVE_BASE_URL \}\}\n/);
  assert.match(step, /MCP_WATCH_EVALUATOR_TOKEN: \$\{\{ secrets\.MCP_WATCH_EVALUATOR_TOKEN \}\}\n/);
  assert.match(step, /run: npx tsx workers\/mcp-watch-evaluation-trigger\.ts\n/);
  assert.ok(!/AIVE_INGEST_DATABASE_URL/.test(step), 'the trigger step never sees the database credential');
  assert.match(wf, /\[ "\$\{\{ steps\.watch\.outcome \}\}" = "failure" \] && failed=1/);

  console.log('verify-watch-evaluation-trigger: all checks passed');
})().catch(e => { console.error(e); process.exitCode = 1; });
