/**
 * scripts/verify-watch-evaluation-trigger.ts -- the Watch evaluation trigger's contract, with fetch replaced by a spy.
 *
 *   npx tsx scripts/verify-watch-evaluation-trigger.ts      (not in package.json / verify.yml: the trigger is not wired)
 *
 * Checks: the token goes only into the Authorization header; the body names the workflow run and nothing else (the
 * caller never asserts an ingest state); logs never contain the token or the response body; exit codes follow the
 * header of workers/mcp-watch-evaluation-trigger.ts.
 */
import assert from 'node:assert/strict';
import { triggerWatchEvaluation } from '../workers/mcp-watch-evaluation-trigger';

const TOKEN = 'tok_' + 'x'.repeat(60), BASE = 'https://app.example';
const env = { AIVE_BASE_URL: BASE + '/', MCP_WATCH_EVALUATOR_TOKEN: TOKEN, GITHUB_RUN_ID: '36730916985', GITHUB_RUN_ATTEMPT: '2' };
const summary = (over: Record<string, unknown>) => ({ version: 'MCP_WATCH_EVALUATION_BATCH_V1', status: 'COMPLETE', skipReason: null, basis: { state: 'COMPLETE', toolCollectionRunId: 'secret-run-id' },
  pages: 3, watchesConsidered: 204, watchesEvaluated: 204, watchesSkipped: 0, eventsRecorded: 5, counts: { HEALTHY: 199, MATERIAL_CHANGE: 5 }, failures: [], stoppedBy: 'END_OF_WATCHES', ...over });

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
  assert.equal((ok.calls[0].init.headers as Record<string, string>).Authorization, `Bearer ${TOKEN}`);
  assert.deepEqual(JSON.parse(String(ok.calls[0].init.body)), { trigger: 'INGEST_COMPLETE', ingestWorkflowRunId: '36730916985', ingestWorkflowRunAttempt: 2 });
  assert.ok(!ok.log.includes(TOKEN) && !ok.log.includes('secret-run-id'), 'no token, no ids in public logs');
  assert.match(ok.log, /status=COMPLETE basis=COMPLETE .* events=5 /);

  assert.equal((await run({ status: 200, body: summary({ status: 'SKIPPED', skipReason: 'COLLECTION_NOT_TERMINAL', pages: 0 }) })).code, 0, 'skip: the collector step reports its own failure');
  assert.equal((await run({ status: 200, body: summary({ status: 'PARTIAL', stoppedBy: 'TIME_BUDGET' }) })).code, 1, 'coverage gap is red');
  assert.equal((await run({ status: 503, body: summary({ status: 'FAILED' }) })).code, 1);
  assert.equal((await run({ status: 401, body: { status: 'UNAUTHORIZED' } })).code, 1);
  assert.equal((await run('network')).code, 1);
  const unset = await run({ status: 200, body: summary({}) }, { ...env, MCP_WATCH_EVALUATOR_TOKEN: '' });
  assert.deepEqual([unset.code, unset.calls.length], [1, 0], 'fails closed without calling anything');
  const noRun = await run({ status: 200, body: summary({}) }, { ...env, GITHUB_RUN_ID: 'abc', GITHUB_RUN_ATTEMPT: '1' });
  assert.deepEqual(JSON.parse(String(noRun.calls[0].init.body)), { trigger: 'INGEST_COMPLETE' }, 'an invalid run id is omitted, never forwarded');
  console.log('verify-watch-evaluation-trigger: all checks passed');
})().catch(e => { console.error(e); process.exitCode = 1; });
