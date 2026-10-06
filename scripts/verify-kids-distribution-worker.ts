// Pure check of the kids distribution trigger: one authenticated POST to AIVE, sanitized output,
// red on owner-attention outcomes, and no business logic, database or provider credentials.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { ALERT_OUTCOMES, ROUTE, TIMEOUT_MS, runKidsDistribution } from '../workers/broadcast-kids-distribution';

async function main() {
  const src = fs.readFileSync('workers/broadcast-kids-distribution.ts', 'utf8').replace(/\/\*[\s\S]*?\*\/|\/\/.*$/gm, '');
  // THIN: nothing but the app call. No DB, no provider APIs, no selection/routing/metadata/capacity logic.
  for (const banned of [/lib\/ingest\/db/, /\bpg\b/, /supabase/i, /DATABASE_URL/, /BLOTATO/i, /YOUTUBE/i, /blotato\.com/i, /googleapis/i,
    /slot_utc|ready_id|publication_package|binding|cadence|orderQueue|checksum/i, /import /])
    assert.ok(!banned.test(src), 'worker must stay thin: ' + banned);
  assert.ok(!fs.readFileSync('.github/workflows/broadcast-kids-distribution.yml', 'utf8').match(/^\s*(schedule:|- cron:)/m), 'cron must stay commented out');
  assert.ok(!/pull_request/.test(fs.readFileSync('.github/workflows/broadcast-kids-distribution.yml', 'utf8').replace(/#.*$/gm, '')), 'never on pull_request');

  const calls: Array<{ url: string; init: RequestInit }> = [];
  const env = { AIVE_BASE_URL: 'https://app.example/', AIVE_CRON_SECRET: 's3cret' };
  const reply = (body: unknown, status = 200) => (async (url: string, init: RequestInit) => { calls.push({ url, init }); return new Response(typeof body === 'string' ? body : JSON.stringify(body), { status }); }) as unknown as typeof fetch;

  // One POST to the daily-run route with the Bearer secret and the audited timeout.
  const ok = await runKidsDistribution(reply({ today: '2026-10-06', report: [
    { channel: 'space-explorers-club', outcome: 'SUBMITTED', state: 'provider_accepted', detail: 'https://x/y?token=T', slot_id: 'abc' },
    { channel: 'quiz-adventure-club', outcome: 'NOT_YET_DUE', state: null },
    { channel: 'ocean<script>', outcome: 'NO_READY_INVENTORY ', state: 'a b/c' }] }), env);
  assert.equal(calls.length, 1); assert.equal(calls[0].url, 'https://app.example' + ROUTE); assert.equal(calls[0].init.method, 'POST');
  assert.deepEqual(calls[0].init.headers, { Authorization: 'Bearer s3cret' }); assert.ok(calls[0].init.signal instanceof AbortSignal); assert.equal(TIMEOUT_MS, 320_000);
  assert.deepEqual(ok.rows, [{ channel: 'space-explorers-club', outcome: 'SUBMITTED', state: 'provider_accepted' },
    { channel: 'quiz-adventure-club', outcome: 'NOT_YET_DUE', state: null }, { channel: 'oceanscript', outcome: 'NO_READY_INVENTORY', state: 'abc' }]);
  assert.ok(!JSON.stringify(ok).match(/token|https|slot_id|detail/));
  assert.equal(ok.alerts.length, 0);

  // Owner-attention outcomes turn the run red.
  for (const outcome of ALERT_OUTCOMES) assert.equal((await runKidsDistribution(reply({ today: 'd', report: [{ channel: 'c', outcome }] }), env)).alerts.length, 1, outcome);
  for (const outcome of ['SUBMITTED', 'UPLOAD_CONFIRMED', 'PUBLIC_VERIFIED', 'ALREADY_HANDLED_TODAY', 'NOT_YET_DUE', 'DEFERRED_TIME_BUDGET', 'NOT_ENABLED'])
    assert.equal((await runKidsDistribution(reply({ today: 'd', report: [{ channel: 'c', outcome }] }), env)).alerts.length, 0, outcome);

  // HTTP failures (401 auth, 503 not live, 500) and malformed bodies fail without echoing the body.
  for (const status of [401, 503, 500]) await assert.rejects(runKidsDistribution(reply({ error: 'secret detail' }, status), env), new RegExp('^Error: daily-run failed: HTTP ' + status + '$'));
  await assert.rejects(runKidsDistribution(reply('<html>secret</html>'), env), /unparseable JSON$/);
  await assert.rejects(runKidsDistribution(reply({ error: 'x' }), env), /no report$/);
  await assert.rejects(runKidsDistribution(reply({}), { AIVE_BASE_URL: 'x' }), /AIVE_CRON_SECRET is not set/);
  console.log('PASS kids distribution worker: one Bearer POST to the AIVE daily-run route (320s timeout); sanitized channel/outcome/state only; red on owner-attention outcomes; HTTP/malformed failures opaque; thin (no DB, provider or scheduling logic); cron commented out; never on pull_request');
}
main().catch(e => { console.error(e); process.exit(1); });
