/**
 * verify:publishing-calendar-trigger-health — expected deployment states never alert, every unhealthy state does, the
 * public log carries no details, and the workflow keeps the calendar step first and unchanged.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { summarize } from '../workers/publishing-calendar-trigger-health';

const h = (verdict: string, alert: boolean, extra: Record<string, unknown> = {}) => ({ verdict, alert, credential: 'match', lastRunAt: '2026-11-02T14:08:00.123+00:00', lastDeliveredAt: '2026-11-02T14:08:01+00:00', consecutiveFailures: 0, ...extra });
let checks = 0;
const expect = (status: number, body: unknown, exitCode: number, label: string) => { assert.equal(summarize(status, body as never).exitCode, exitCode, label); checks++; };

// Expected states during and after the release: no alert.
expect(404, null, 0, 'route not deployed yet');
expect(200, { verdict: 'NOT_INSTALLED', alert: false, credential: 'unknown' }, 0, 'migration not applied');
expect(200, h('NOT_ACTIVATED', false, { credential: 'unset' }), 0, 'installed, not activated');
expect(200, h('DEACTIVATED', false), 0, 'deliberately deactivated');
expect(200, h('WARMING_UP', false), 0, 'first hours after activation');
expect(200, h('DELIVERY_FAILED', false, { consecutiveFailures: 1 }), 0, 'one transient failure');
expect(200, h('OK', false), 0, 'healthy');

// Unhealthy: alert, each distinguishable in the log.
for (const v of ['NO_JOB', 'SECRET_MISSING', 'NOT_RUNNING', 'STALE', 'ATTENTION']) {
  const r = summarize(200, h(v, true));
  assert.equal(r.exitCode, 1, v); assert.match(r.lines[0], new RegExp(`verdict=${v} alert=YES`)); assert.equal(r.lines.length, 2); checks++;
}
expect(200, h('DELIVERY_FAILED', true, { consecutiveFailures: 2 }), 1, 'repeated delivery failure');
const drift = summarize(200, h('OK', true, { credential: 'mismatch' }));
assert.equal(drift.exitCode, 1); assert.match(drift.lines[1], /does not match the database \(mismatch\)/); checks++;

// Fail closed on errors and malformed replies.
expect(500, { verdict: 'HEALTH_UNAVAILABLE', alert: true }, 1, 'app could not read health');
expect(401, null, 1, 'machine credential refused');
expect(200, null, 1, 'no body');
expect(200, { verdict: 'OK' }, 1, 'alert missing');
expect(200, { verdict: 'ok; rm -rf', alert: false }, 1, 'unexpected verdict');

// Public log: codes, times and counts only.
const leaky = summarize(200, { verdict: 'STALE', alert: true, credential: 'secret-value-xyz', lastRunAt: '2026-11-02T14:08:00+00:00 token=abc', lastDeliveredAt: 'https://x/?k=1', consecutiveFailures: 'many' as never });
const text = leaky.lines.join('\n');
assert.ok(!/secret-value-xyz|token=abc|https:/.test(text), text);
assert.match(leaky.lines[0], /credential=unknown lastRun=2026-11-02T14:08Z lastHttp200=- consecutiveFailures=0/); checks++;

// Workflow: calendar step first and unchanged; the health step runs after it even if it failed, and only reads.
const wf = readFileSync('.github/workflows/publishing-calendar.yml', 'utf8').replace(/\r/g, '');
const cal = wf.indexOf('- name: Run the publishing calendar'), chk = wf.indexOf('- name: Check the database trigger');
assert.ok(cal > 0 && chk > cal, 'calendar step first, health check after it'); checks++;
assert.match(wf, /- name: Run the publishing calendar\n {8}env:\n {10}AIVE_CRON_SECRET: \$\{\{ secrets\.AIVE_CRON_SECRET \}\}\n {10}AIVE_BASE_URL: \$\{\{ secrets\.AIVE_BASE_URL \}\}\n {10}CALENDAR_DRY_RUN: \$\{\{ inputs\.dry_run \}\}\n {8}run: npx tsx workers\/publishing-calendar\.ts\n/, 'calendar step unchanged'); checks++;
const step = wf.slice(chk, wf.indexOf('run: npx tsx workers/publishing-calendar-trigger-health.ts', chk) + 70);
assert.match(step, /if: \$\{\{ !cancelled\(\) \}\}/); assert.match(step, /AIVE_CRON_SECRET: \$\{\{ secrets\.AIVE_CRON_SECRET \}\}/); assert.ok(!/DRY_RUN/.test(step)); checks++;
assert.ok(readFileSync('workers/publishing-calendar-trigger-health.ts', 'utf8').includes("trigger-health`, { headers:"), 'GET, no method override'); checks++;
assert.ok(readFileSync('.github/workflows/workflow-failure-alert.yml', 'utf8').includes("- 'Publishing Calendar'"), 'failure alert covers the workflow'); checks++;
console.log(`verify-publishing-calendar-trigger-health: OK (${checks} checks)`);
