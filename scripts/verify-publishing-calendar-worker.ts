/**
 * verify:publishing-calendar-worker — the public log never carries details, and anything needing attention fails the run.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { summarize } from '../workers/publishing-calendar';

const ok = summarize(200, { paused: false, due: 1, attention: false, results: [{ platform: 'devto', plannedDate: '2026-11-02', outcome: 'PUBLISHED + VERIFIED', attention: false }] });
assert.equal(ok.exitCode, 0);
assert.deepEqual(ok.lines, ['publishing-calendar: paused=false due=1', '  devto 2026-11-02: PUBLISHED + VERIFIED']);

const unknown = summarize(200, { due: 1, attention: true, results: [{ platform: 'tumblr', plannedDate: '2026-11-07', outcome: 'OUTCOME UNKNOWN: platform blocked until owner reconciliation', attention: true }] });
assert.equal(unknown.exitCode, 1);
assert.equal(unknown.lines[1], '  tumblr 2026-11-07: OUTCOME UNKNOWN');

const leaky = summarize(200, { due: 1, attention: true, results: [{ platform: 'wordpress', plannedDate: '2026-11-03', outcome: 'PRECHECK_FAILED: draft unreadable (HTTP_401 secret-token-abc https://x)', attention: true }] });
assert.equal(leaky.lines[1], '  wordpress 2026-11-03: PRECHECK_FAILED', 'only the leading outcome code is logged');
assert.ok(!leaky.lines.join('\n').includes('secret-token-abc'));

assert.equal(summarize(500, { error: 'calendar_run_failed', attention: true }).exitCode, 1);
assert.equal(summarize(401, null).exitCode, 1);
assert.equal(summarize(200, { paused: true, due: 0, attention: false, results: [] }).exitCode, 0);

const workflow = readFileSync('.github/workflows/publishing-calendar.yml', 'utf8');
assert.match(workflow, /^name: Publishing Calendar\r?$/m);
assert.ok(!/^\s*pull_request(_target)?\s*:/m.test(workflow), 'never reachable from forks (no pull_request trigger)');
assert.match(workflow, /AIVE_CRON_SECRET: \$\{\{ secrets\.AIVE_CRON_SECRET \}\}/);
assert.ok(readFileSync('.github/workflows/workflow-failure-alert.yml', 'utf8').includes("- 'Publishing Calendar'"), 'failure alert covers the workflow');
console.log('verify-publishing-calendar-worker: OK (8 checks)');
