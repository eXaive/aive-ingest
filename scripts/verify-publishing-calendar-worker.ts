/**
 * verify:publishing-calendar-worker — the public log never carries details, and anything needing attention fails the run.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { summarize } from '../workers/publishing-calendar';
import { summarize as summarizeAutopilot } from '../workers/publishing-autopilot';

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

// Autopilot step: state codes only in the public log; held articles and uncertain outcomes turn the run red.
const auto = summarizeAutopilot(200, { planned: 2, attention: true, stages: [
  { platform: 'devto', slot: '2026-11-09', from: 'GENERATED', to: 'VERIFIED', attention: false, detail: '' },
  { platform: 'tumblr', slot: '2026-11-14', from: 'GENERATED', to: 'HELD', attention: true, detail: 'GATES_FAILED: QUOTE_NOT_IN_EVIDENCE (secret-token-abc https://x)' }] });
assert.equal(auto.exitCode, 1);
assert.deepEqual(auto.lines, ['publishing-autopilot: planned=2 stages=2', '  devto 2026-11-09: GENERATED -> VERIFIED', '  tumblr 2026-11-14: GENERATED -> HELD GATES_FAILED']);
assert.ok(!auto.lines.join('\n').includes('secret-token-abc') && !auto.lines.join('\n').includes('QUOTE_NOT'), 'only the leading reason code is logged');
assert.equal(summarizeAutopilot(200, { planned: 0, stages: [], attention: false }).exitCode, 0);
assert.equal(summarizeAutopilot(200, { dryRun: true, paused: true, counts: { SCHEDULED: 2, 'bad key <x>': 1 }, attention: false }).lines[0], 'publishing-autopilot: dry run paused=true SCHEDULED=2');
assert.equal(summarizeAutopilot(500, { error: 'autopilot_tick_failed', attention: true }).exitCode, 1);
assert.match(workflow, /run: npx tsx workers\/publishing-autopilot\.ts/);
assert.ok(workflow.indexOf('publishing-autopilot.ts') < workflow.indexOf('publishing-calendar.ts'), 'autopilot runs before the calendar step');
assert.match(workflow, /- name: Run the publishing calendar\r?\n\s+if: \$\{\{ !cancelled\(\) \}\}/, 'the calendar step runs even if the autopilot step fails');
console.log('verify-publishing-calendar-worker: OK (17 checks)');
