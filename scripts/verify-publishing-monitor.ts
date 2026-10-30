/**
 * verify:publishing-monitor — the monitor worker fails exactly on alerts and unusable replies, never on warnings or a
 * not-yet-deployed endpoint; its public log carries no details beyond codes; the workflow only reads, is never reachable
 * from forks, and is covered by the Workflow Failure Alert.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { summarize } from '../workers/publishing-monitor';

let checks = 0;
const exit = (status: number, body: unknown) => summarize(status, body as never).exitCode;

assert.equal(exit(404, null), 0, 'endpoint not deployed yet'); checks++;
assert.equal(exit(200, { verdict: 'OK', alert: false, exceptions: [], warnings: [], spendMonthUsd: 0.4, acceptance: 'waiting' }), 0); checks++;
const warn = summarize(200, { verdict: 'WARN', alert: false, exceptions: [], warnings: [{ code: 'SPEND_WARN' }, { code: 'AUTOPILOT_PAUSED' }], spendMonthUsd: 5.31, acceptance: 'open' });
assert.equal(warn.exitCode, 0, 'warnings never fail the run'); assert.match(warn.lines[0], /verdict=WARN alert=no spendMonth=\$5\.31 acceptance=open warnings=SPEND_WARN,AUTOPILOT_PAUSED/); checks++;
const alert = summarize(200, { verdict: 'ALERT', alert: true, exceptions: [{ code: 'TRIGGER_AUTOPILOT_NOT_RUNNING', detail: 'database trigger health NOT_RUNNING' }, { code: 'ACCEPTANCE_GATE_BREACH', detail: '2 live articles approved: devto 2026-11-09, wordpress 2026-11-10' }], warnings: [], spendMonthUsd: 1, acceptance: 'waiting' });
assert.equal(alert.exitCode, 1); assert.equal(alert.lines.length, 3); assert.match(alert.lines[1], /^  TRIGGER_AUTOPILOT_NOT_RUNNING: /); checks++;
assert.equal(exit(500, { verdict: 'ALERT', alert: true, exceptions: [{ code: 'MONITOR_UNAVAILABLE', detail: 'status could not be read' }], warnings: [] }), 1); checks++;
for (const [s, b] of [[401, { error: 'unauthorized' }], [500, null], [200, null], [200, { verdict: 'OK' }], [200, { verdict: 'fine', alert: false }], [200, { verdict: 'OK', alert: 'no' }]] as const) { assert.equal(exit(s, b), 1, JSON.stringify(b)); checks++; }
// Public log: no URLs, tokens or free text beyond the safe set; unknown codes are dropped.
const leaky = summarize(200, { verdict: 'ALERT', alert: true, exceptions: [{ code: 'RECEIPT_MISSING', detail: 'https://x.test/?token=abc <script>' }, { code: 'not a code', detail: 'x' }], warnings: [{ code: 'secret=1' }], spendMonthUsd: 2, acceptance: '<b>' });
const text = leaky.lines.join('\n');
assert.ok(!/https:\/\/|token=abc|<script>|secret=1|<b>/.test(text), text); assert.equal(leaky.lines.length, 2); checks++;

const wf = readFileSync('.github/workflows/publishing-monitor.yml', 'utf8').replace(/\r/g, '');
assert.match(wf, /^name: Publishing Monitor$/m); assert.ok(!/^\s*pull_request(_target)?\s*:/m.test(wf), 'never reachable from forks'); checks++;
assert.match(wf, /cron: '52 \* \* \* \*'/); assert.match(wf, /permissions:\n {2}contents: read/); checks++;
assert.match(wf, /run: npx tsx workers\/publishing-monitor\.ts/); assert.ok(!/DRY_RUN|publishing-calendar\.ts|publishing-autopilot\.ts/.test(wf), 'only the monitor runs here'); checks++;
const worker = readFileSync('workers/publishing-monitor.ts', 'utf8');
assert.ok(worker.includes('/api/publishing/monitor`, { headers:') && !/method:/.test(worker), 'a GET, nothing else'); checks++;
assert.ok(readFileSync('.github/workflows/workflow-failure-alert.yml', 'utf8').includes("- 'Publishing Monitor'"), 'failure alert covers the workflow'); checks++;
console.log(`verify-publishing-monitor: OK (${checks} checks)`);
