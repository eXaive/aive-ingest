/**
 * Verifier: Protocol Watch M1 trial guard (lib/protocolWatch/trialGuard.ts, scripts/protocol-watch-trial-guard.ts,
 * .github/workflows/protocol-watch.yml). Offline: no database, no network.
 * Run: npx tsx scripts/verify-protocol-watch-trial-guard.ts
 */
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { decideTrial, OBSERVATION_SURGE_LIMIT, type TrialRun } from '../lib/protocolWatch/trialGuard';

let n = 0;
const ok = (msg: string) => { n++; console.log(`  ✓ ${msg}`); };
const root = join(__dirname, '..');
const now = new Date('2026-11-07T01:15:00Z');
const until = '2026-11-21T01:00:00Z';
const run = (runKey: string, status: TrialRun['status'], observationsWritten: number, startedAt: string): TrialRun => ({ runKey, status, observationsWritten, startedAt });
// The real first run (Approval C, 2026-11-06): SUCCEEDED, 16 baseline observations.
const approvalC = run('manual:gh-42294496916', 'SUCCEEDED', 16, '2026-11-06T21:10:28.000Z');

console.log('protocol watch trial guard');

// 1. Window: fail-closed.
assert.equal(decideTrial({ now, untilRaw: undefined, runs: [] }).code, 'NO_TRIAL_WINDOW');
assert.equal(decideTrial({ now, untilRaw: '   ', runs: [] }).code, 'NO_TRIAL_WINDOW');
for (const bad of ['2026-11-21', 'tomorrow', '2026-11-21T00:00:00+01:00', '21/11/2026', '2026-13-40T99:99:99Z']) {
  const d = decideTrial({ now, untilRaw: bad, runs: [] });
  assert.equal(d.proceed, false, bad); assert.ok(['INVALID_TRIAL_WINDOW', 'TRIAL_EXPIRED'].includes(d.code), `${bad} -> ${d.code}`);
}
for (const impossible of ['2026-02-30T00:00:00Z', '2026-11-31T00:00:00Z', '2026-11-20T24:00:00Z', '2026-11-20T12:60:00Z', '2026-11-20T12:00:60Z']) {
  assert.equal(decideTrial({ now, untilRaw: impossible, runs: [] }).code, 'INVALID_TRIAL_WINDOW', `${impossible} is rejected, not rolled over`);
}
assert.equal(decideTrial({ now, untilRaw: '2028-02-29T00:00:00Z', runs: [] }).code, 'TRIAL_WINDOW_TOO_LONG', 'a real leap day parses (then the 15-day cap applies)');
assert.equal(decideTrial({ now, untilRaw: '2026-11-21T00:00Z', runs: [] }).proceed, true, 'minute precision still valid');
assert.equal(decideTrial({ now, untilRaw: '2026-11-21T00:00:00.500Z', runs: [] }).proceed, true, 'millisecond precision still valid');
assert.equal(decideTrial({ now, untilRaw: '2026-11-07T01:15:00Z', runs: [] }).code, 'TRIAL_EXPIRED', 'expiry instant itself is expired');
assert.equal(decideTrial({ now, untilRaw: '2026-11-01T00:00:00Z', runs: [] }).code, 'TRIAL_EXPIRED');
assert.equal(decideTrial({ now, untilRaw: '2026-11-23T01:15:01Z', runs: [] }).code, 'TRIAL_WINDOW_TOO_LONG');
assert.equal(decideTrial({ now, untilRaw: until, runs: [] }).proceed, true);
ok('window: missing/invalid/impossible-date/expired/>15 days all skip; inside window proceeds');

// 2. Expiry holds for the whole trial: last slot before the end proceeds, first slot after it skips.
assert.equal(decideTrial({ now: new Date('2026-11-20T13:15:00Z'), untilRaw: until, runs: [approvalC] }).proceed, true);
assert.equal(decideTrial({ now: new Date('2026-11-21T01:15:00Z'), untilRaw: until, runs: [approvalC] }).code, 'TRIAL_EXPIRED');
assert.equal(decideTrial({ now: new Date('2026-12-30T13:15:00Z'), untilRaw: until, runs: [approvalC] }).code, 'TRIAL_EXPIRED', 'late-delivered schedule after expiry still skips');
ok('expiry: automatic, no person or machine needed; late-delivered slots after the end skip');

// 3. Stop rules (latching).
assert.equal(decideTrial({ now, untilRaw: until, runs: [approvalC] }).proceed, true, 'real first run passes');
assert.equal(decideTrial({ now, untilRaw: until, runs: [approvalC, run('schedule:2026-11-07t00', 'FAILED', 0, '2026-11-07T01:16:00Z')] }).code, 'LAST_RUN_FAILED');
assert.equal(decideTrial({ now, untilRaw: until, runs: [approvalC, run('schedule:2026-11-07t00', 'ABANDONED', 0, '2026-11-07T01:16:00Z')] }).code, 'LAST_RUN_FAILED');
assert.equal(decideTrial({ now, untilRaw: until, runs: [approvalC, run('s1', 'PARTIAL', 2, '2026-11-07T01:16:00Z'), run('s2', 'PARTIAL', 1, '2026-11-07T13:16:00Z')] }).code, 'TWO_CONSECUTIVE_PARTIAL');
assert.equal(decideTrial({ now, untilRaw: until, runs: [run('s1', 'PARTIAL', 2, '2026-11-07T01:16:00Z'), run('s2', 'SUCCEEDED', 1, '2026-11-07T13:16:00Z')] }).proceed, true, 'one PARTIAL then success proceeds');
assert.equal(decideTrial({ now, untilRaw: until, runs: [run('s1', 'SUCCEEDED', OBSERVATION_SURGE_LIMIT, '2026-11-07T01:16:00Z')] }).proceed, true, 'exactly 50 proceeds');
assert.equal(decideTrial({ now, untilRaw: until, runs: [run('s1', 'SUCCEEDED', OBSERVATION_SURGE_LIMIT + 1, '2026-11-07T01:16:00Z')] }).code, 'OBSERVATION_SURGE');
assert.equal(decideTrial({ now, untilRaw: until, runs: [approvalC, run('s1', 'RUNNING', 0, '2026-11-07T01:16:00Z')] }).proceed, true, 'a RUNNING receipt is not a finished run');
ok('stop rules: FAILED/ABANDONED, two consecutive PARTIAL, >50 observations; RUNNING ignored');

// 4. Acknowledgement after owner review.
const failed = run('schedule:2026-11-07t00', 'FAILED', 0, '2026-11-07T01:16:00Z');
assert.equal(decideTrial({ now, untilRaw: until, ackRunKey: 'schedule:2026-11-07t00', runs: [approvalC, failed] }).proceed, true, 'reviewed failure acknowledged');
assert.equal(decideTrial({ now, untilRaw: '2026-11-01T00:00:00Z', ackRunKey: 'schedule:2026-11-07t00', runs: [approvalC, failed] }).code, 'TRIAL_EXPIRED', 'ack never extends the window');
assert.equal(decideTrial({ now, untilRaw: until, ackRunKey: 'unknown-key', runs: [approvalC, failed] }).code, 'LAST_RUN_FAILED', 'unknown ack changes nothing');
ok('ack: names a reviewed run; never overrides the window; unknown ack ignored');

// 5. Guard script: fail-closed without a window and never touches the database for window decisions.
const dir = mkdtempSync(join(tmpdir(), 'pw-guard-'));
for (const [label, untilRaw] of [['no window', ''], ['expired', '2020-01-01T00:00:00Z']] as const) {
  const out = join(dir, `out-${label.replace(/\W/g, '')}`);
  const env = { ...process.env, GITHUB_OUTPUT: out, PROTOCOL_WATCH_TRIAL_UNTIL: untilRaw, AIVE_INGEST_DATABASE_URL: '' };
  const r = spawnSync(process.execPath, [join(root, 'node_modules', 'tsx', 'dist', 'cli.mjs'), join(root, 'scripts', 'protocol-watch-trial-guard.ts')], { env, encoding: 'utf8' });
  assert.equal(r.status, 0, r.stderr);
  assert.match(readFileSync(out, 'utf8'), /^proceed=false$/m);
}
const src = readFileSync(join(root, 'scripts', 'protocol-watch-trial-guard.ts'), 'utf8');
assert.ok(src.indexOf("await import('../lib/ingest/db')") > src.indexOf('if (!windowOnly.proceed)'), 'db imported only after the window check');
assert.doesNotMatch(src, /\b(INSERT|UPDATE|DELETE|TRUNCATE)\b/, 'guard never writes');
ok('guard script: proceed=false without a window or after expiry, exit 0, no database connection');

// 6. Workflow wiring.
const wf = readFileSync(join(root, '.github', 'workflows', 'protocol-watch.yml'), 'utf8').replace(/\r/g, '');
const guardAt = wf.indexOf('- name: Trial guard'); const workerAt = wf.indexOf('- name: Protocol watch');
assert.ok(guardAt > 0 && workerAt > guardAt, 'guard step precedes the worker');
assert.match(wf, /- name: Trial guard\n {8}id: guard\n/);
assert.match(wf, /- name: Protocol watch\n {8}if: steps\.guard\.outputs\.proceed == 'true'\n/);
assert.match(wf, /if: \$\{\{ vars\.PROTOCOL_WATCH_ENABLED == 'true' \}\}/, 'job gate unchanged');
assert.equal((wf.match(/cron:/g) ?? []).length, 1); assert.match(wf, /cron: '15 1,13 \* \* \*'/, 'schedule unchanged');
assert.deepEqual([...new Set([...wf.matchAll(/secrets\.([A-Z_]+)/g)].map((m) => m[1]))].sort(), ['AIVE_INGEST_DATABASE_URL', 'GITHUB_TOKEN'], 'no new secret');
ok('workflow: guard before worker, worker conditional on proceed, job gate/schedule/secrets unchanged');

console.log(`protocol watch trial guard: ${n} checks passed`);
