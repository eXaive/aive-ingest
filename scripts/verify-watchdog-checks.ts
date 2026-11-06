/**
 * Watchdog checks 6-9 (2026-11-06, MCP collection audit 2026-11-05):
 * full-pass freshness, tools/list run state, change-ledger freshness and
 * abandoned scan_runs. Pure evaluators only: no database, no network.
 * The cases replay the exact states found in production on 2026-11-05.
 *
 * Run: npx tsx scripts/verify-watchdog-checks.ts
 */
import assert from 'node:assert/strict';
import {
  judgeCollectionRun, judgeFullPass, judgeChangeLedger, judgeAbandonedScanRuns,
  fullPassRecovering, FULL_PASS_PROGRESS_HOURS, FULL_PASS_RECOVERY_MAX_HOURS, CHANGE_LEDGER_MODE,
  FULL_PASS_STALE_HOURS, CHANGE_LEDGER_STALE_HOURS, SCAN_RUN_ABANDONED_HOURS, SCAN_RUN_LOOKBACK_DAYS,
  TOOL_RUNNING_LIMIT_HOURS, TOOL_STALE_HOURS, PROBE_RUNNING_LIMIT_HOURS, PROBE_STALE_HOURS,
  type CollectionRunRow,
} from '../workers/ingest-watchdog';

const NOW = Date.parse('2026-11-06T00:30:00Z');
const H = 3_600_000;
const ms = (iso: string) => String(Date.parse(iso));

// ── 6. Registry full pass ─────────────────────────────────────────────────────
{
  assert.equal(FULL_PASS_STALE_HOURS, 168);
  // The production row on 2026-11-05: delta since 09-15.
  const stuck = judgeFullPass({ phase: 'delta', last_full_pass_at: '2026-09-15T06:23:09.900Z', updated_at: '2026-11-05T12:56:22.557Z' }, NOW);
  assert.deepEqual(stuck.map((b) => b.kind).sort(), ['REGISTRY_DELTA_PHASE', 'REGISTRY_FULL_PASS_STALE'], 'the 2026-11-05 state raises both');
  const stale = stuck.find((b) => b.kind === 'REGISTRY_FULL_PASS_STALE')!;
  assert.equal(stale.since, new Date(Date.parse('2026-09-15T06:23:09.900Z') + 168 * H).toISOString(), 'onset = pass + limit');
  // Healthy: backfill with a recent pass.
  assert.deepEqual(judgeFullPass({ phase: 'backfill', last_full_pass_at: '2026-11-03T06:00:00Z', updated_at: null }, NOW), []);
  // A multi-day walk in progress is not a fault until the limit passes.
  assert.deepEqual(judgeFullPass({ phase: 'backfill', last_full_pass_at: new Date(NOW - 167 * H).toISOString(), updated_at: null }, NOW), []);
  assert.equal(judgeFullPass({ phase: 'backfill', last_full_pass_at: new Date(NOW - 169 * H).toISOString(), updated_at: null }, NOW)[0].kind, 'REGISTRY_FULL_PASS_STALE');
  // The SQL returns Postgres ::text timestamps ("2026-09-15 06:23:09.9+00"), not ISO; they must parse.
  const pgText = judgeFullPass({ phase: 'backfill', last_full_pass_at: '2026-09-15 06:23:09.9+00', updated_at: '2026-11-05 12:56:22.557714+00' }, NOW);
  assert.deepEqual(pgText.map((b) => b.kind), ['REGISTRY_FULL_PASS_STALE'], 'Postgres text timestamps parse (not misread as NEVER)');
  assert.equal(judgeChangeLedger('2026-09-03 09:41:51.468+00', NOW)!.kind, 'CHANGE_LEDGER_STALE');
  // Never completed / missing row.
  assert.equal(judgeFullPass({ phase: 'backfill', last_full_pass_at: null, updated_at: null }, NOW)[0].kind, 'REGISTRY_FULL_PASS_NEVER');
  assert.equal(judgeFullPass(undefined, NOW)[0].kind, 'REGISTRY_CHECKPOINT_MISSING');
  // Pinned delta is still reported: an operator override stays visible every day it lasts.
  assert.equal(judgeFullPass({ phase: 'delta', last_full_pass_at: new Date(NOW - 2 * H).toISOString(), updated_at: null }, NOW)[0].kind, 'REGISTRY_DELTA_PHASE');
  console.log('  [6] registry full pass: delta phase and a 51-day-old pass are breaches; a walk inside 7 days is not');
}

// ── 6b. Expected recovery vs failure after the re-arm ────────────────────────
{
  const stalePass = { phase: 'backfill', last_full_pass_at: '2026-09-15T06:23:09.900Z', updated_at: new Date(NOW - 2 * H).toISOString() };
  const iso = (hAgo: number) => new Date(NOW - hAgo * H).toISOString();
  // Re-armed 2 days ago, backfill progress 6 h ago: recovering, not a breach.
  assert.equal(fullPassRecovering(stalePass, { rearmedAt: iso(48), lastBackfillProgressAt: iso(6) }, NOW), true);
  assert.deepEqual(judgeFullPass(stalePass, NOW, FULL_PASS_STALE_HOURS, { rearmedAt: iso(48), lastBackfillProgressAt: iso(6) }), [], 'progressing recovery is not red');
  // Stalled: no backfill progress for longer than the progress window → red.
  assert.equal(judgeFullPass(stalePass, NOW, FULL_PASS_STALE_HOURS, { rearmedAt: iso(48), lastBackfillProgressAt: iso(FULL_PASS_PROGRESS_HOURS + 1) })[0].kind, 'REGISTRY_FULL_PASS_STALE', 'stalled recovery is red');
  // Overlong: re-armed more than the recovery window ago, still not complete → red even while progressing.
  assert.equal(judgeFullPass(stalePass, NOW, FULL_PASS_STALE_HOURS, { rearmedAt: iso(FULL_PASS_RECOVERY_MAX_HOURS + 1), lastBackfillProgressAt: iso(2) })[0].kind, 'REGISTRY_FULL_PASS_STALE', 'overlong recovery is red');
  // No re-arm logged: a stale pass is a plain failure.
  assert.equal(judgeFullPass(stalePass, NOW, FULL_PASS_STALE_HOURS, { rearmedAt: null, lastBackfillProgressAt: iso(2) })[0].kind, 'REGISTRY_FULL_PASS_STALE');
  // Still in delta: never "recovering" (that is the stuck state itself).
  const stuck = judgeFullPass({ ...stalePass, phase: 'delta' }, NOW, FULL_PASS_STALE_HOURS, { rearmedAt: iso(48), lastBackfillProgressAt: iso(6) });
  assert.deepEqual(stuck.map((b) => b.kind).sort(), ['REGISTRY_DELTA_PHASE', 'REGISTRY_FULL_PASS_STALE']);
  // Postgres ::text timestamps in the evidence parse too.
  assert.equal(fullPassRecovering(stalePass, { rearmedAt: '2026-11-04 01:00:00+00', lastBackfillProgressAt: '2026-11-05 20:00:00.5+00' }, NOW), true);
  console.log('  [6b] re-arm recovery: progressing = notice; stalled, overlong, unexplained or still-delta = breach');
}
// Change-ledger mode defaults to report-only (detector ships disabled).
assert.equal(CHANGE_LEDGER_MODE, process.env.WATCHDOG_CHANGE_LEDGER?.trim() === 'enforce' ? 'enforce' : 'report');
if (!process.env.WATCHDOG_CHANGE_LEDGER) assert.equal(CHANGE_LEDGER_MODE, 'report', 'default is report-only');

// ── 7. tools/list runs (and probes, through the same judge) ───────────────────
{
  const run = (over: Partial<CollectionRunRow>): CollectionRunRow => ({
    state: 'COMPLETE', started_at: '2026-11-05T16:13:26Z', started_ms: ms('2026-11-05T16:13:26Z'), age_hours: '8.3',
    expected: 38643, persisted: 5461, failed_internal: 12, failure_code: null, ...over,
  });
  const label = { source: 'mcp-tools', prefix: 'TOOL_RUN', noun: 'tools/list collection' };
  const limits = { runningLimitHours: TOOL_RUNNING_LIMIT_HOURS, staleHours: TOOL_STALE_HOURS };
  // The production run on 2026-11-05.
  const partial = judgeCollectionRun(run({ state: 'PARTIAL', failure_code: 'CAPTURE_PERSISTENCE_FAILED' }), label, limits)!;
  assert.equal(partial.kind, 'TOOL_RUN_PARTIAL');
  assert.match(partial.detail, /failure_code=CAPTURE_PERSISTENCE_FAILED/);
  assert.equal(judgeCollectionRun(run({ state: 'FAILED' }), label, limits)!.kind, 'TOOL_RUN_FAILED');
  assert.equal(judgeCollectionRun(run({ state: 'COMPLETE' }), label, limits), null, 'a complete recent run is healthy');
  assert.equal(judgeCollectionRun(run({ state: 'RUNNING', age_hours: '1.0' }), label, limits), null, 'a run in progress is not abandoned');
  assert.equal(judgeCollectionRun(run({ state: 'RUNNING', age_hours: '5.0' }), label, limits)!.kind, 'TOOL_RUN_ABANDONED');
  assert.equal(judgeCollectionRun(run({ age_hours: '49.0' }), label, limits)!.kind, 'TOOL_RUN_STALE');
  assert.equal(judgeCollectionRun(undefined, label, limits)!.kind, 'TOOL_RUN_NEVER');
  // Probe kinds are unchanged by the refactor onto the shared judge.
  const plabel = { source: 'mcp-probes', prefix: 'PROBE_RUN', noun: 'endpoint probe sweep' };
  const plimits = { runningLimitHours: PROBE_RUNNING_LIMIT_HOURS, staleHours: PROBE_STALE_HOURS };
  for (const [state, age, kind] of [['PARTIAL', '1', 'PROBE_RUN_PARTIAL'], ['FAILED', '1', 'PROBE_RUN_FAILED'], ['RUNNING', '4', 'PROBE_RUN_ABANDONED'], ['COMPLETE', '50', 'PROBE_RUN_STALE']] as const) {
    assert.equal(judgeCollectionRun(run({ state, age_hours: age }), plabel, plimits)!.kind, kind);
  }
  assert.equal(judgeCollectionRun(undefined, plabel, plimits)!.kind, 'PROBE_RUN_NEVER');
  console.log('  [7] tools/list: the 2026-11-05 PARTIAL is a breach; probe kinds unchanged through the shared judge');
}

// ── 8. Change ledger ──────────────────────────────────────────────────────────
{
  assert.equal(CHANGE_LEDGER_STALE_HOURS, 48);
  const frozen = judgeChangeLedger('2026-09-03T09:41:51.468Z', NOW)!;
  assert.equal(frozen.kind, 'CHANGE_LEDGER_STALE', 'the frozen 2026-09-03 ledger is a breach');
  assert.equal(judgeChangeLedger(new Date(NOW - 10 * H).toISOString(), NOW), null);
  const denied = judgeChangeLedger('unreadable', NOW, 48, 'permission denied for table mcp_change_events_v1')!;
  assert.equal(denied.kind, 'CHANGE_LEDGER_UNREADABLE', 'no grant is reported, never treated as fresh');
  assert.match(denied.detail, /permission denied/);
  assert.equal(judgeChangeLedger(null, NOW)!.kind, 'CHANGE_LEDGER_EMPTY');
  assert.equal(judgeChangeLedger('not a date', NOW)!.kind, 'CHANGE_LEDGER_UNREADABLE');
  console.log('  [8] change ledger: frozen, unreadable and empty are all breaches; a fresh ledger is not');
}

// ── 9. Abandoned scan_runs ────────────────────────────────────────────────────
{
  assert.equal(SCAN_RUN_ABANDONED_HOURS, 4); assert.equal(SCAN_RUN_LOOKBACK_DAYS, 7);
  const row = (iso: string, status = 'running') => ({ started_at: iso, started_ms: ms(iso), status });
  assert.equal(judgeAbandonedScanRuns([], NOW), null);
  assert.equal(judgeAbandonedScanRuns([row(new Date(NOW - 1 * H).toISOString())], NOW), null, 'a run in its first hours is fine');
  const b = judgeAbandonedScanRuns([row(new Date(NOW - 30 * H).toISOString()), row(new Date(NOW - 5 * H).toISOString()), row(new Date(NOW - 90 * H).toISOString(), 'ok')], NOW)!;
  assert.equal(b.kind, 'SCAN_RUNS_ABANDONED');
  assert.match(b.detail, /^2 scan_runs row\(s\)/, 'only rows still running and past the limit count');
  console.log('  [9] abandoned scan_runs: open rows past 4h inside the 7-day window are a breach');
}

console.log('Watchdog checks 6-9 contract: passed');
