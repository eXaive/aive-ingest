/**
 * Ingest watchdog — judges the DATA, not the run.
 *
 * WHY THIS EXISTS. Every other alert in this repo is the same shape: a worker
 * notices a problem and exits non-zero, and the red Actions run is the alert.
 * That shape has one blind spot it cannot see past — it requires the worker to
 * run. A disabled workflow, a cron that stops dispatching, a runner that never
 * starts, a repo gone read-only: each of those produces NO red run, no log, no
 * signal at all. A silent ingest and a healthy quiet day look identical.
 *
 * So this worker asks a question no ingest worker can ask about itself: is the
 * data still arriving? It reads the database and nothing else. It shares no
 * code path, no schedule and no failure mode with the ingest workers, which is
 * the entire point — a watchdog that fails when the thing it watches fails is
 * a second copy of the problem.
 *
 * IT ALSO CARRIES THE OTHER HALF OF THE EXIT-CODE CHANGE. As of 2026-09-02 a
 * cache-refresh failure no longer paints the daily registry run red (see
 * exitCodeForRegistryOutcome). That is only defensible because REPEATED cache
 * failure is caught here instead. Removing this file silently un-does that
 * decision and leaves cache failures with no alarm at all.
 *
 * WHAT IT CHECKS
 *   1. INGESTION GAP  — hours since the newest SUCCESSFUL ingestion_log row per
 *      source. A failed attempt is not an ingestion: on 2026-10-05/06 two
 *      transport failures each wrote a row and kept this check quiet while no
 *      registry data arrived.
 *   2. FAILED RUNS — the newest run failing is reported at once
 *      (LATEST_RUN_FAILED); an unbroken streak of CONSECUTIVE is escalated.
 *   3. CACHE REFRESH — consecutive cache-refresh failures recorded in the
 *      registry runs' metadata, the signal moved off the ingest exit code.
 *   4. CACHE AGE — each cache's own age against its own SLA. Checks 1-3 read
 *      run history and so cannot flag anything before N runs have happened;
 *      this one is true on day one and true when nothing runs at all.
 *   5. ENDPOINT PROBES — the newest mcp_probe_runs row: PARTIAL / FAILED, a run
 *      still RUNNING past its job limit, or no run within PROBE_STALE_HOURS.
 *   6. REGISTRY FULL PASS (2026-11-06) — ingest_checkpoints: a checkpoint in
 *      'delta' (valid only under an operator pin), or last_full_pass_at older
 *      than FULL_PASS_STALE_HOURS. Every run was green for 51 days while the
 *      walk was stuck in delta; this is the check that would have said so.
 *   7. TOOLS/LIST COLLECTION (2026-11-06) — the newest mcp_tool_collection_runs
 *      row, judged exactly like probe runs (check 5, same judgeCollectionRun).
 *      2026-11-05 ended PARTIAL (connection limit) and nothing here noticed.
 *   8. CHANGE LEDGER (2026-11-06) — newest mcp_change_events_v1.detected_at
 *      older than CHANGE_LEDGER_STALE_HOURS. The ledger has had no writer
 *      since its 2026-09-04 backfill. Unreadable (no SELECT grant yet) is
 *      reported as CHANGE_LEDGER_UNREADABLE, never as healthy.
 *   9. ABANDONED SCAN RUNS (2026-11-06) — scan_runs rows still 'running' more
 *      than SCAN_RUN_ABANDONED_HOURS after they started, within the last
 *      SCAN_RUN_LOOKBACK_DAYS (older ones are history, not a live fault).
 *
 * Every breach carries an onset. Breaches whose onset falls inside
 * NEW_WITHIN_HOURS print as NEW, older ones as ONGOING, so a condition that
 * has been red for a week cannot hide a failure that started today.
 *
 * Read-only by construction: the role holds SELECT and no write grant on
 * ingestion_log, so this cannot record its own all-clear.
 *
 * Exit 0 = everything within limits. Exit 1 = at least one breach, detail on
 * stdout. THIS REPO IS PUBLIC: print source slugs, counts and ages, never a
 * row body or a connection string.
 *
 * Env (all optional; documented defaults):
 *   WATCHDOG_GAP_HOURS            default 24
 *   WATCHDOG_CONSECUTIVE_FAILURES default 3
 *   WATCHDOG_SOURCES              comma-separated slugs; default mcp-registry
 *   WATCHDOG_FULL_PASS_HOURS      default 168 (FULL_PASS_STALE_HOURS)
 *   WATCHDOG_CHANGE_LEDGER_HOURS  default 48  (CHANGE_LEDGER_STALE_HOURS)
 *   WATCHDOG_CHANGE_LEDGER        'enforce' makes check 8 a breach; anything else (default) reports it as a notice,
 *                                 because the change detector ships DISABLED and a stale ledger is then expected.
 *
 * EXPECTED RECOVERY vs FAILURE (2026-11-06). Two conditions are expected for a while after the repairs ship and must
 * not page anyone, but must turn red if they stop being temporary:
 *   - REGISTRY RE-ARM: after the delta checkpoint re-arms (logged phase_decision *_rearm), the first full walk spans
 *     several runs and last_full_pass_at stays old. That is reported as RECOVERING (notice) only while a re-arm was
 *     logged within FULL_PASS_RECOVERY_MAX_HOURS AND a successful backfill run committed progress within
 *     FULL_PASS_PROGRESS_HOURS. A stalled or overlong recovery is the normal REGISTRY_FULL_PASS_STALE breach.
 *   - CHANGE LEDGER: report-only until the owner enables the detector and sets WATCHDOG_CHANGE_LEDGER=enforce.
 */

import { q, endIngestPool } from '../lib/ingest/db';
import { parseLimit } from '../lib/ingest/parseLimit';

const DEFAULT_SOURCES = ['mcp-registry'];

function readLimit(name: string, fallback: number): number {
  const parsed = parseLimit(process.env[name], fallback);
  if (!parsed.ok) {
    /* Fail CLOSED, unlike the ingest worker's freshness window. There a bad
       value skews a report; here it would widen the window in which an outage
       goes unreported, and a watchdog that quietly relaxes its own threshold
       is worse than no watchdog. */
    console.error(`[ingest-watchdog] ${name} is unreadable: ${parsed.problem} — refusing to run on a guessed threshold`);
    process.exit(1);
  }
  return parsed.value ?? fallback;
}

const GAP_HOURS = readLimit('WATCHDOG_GAP_HOURS', 24);
const CONSECUTIVE = readLimit('WATCHDOG_CONSECUTIVE_FAILURES', 3);
const SOURCES = (process.env.WATCHDOG_SOURCES?.trim() || DEFAULT_SOURCES.join(','))
  .split(',').map((s) => s.trim()).filter(Boolean);

interface Breach { source: string; kind: string; detail: string; /** ISO onset; null when it cannot be dated. */ since: string | null }

/** The watchdog runs daily but GitHub drifts scheduled dispatch by up to ~8h, so
 *  consecutive runs can sit ~32h apart. Anything that began inside this window
 *  may not have been reported by the previous run. */
export const NEW_WITHIN_HOURS = 36;
/** Daily probe sweep with the same dispatch drift; matches the AIVE reachability panel's 48h. */
export const PROBE_STALE_HOURS = 48;
/** probe-mcp-endpoints.yml timeout-minutes is 120; RUNNING beyond that means the job died mid-sweep. */
export const PROBE_RUNNING_LIMIT_HOURS = 3;
/** A checkpointed full walk spans several daily runs once the corpus outgrows one 20-minute budget
 *  (about 1,059 pages on 2026-09-15, and growing). 7 days leaves room for that while catching any
 *  walk that has stopped completing; the stuck state on 2026-11-05 was 51 days old. */
export const FULL_PASS_STALE_HOURS = readLimit('WATCHDOG_FULL_PASS_HOURS', 168);
/** collect-mcp-tools.yml timeout-minutes is 210 for all three list collectors; the tools run alone is
 *  ~10-30 min. RUNNING past 4h means the job died with the row open. */
export const TOOL_RUNNING_LIMIT_HOURS = 4;
/** Daily tools sweep with the same dispatch drift as probes. */
export const TOOL_STALE_HOURS = 48;
/** 2 x the daily cadence a live detector would run at. */
export const CHANGE_LEDGER_STALE_HOURS = readLimit('WATCHDOG_CHANGE_LEDGER_HOURS', 48);
/** 'enforce' only when the owner has enabled the change detector; see the header. */
export const CHANGE_LEDGER_MODE: 'enforce' | 'report' = process.env.WATCHDOG_CHANGE_LEDGER?.trim() === 'enforce' ? 'enforce' : 'report';
/** Longest MCP job limit (discover, 180 min) plus slack. */
export const SCAN_RUN_ABANDONED_HOURS = 4;
/** Rows older than this are history (23 such rows, 2026-08-03..10-28, exist today), not a live fault. */
export const SCAN_RUN_LOOKBACK_DAYS = 7;

export interface CollectionRunRow {
  state: string; started_at: string; started_ms: string; age_hours: string;
  expected: number | null; persisted: number | null; failed_internal: number | null; failure_code: string | null;
}

/** Pure judgement of the newest row of a *_runs table (probes, tools/list). null = within limits. */
export function judgeCollectionRun(
  run: CollectionRunRow | undefined,
  label: { source: string; prefix: string; noun: string },
  limits: { runningLimitHours: number; staleHours: number },
): Breach | null {
  if (!run) return { source: label.source, kind: `${label.prefix}_NEVER`, detail: `no ${label.noun} has been recorded`, since: null };
  const age = Number(run.age_hours);
  const tally = `persisted ${run.persisted ?? '?'} of ${run.expected ?? '?'} endpoints, ${run.failed_internal ?? 0} failed internally${run.failure_code ? `, failure_code=${run.failure_code}` : ''}`;
  if (run.state === 'PARTIAL' || run.state === 'FAILED') {
    return { source: label.source, kind: `${label.prefix}_${run.state}`, detail: `newest ${label.noun} started ${run.started_at} ended ${run.state}: ${tally}`, since: isoAt(Number(run.started_ms)) };
  }
  if (run.state === 'RUNNING' && age > limits.runningLimitHours) {
    return { source: label.source, kind: `${label.prefix}_ABANDONED`, detail: `newest ${label.noun} started ${run.started_at} is still RUNNING after ${age}h (limit ${limits.runningLimitHours}h): ${tally}`, since: isoAt(Number(run.started_ms) + limits.runningLimitHours * HOUR_MS) };
  }
  if (age > limits.staleHours) {
    return { source: label.source, kind: `${label.prefix}_STALE`, detail: `newest ${label.noun} started ${run.started_at}, ${age}h ago (limit ${limits.staleHours}h)`, since: isoAt(Number(run.started_ms) + limits.staleHours * HOUR_MS) };
  }
  return null;
}

/** A recovery is allowed this long after the logged re-arm (at ~1,000-1,600 pages and 20 min/day, ~3-6 days needed). */
export const FULL_PASS_RECOVERY_MAX_HOURS = 240;
/** A recovering walk must have committed a successful backfill run this recently (daily cron plus ~8 h drift). */
export const FULL_PASS_PROGRESS_HOURS = 32;

/** Evidence of an in-progress recovery, read from ingestion_log. null fields = no such evidence. */
export interface FullPassRecoveryEvidence { rearmedAt: string | null; lastBackfillProgressAt: string | null }

/** True when a stale full pass is an expected, progressing recovery rather than a failure. Pure. */
export function fullPassRecovering(
  cp: { phase: string } | undefined, evidence: FullPassRecoveryEvidence | undefined, nowMs: number,
): boolean {
  if (!cp || cp.phase !== 'backfill' || !evidence?.rearmedAt || !evidence.lastBackfillProgressAt) return false;
  const rearm = Date.parse(evidence.rearmedAt), progress = Date.parse(evidence.lastBackfillProgressAt);
  if (!Number.isFinite(rearm) || !Number.isFinite(progress)) return false;
  return nowMs - rearm <= FULL_PASS_RECOVERY_MAX_HOURS * HOUR_MS && nowMs - progress <= FULL_PASS_PROGRESS_HOURS * HOUR_MS;
}

/** Pure judgement of the registry checkpoint row. Both conditions can hold at once. */
export function judgeFullPass(
  cp: { phase: string; last_full_pass_at: string | null; updated_at: string | null } | undefined,
  nowMs: number,
  staleHours = FULL_PASS_STALE_HOURS,
  recovery?: FullPassRecoveryEvidence,
): Breach[] {
  if (!cp) return [{ source: 'mcp-registry', kind: 'REGISTRY_CHECKPOINT_MISSING', detail: 'no ingest_checkpoints row for mcp-registry', since: null }];
  const out: Breach[] = [];
  if (cp.phase === 'delta') {
    out.push({ source: 'mcp-registry', kind: 'REGISTRY_DELTA_PHASE',
      detail: `checkpoint is in delta: valid only under an operator pin (MCP_REGISTRY_DELTA_PIN_UNTIL); last_seen stops meaning "confirmed in a full walk"`,
      since: cp.updated_at });
  }
  const last = cp.last_full_pass_at ? Date.parse(cp.last_full_pass_at) : NaN;
  if (!Number.isFinite(last)) {
    out.push({ source: 'mcp-registry', kind: 'REGISTRY_FULL_PASS_NEVER', detail: 'no completed full registry walk is recorded', since: null });
  } else if (nowMs - last > staleHours * HOUR_MS && !fullPassRecovering(cp, recovery, nowMs)) {
    out.push({ source: 'mcp-registry', kind: 'REGISTRY_FULL_PASS_STALE',
      detail: `last completed full walk ${cp.last_full_pass_at}, ${((nowMs - last) / HOUR_MS).toFixed(1)}h ago (limit ${staleHours}h)`,
      since: isoAt(last + staleHours * HOUR_MS) });
  }
  return out;
}

/** Pure judgement of change-ledger freshness. 'unreadable' is its own breach, never treated as fresh. */
export function judgeChangeLedger(latest: string | null | 'unreadable', nowMs: number, staleHours = CHANGE_LEDGER_STALE_HOURS, problem = ''): Breach | null {
  if (latest === 'unreadable') return { source: 'mcp-changes', kind: 'CHANGE_LEDGER_UNREADABLE', detail: `mcp_change_events_v1 cannot be read by this role${problem ? `: ${problem}` : ''}; ledger freshness is unknown`, since: null };
  if (latest === null) return { source: 'mcp-changes', kind: 'CHANGE_LEDGER_EMPTY', detail: 'mcp_change_events_v1 holds no detected changes', since: null };
  const at = Date.parse(latest);
  if (!Number.isFinite(at)) return { source: 'mcp-changes', kind: 'CHANGE_LEDGER_UNREADABLE', detail: `newest detected_at is not a time: ${latest}`, since: null };
  if (nowMs - at > staleHours * HOUR_MS) {
    return { source: 'mcp-changes', kind: 'CHANGE_LEDGER_STALE', detail: `newest change detected ${latest}, ${((nowMs - at) / HOUR_MS).toFixed(1)}h ago (limit ${staleHours}h): registry status changes after it are not recorded`, since: isoAt(at + staleHours * HOUR_MS) };
  }
  return null;
}

/** Pure judgement of scan_runs still open past the abandon limit (rows already filtered to the lookback window). */
export function judgeAbandonedScanRuns(rows: readonly { started_at: string; started_ms: string; status: string | null }[], nowMs: number, limitHours = SCAN_RUN_ABANDONED_HOURS): Breach | null {
  const open = rows.filter((r) => r.status === 'running' && nowMs - Number(r.started_ms) > limitHours * HOUR_MS);
  if (open.length === 0) return null;
  const oldest = open.reduce((a, b) => (Number(a.started_ms) <= Number(b.started_ms) ? a : b));
  return { source: 'scan-runs', kind: 'SCAN_RUNS_ABANDONED',
    detail: `${open.length} scan_runs row(s) still 'running' more than ${limitHours}h after start (oldest ${oldest.started_at}): a job died without closing its row`,
    since: isoAt(Number(oldest.started_ms) + limitHours * HOUR_MS) };
}

const HOUR_MS = 3_600_000;
const isoAt = (ms: number): string => new Date(ms).toISOString();

/** NEW when the onset is inside the window (or unknown — an undatable fault is
 *  never allowed to pass as old news); ONGOING otherwise. */
export function breachAge(since: string | null, nowMs: number): 'NEW' | 'ONGOING' {
  if (since === null) return 'NEW';
  const at = Date.parse(since);
  if (!Number.isFinite(at)) return 'NEW';
  return nowMs - at <= NEW_WITHIN_HOURS * HOUR_MS ? 'NEW' : 'ONGOING';
}

/** Index (newest-first) of the run at which an unbroken failure streak first
 *  reached the threshold, i.e. the onset of the escalated condition. */
/** Newest-first refresh statuses -> row indices of the unbroken FAILED streak.
 *  SUCCEEDED ends it. SKIPPED / unknown is not evidence either way, so it is
 *  stepped over rather than treated as a reset: on 2026-10-05/06 two
 *  transport-failed ingests recorded SKIPPED and silently cleared a dashboard
 *  refresh that had failed on every evaluated run since 2026-10-02. */
export function cacheFailureStreak(statuses: readonly (string | null)[]): number[] {
  const failed: number[] = [];
  for (let i = 0; i < statuses.length; i++) {
    if (statuses[i] === 'SUCCEEDED') break;
    if (statuses[i] === 'FAILED') failed.push(i);
  }
  return failed;
}

export function streakOnsetIndex(streak: number, threshold: number): number | null {
  return streak >= threshold ? streak - threshold : null;
}

async function main(): Promise<void> {
  const breaches: Breach[] = [];

  /* An empty read is ambiguous: it means "no ingestion has EVER been logged"
     or "RLS is returning zero rows to this role". The second has happened
     twice in this schema and would make the watchdog cry outage on a healthy
     system, so it is treated as a configuration fault and named as one rather
     than reported as a data outage. */
  const [{ total }] = await q<{ total: string }>('SELECT count(*)::text AS total FROM ingestion_log');
  if (Number(total) === 0) {
    console.error(
      '[ingest-watchdog] ingestion_log reads 0 rows for this role. Either nothing has ever been logged, ' +
      'or the SELECT grant has no matching RLS policy and every read is silently empty. ' +
      'Not reporting an outage on an unreadable table.',
    );
    await endIngestPool();
    process.exit(1);
  }
  console.log(`[ingest-watchdog] ingestion_log readable (${total} rows) — thresholds: gap>${GAP_HOURS}h, consecutive failures>=${CONSECUTIVE}`);

  for (const source of SOURCES) {
    // ── 1. Gap since the last logged ingestion ──────────────────────────────
    const gapRows = await q<{ age_hours: string | null; last_at: string | null; last_ms: string | null }>(
      `SELECT round(extract(epoch FROM (now() - max(started_at))) / 3600.0, 1)::text AS age_hours,
              max(started_at)::text AS last_at,
              (extract(epoch FROM max(started_at)) * 1000)::bigint::text AS last_ms
         FROM ingestion_log WHERE source_slug = $1 AND error_message IS NULL`,
      [source],
    );
    const ageHours = gapRows[0]?.age_hours === null || gapRows[0]?.age_hours === undefined
      ? null : Number(gapRows[0].age_hours);

    if (ageHours === null) {
      breaches.push({ source, kind: 'NO_INGESTION_EVER', detail: 'no successful ingestion_log row has ever been written for this source', since: null });
    } else if (ageHours > GAP_HOURS) {
      breaches.push({
        source, kind: 'INGESTION_GAP',
        detail: `${ageHours}h since the last successful ingestion (limit ${GAP_HOURS}h) — last success at ${gapRows[0].last_at}`,
        since: isoAt(Number(gapRows[0].last_ms) + GAP_HOURS * HOUR_MS),
      });
    } else {
      console.log(`[ingest-watchdog] ${source}: last successful ingestion ${ageHours}h ago — within ${GAP_HOURS}h`);
    }

    // ── 2. Consecutive failed runs ──────────────────────────────────────────
    /* Newest-first, counting the unbroken prefix that carries an error. A run
       that succeeded ends the streak; older failures behind it are history,
       not an ongoing outage. */
    const recent = await q<{ failed: boolean; started_at: string; started_ms: string }>(
      `SELECT (error_message IS NOT NULL) AS failed, started_at::text,
              (extract(epoch FROM started_at) * 1000)::bigint::text AS started_ms
         FROM ingestion_log WHERE source_slug = $1
        ORDER BY started_at DESC LIMIT $2`,
      [source, Math.max(CONSECUTIVE, 10)],
    );
    let streak = 0;
    for (const row of recent) { if (!row.failed) break; streak++; }
    if (streak >= CONSECUTIVE) {
      breaches.push({
        source, kind: 'CONSECUTIVE_FAILURES',
        detail: `${streak} consecutive failed run(s), oldest in the streak at ${recent[streak - 1]?.started_at}`,
        since: isoAt(Number(recent[streakOnsetIndex(streak, CONSECUTIVE) ?? 0].started_ms)),
      });
    } else if (streak > 0) {
      /* Below the escalation threshold is still a failure: the registry runs
         once a day, so waiting for three would hide it for three days. */
      breaches.push({
        source, kind: 'LATEST_RUN_FAILED',
        detail: `newest run failed at ${recent[0].started_at} (${streak} in a row; escalates at ${CONSECUTIVE})`,
        since: isoAt(Number(recent[0].started_ms)),
      });
    } else {
      console.log(`[ingest-watchdog] ${source}: newest run carries no error`);
    }

    // ── 3. Consecutive cache-refresh failures ───────────────────────────────
    /* The signal that moved off the registry exit code. Reads the outcome the
       worker recorded in metadata; runs that never evaluated a cache (SKIPPED,
       or a null from the un-evaluated fix) are not counted as failures -- that
       conflation is the bug this pass removed, and re-introducing it here
       would put it straight back. */
    const cacheRows = await q<{ dash: string | null; reach: string | null; started_at: string; started_ms: string }>(
      `SELECT metadata->>'dashboard_refresh_status' AS dash,
              metadata->>'reachability_refresh_status' AS reach,
              started_at::text,
              (extract(epoch FROM started_at) * 1000)::bigint::text AS started_ms
         FROM ingestion_log
        WHERE source_slug = $1 AND metadata ? 'dashboard_refresh_status'
        ORDER BY started_at DESC LIMIT $2`,
      [source, Math.max(CONSECUTIVE, 10)],
    );
    for (const component of ['dash', 'reach'] as const) {
      const failedAt = cacheFailureStreak(cacheRows.map((row) => row[component]));
      const cacheStreak = failedAt.length;
      if (cacheStreak >= CONSECUTIVE) {
        breaches.push({
          source, kind: 'CACHE_REFRESH_FAILURES',
          detail: `${component === 'dash' ? 'dashboard' : 'reachability'} cache refresh failed ${cacheStreak} run(s) in a row — the surface is serving stale figures`,
          since: isoAt(Number(cacheRows[failedAt[streakOnsetIndex(cacheStreak, CONSECUTIVE) ?? 0]].started_ms)),
        });
      }
    }
  }

  // ── 4. Cache age against its own SLA ──────────────────────────────────────
  /* Checks 1-3 all read run HISTORY, so the soonest they can call a stale
     cache is after CONSECUTIVE runs -- three days at a daily cadence. This
     asks the cache directly how old it is, which is true on day one and stays
     true when no run happens at all. It is the check that actually pays for
     moving cache failures off the ingest exit code; the streak check alone
     would have traded a same-day red run for a three-day delay. */
  try {
    const healthRows = await q<{ health: {
      dashboard?: { age_hours: number | null; sla_hours: number; status: string } | null;
      reachability?: { age_hours: number | null; sla_hours: number; status: string } | null;
    } | null }>('SELECT public.mcp_dashboard_refresh_health() AS health');
    const health = healthRows[0]?.health ?? null;

    if (!health) {
      breaches.push({ source: 'mcp-cache', kind: 'CACHE_HEALTH_UNREADABLE', detail: 'mcp_dashboard_refresh_health() returned nothing — cache state cannot be judged', since: null });
    } else {
      for (const name of ['dashboard', 'reachability'] as const) {
        const c = health[name];
        if (!c) {
          breaches.push({ source: 'mcp-cache', kind: 'CACHE_NEVER_COMPUTED', detail: `${name} cache has no recorded computation`, since: null });
          continue;
        }
        if (c.age_hours === null) {
          breaches.push({ source: 'mcp-cache', kind: 'CACHE_NEVER_COMPUTED', detail: `${name} cache reports no age`, since: null });
        } else if (c.age_hours > c.sla_hours) {
          breaches.push({
            source: 'mcp-cache', kind: 'CACHE_STALE',
            detail: `${name} cache is ${c.age_hours.toFixed(1)}h old against a ${c.sla_hours}h SLA (last status ${c.status})`,
            since: isoAt(Date.now() - (c.age_hours - c.sla_hours) * HOUR_MS),
          });
        } else {
          console.log(`[ingest-watchdog] ${name} cache ${c.age_hours.toFixed(1)}h old — within its ${c.sla_hours}h SLA (${c.status})`);
        }
      }
    }
  } catch (e) {
    /* Unreadable is NOT healthy. Swallowing this would make the one check that
       does not depend on run history disappear exactly when the database is
       the thing having trouble. */
    breaches.push({
      source: 'mcp-cache', kind: 'CACHE_HEALTH_UNREADABLE',
      detail: `cache health read failed: ${e instanceof Error ? e.message : String(e)}`,
      since: null,
    });
  }

  // ── 5. Endpoint probe sweep ────────────────────────────────────────────────
  /* A sweep that persists most but not all endpoints ends PARTIAL and its own
     run is red once, on the day; nothing here looked at it, so the next green
     day erased it. The newest run's own recorded state is the evidence. */
  try {
    const runs = await q<{
      state: string; started_at: string; started_ms: string; age_hours: string;
      expected: number | null; persisted: number | null; failed_internal: number | null; failure_code: string | null;
    }>(
      `SELECT state, started_at::text,
              (extract(epoch FROM started_at) * 1000)::bigint::text AS started_ms,
              round(extract(epoch FROM (now() - started_at)) / 3600.0, 1)::text AS age_hours,
              expected_endpoint_count AS expected, persisted_endpoint_count AS persisted,
              failed_internal_count AS failed_internal, failure_code
         FROM mcp_probe_runs ORDER BY started_at DESC LIMIT 1`,
    );
    const run = runs[0];
    const breach = judgeCollectionRun(run, { source: 'mcp-probes', prefix: 'PROBE_RUN', noun: 'endpoint probe sweep' },
      { runningLimitHours: PROBE_RUNNING_LIMIT_HOURS, staleHours: PROBE_STALE_HOURS });
    if (breach) breaches.push(breach);
    else console.log(`[ingest-watchdog] mcp-probes: newest sweep ${run!.state} ${run!.age_hours}h ago — persisted ${run!.persisted ?? '?'} of ${run!.expected ?? '?'}`);
  } catch (e) {
    breaches.push({
      source: 'mcp-probes', kind: 'PROBE_RUNS_UNREADABLE',
      detail: `probe run read failed: ${e instanceof Error ? e.message : String(e)}`,
      since: null,
    });
  }

  // ── 6. Registry full-pass freshness ─────────────────────────────────────────
  try {
    const cps = await q<{ phase: string; last_full_pass_at: string | null; updated_at: string | null }>(
      `SELECT phase, last_full_pass_at::text, updated_at::text FROM ingest_checkpoints WHERE source = 'mcp-registry'`,
    );
    const ev = await q<{ rearmed_at: string | null; progress_at: string | null }>(
      `SELECT (SELECT max(started_at)::text FROM ingestion_log
                 WHERE source_slug = 'mcp-registry' AND metadata->>'phase_decision' LIKE 'delta%rearm') AS rearmed_at,
               (SELECT max(started_at)::text FROM ingestion_log
                 WHERE source_slug = 'mcp-registry' AND error_message IS NULL AND metadata->>'kind' = 'run'
                   AND metadata->>'phase_before' = 'backfill' AND metadata->>'stop' IN ('budget', 'complete')) AS progress_at`,
    );
    const evidence: FullPassRecoveryEvidence = { rearmedAt: ev[0]?.rearmed_at ?? null, lastBackfillProgressAt: ev[0]?.progress_at ?? null };
    const nowMs = Date.now();
    const found = judgeFullPass(cps[0], nowMs, FULL_PASS_STALE_HOURS, evidence);
    breaches.push(...found);
    if (fullPassRecovering(cps[0], evidence, nowMs) && cps[0].last_full_pass_at && nowMs - Date.parse(cps[0].last_full_pass_at) > FULL_PASS_STALE_HOURS * HOUR_MS) {
      const msg = `full walk RECOVERING after re-arm at ${evidence.rearmedAt}; last progress ${evidence.lastBackfillProgressAt}; last completed pass ${cps[0].last_full_pass_at} (red if no progress for ${FULL_PASS_PROGRESS_HOURS}h or not done by re-arm + ${FULL_PASS_RECOVERY_MAX_HOURS}h)`;
      console.log(`[ingest-watchdog] mcp-registry: ${msg}`);
      console.log(`::notice title=Registry full walk recovering::${msg}`);
    } else if (found.length === 0) {
      console.log(`[ingest-watchdog] mcp-registry: phase ${cps[0].phase}, last full pass ${cps[0].last_full_pass_at} — within ${FULL_PASS_STALE_HOURS}h`);
    }
  } catch (e) {
    breaches.push({ source: 'mcp-registry', kind: 'REGISTRY_CHECKPOINT_UNREADABLE', detail: `checkpoint read failed: ${e instanceof Error ? e.message : String(e)}`, since: null });
  }

  // ── 7. tools/list collection runs ───────────────────────────────────────────
  try {
    const runs = await q<CollectionRunRow>(
      `SELECT state, started_at::text,
              (extract(epoch FROM started_at) * 1000)::bigint::text AS started_ms,
              round(extract(epoch FROM (now() - started_at)) / 3600.0, 1)::text AS age_hours,
              expected_endpoint_count AS expected, persisted_endpoint_count AS persisted,
              failed_internal_count AS failed_internal, failure_code
         FROM mcp_tool_collection_runs ORDER BY started_at DESC LIMIT 1`,
    );
    const breach = judgeCollectionRun(runs[0], { source: 'mcp-tools', prefix: 'TOOL_RUN', noun: 'tools/list collection' },
      { runningLimitHours: TOOL_RUNNING_LIMIT_HOURS, staleHours: TOOL_STALE_HOURS });
    if (breach) breaches.push(breach);
    else console.log(`[ingest-watchdog] mcp-tools: newest collection ${runs[0].state} ${runs[0].age_hours}h ago`);
  } catch (e) {
    breaches.push({ source: 'mcp-tools', kind: 'TOOL_RUNS_UNREADABLE', detail: `tool collection run read failed: ${e instanceof Error ? e.message : String(e)}`, since: null });
  }

  // ── 8. Change-ledger freshness ──────────────────────────────────────────────
  {
    let latest: string | null | 'unreadable';
    let problem = '';
    try {
      const rows = await q<{ latest: string | null }>('SELECT max(detected_at)::text AS latest FROM mcp_change_events_v1');
      latest = rows[0]?.latest ?? null;
    } catch (e) {
      latest = 'unreadable';
      problem = e instanceof Error ? e.message : String(e);
    }
    const breach = judgeChangeLedger(latest, Date.now(), CHANGE_LEDGER_STALE_HOURS, problem);
    if (breach && CHANGE_LEDGER_MODE === 'enforce') breaches.push(breach);
    else if (breach) {
      // Report-only: the detector ships disabled, so a stale or unreadable ledger is expected until the owner enables it.
      console.log(`[ingest-watchdog] mcp-changes (report-only): ${breach.kind}: ${breach.detail}`);
      console.log(`::notice title=Change ledger (report-only)::${breach.kind}: ${breach.detail}`);
    } else console.log(`[ingest-watchdog] mcp-changes: newest change detected ${latest} — within ${CHANGE_LEDGER_STALE_HOURS}h`);
  }

  // ── 9. Abandoned scan_runs ──────────────────────────────────────────────────
  try {
    const rows = await q<{ started_at: string; started_ms: string; status: string | null }>(
      `SELECT started_at::text, (extract(epoch FROM started_at) * 1000)::bigint::text AS started_ms, status
         FROM scan_runs
        WHERE status = 'running' AND started_at > now() - make_interval(days => $1)`,
      [SCAN_RUN_LOOKBACK_DAYS],
    );
    const breach = judgeAbandonedScanRuns(rows, Date.now());
    if (breach) breaches.push(breach);
    else console.log(`[ingest-watchdog] scan-runs: no run open past ${SCAN_RUN_ABANDONED_HOURS}h in the last ${SCAN_RUN_LOOKBACK_DAYS} days`);
  } catch (e) {
    breaches.push({ source: 'scan-runs', kind: 'SCAN_RUNS_UNREADABLE', detail: `scan_runs read failed: ${e instanceof Error ? e.message : String(e)}`, since: null });
  }

  if (breaches.length === 0) {
    console.log('[ingest-watchdog] OK — all watched sources within limits');
    await endIngestPool();
    return;
  }

  console.error('');
  console.error('================================================================');
  const nowMs = Date.now();
  const aged = breaches
    .map((b) => ({ ...b, age: breachAge(b.since, nowMs) }))
    .sort((a, b) => (a.age === b.age ? 0 : a.age === 'NEW' ? -1 : 1));
  const fresh = aged.filter((b) => b.age === 'NEW').length;
  console.error(`  INGEST WATCHDOG BREACH — ${fresh} NEW (onset within ${NEW_WITHIN_HOURS}h), ${aged.length - fresh} ONGOING`);
  for (const b of aged) console.error(`  ${b.age.padEnd(7)} [${b.source}] ${b.kind}: ${b.detail}${b.since ? ` (since ${b.since})` : ''}`);
  console.error('================================================================');
  for (const b of aged) {
    const label = b.age === 'NEW' ? 'NEW' : `ONGOING since ${b.since}`;
    console.log(`::error title=Ingest watchdog ${label}: ${b.kind} (${b.source})::${b.detail}`);
  }
  await endIngestPool();
  process.exit(1);
}

if (require.main === module) {
  main().catch(async (e) => {
    console.error('[ingest-watchdog] watchdog itself failed:', e instanceof Error ? e.message : e);
    await endIngestPool();
    process.exit(1);
  });
}
