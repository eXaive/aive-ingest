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
 *   1. INGESTION GAP  — hours since the newest ingestion_log row per source.
 *   2. CONSECUTIVE FAILURES — the newest runs per source, all with an error.
 *   3. CACHE REFRESH — consecutive cache-refresh failures recorded in the
 *      registry runs' metadata, the signal moved off the ingest exit code.
 *   4. CACHE AGE — each cache's own age against its own SLA. Checks 1-3 read
 *      run history and so cannot flag anything before N runs have happened;
 *      this one is true on day one and true when nothing runs at all.
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

interface Breach { source: string; kind: string; detail: string }

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
    const gapRows = await q<{ age_hours: string | null; last_at: string | null }>(
      `SELECT round(extract(epoch FROM (now() - max(started_at))) / 3600.0, 1)::text AS age_hours,
              max(started_at)::text AS last_at
         FROM ingestion_log WHERE source_slug = $1`,
      [source],
    );
    const ageHours = gapRows[0]?.age_hours === null || gapRows[0]?.age_hours === undefined
      ? null : Number(gapRows[0].age_hours);

    if (ageHours === null) {
      breaches.push({ source, kind: 'NO_INGESTION_EVER', detail: 'no ingestion_log row has ever been written for this source' });
    } else if (ageHours > GAP_HOURS) {
      breaches.push({
        source, kind: 'INGESTION_GAP',
        detail: `${ageHours}h since the last run (limit ${GAP_HOURS}h) — last at ${gapRows[0].last_at}`,
      });
    } else {
      console.log(`[ingest-watchdog] ${source}: last ingestion ${ageHours}h ago — within ${GAP_HOURS}h`);
    }

    // ── 2. Consecutive failed runs ──────────────────────────────────────────
    /* Newest-first, counting the unbroken prefix that carries an error. A run
       that succeeded ends the streak; older failures behind it are history,
       not an ongoing outage. */
    const recent = await q<{ failed: boolean; started_at: string }>(
      `SELECT (error_message IS NOT NULL) AS failed, started_at::text
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
      });
    } else if (streak > 0) {
      console.log(`[ingest-watchdog] ${source}: ${streak} recent failure(s) — under the ${CONSECUTIVE} threshold`);
    } else {
      console.log(`[ingest-watchdog] ${source}: newest run carries no error`);
    }

    // ── 3. Consecutive cache-refresh failures ───────────────────────────────
    /* The signal that moved off the registry exit code. Reads the outcome the
       worker recorded in metadata; runs that never evaluated a cache (SKIPPED,
       or a null from the un-evaluated fix) are not counted as failures -- that
       conflation is the bug this pass removed, and re-introducing it here
       would put it straight back. */
    const cacheRows = await q<{ dash: string | null; reach: string | null; started_at: string }>(
      `SELECT metadata->>'dashboard_refresh_status' AS dash,
              metadata->>'reachability_refresh_status' AS reach,
              started_at::text
         FROM ingestion_log
        WHERE source_slug = $1 AND metadata ? 'dashboard_refresh_status'
        ORDER BY started_at DESC LIMIT $2`,
      [source, Math.max(CONSECUTIVE, 10)],
    );
    for (const component of ['dash', 'reach'] as const) {
      let cacheStreak = 0;
      for (const row of cacheRows) {
        const status = row[component];
        if (status === 'SUCCEEDED') break;      // a good refresh ends the streak
        if (status === 'FAILED') cacheStreak++;
        else break;                              // SKIPPED / unknown: not evidence either way
      }
      if (cacheStreak >= CONSECUTIVE) {
        breaches.push({
          source, kind: 'CACHE_REFRESH_FAILURES',
          detail: `${component === 'dash' ? 'dashboard' : 'reachability'} cache refresh failed ${cacheStreak} run(s) in a row — the surface is serving stale figures`,
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
      breaches.push({ source: 'mcp-cache', kind: 'CACHE_HEALTH_UNREADABLE', detail: 'mcp_dashboard_refresh_health() returned nothing — cache state cannot be judged' });
    } else {
      for (const name of ['dashboard', 'reachability'] as const) {
        const c = health[name];
        if (!c) {
          breaches.push({ source: 'mcp-cache', kind: 'CACHE_NEVER_COMPUTED', detail: `${name} cache has no recorded computation` });
          continue;
        }
        if (c.age_hours === null) {
          breaches.push({ source: 'mcp-cache', kind: 'CACHE_NEVER_COMPUTED', detail: `${name} cache reports no age` });
        } else if (c.age_hours > c.sla_hours) {
          breaches.push({
            source: 'mcp-cache', kind: 'CACHE_STALE',
            detail: `${name} cache is ${c.age_hours.toFixed(1)}h old against a ${c.sla_hours}h SLA (last status ${c.status})`,
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
    });
  }

  if (breaches.length === 0) {
    console.log('[ingest-watchdog] OK — all watched sources within limits');
    await endIngestPool();
    return;
  }

  console.error('');
  console.error('================================================================');
  console.error('  INGEST WATCHDOG BREACH');
  for (const b of breaches) console.error(`  [${b.source}] ${b.kind}: ${b.detail}`);
  console.error('================================================================');
  for (const b of breaches) {
    console.log(`::error title=Ingest watchdog: ${b.kind} (${b.source})::${b.detail}`);
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
