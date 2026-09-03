import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {
  buildRegistryOutcome,
  classifyCacheRefreshError,
  exitCodeForRegistryOutcome,
  type CacheRefreshHealth,
  type RegistryOutcomeInput,
} from '../lib/ingest/mcpRegistryOutcome';

const component = (fresh: boolean) => ({
  status: fresh ? 'SUCCEEDED' as const : 'FAILED' as const,
  computed_at: '2026-08-31T12:00:00Z', age_hours: fresh ? 1 : 200,
  sla_hours: 168, fresh, started_at: '2026-08-31T11:00:00Z',
  finished_at: '2026-08-31T11:00:08Z', duration_ms: 8_000,
  error: fresh ? null : 'statement timeout',
  error_class: fresh ? null : 'STATEMENT_TIMEOUT' as const,
});
const health = (dashboardFresh: boolean, reachabilityFresh: boolean): CacheRefreshHealth => ({
  cache_computed_at: '2026-08-31T12:00:00Z', cache_age_hours: 1, cache_sla_hours: 168,
  fallback_available: true, fallback_active: true, fallback_recent_run_count: 5,
  fallback_recent_healthy: true, fallback_last_succeeded_at: '2026-08-31T12:00:00Z',
  fallback_alert_enforced: false,
  dashboard_fresh: dashboardFresh, reachability_fresh: reachabilityFresh,
  dashboard: component(dashboardFresh), reachability: component(reachabilityFresh),
});
const success = {
  status: 'SUCCEEDED' as const, error: null, errorClass: null, durationMs: 8_123,
  startedAt: '2026-08-31T11:00:00Z', finishedAt: '2026-08-31T11:00:08.123Z',
};
const failure = {
  status: 'FAILED' as const, error: 'canceling statement due to statement timeout',
  errorClass: 'STATEMENT_TIMEOUT' as const, durationMs: 60_100,
  startedAt: '2026-08-31T11:00:08.124Z', finishedAt: '2026-08-31T11:01:08.224Z',
};
const base: RegistryOutcomeInput = {
  ingestErrors: 0, ingestFirstError: null,
  dashboard: success, reachability: success, cacheHealthAfter: health(true, true),
};

const complete = buildRegistryOutcome(base);
assert.equal(complete.overall_status, 'COMPLETE');
assert.equal(exitCodeForRegistryOutcome(complete), 0);

/* EXIT-CODE CONTRACT CHANGED 2026-09-02. These four cases previously asserted
   exit 1. They now assert 0, and that is the point of the change rather than a
   test bent to fit it: in every one of them the REGISTRY DATA LANDED and only a
   derived cache is stale. Exiting 1 made four consecutive cache-only failures
   look like four failed ingests, which is how a red run stops meaning anything.
   The status string still records exactly what went wrong, the run still prints
   a ::warning::, and repeated cache failure now alarms through
   workers/ingest-watchdog.ts. Do not restore these to 1 without also removing
   the watchdog -- the two were changed as one trade. */
const reachabilityFailed = buildRegistryOutcome({ ...base, reachability: failure, cacheHealthAfter: health(true, false) });
assert.equal(reachabilityFailed.overall_status, 'INGEST_SUCCEEDED_DASHBOARD_FRESH_REACHABILITY_STALE');
assert.equal(reachabilityFailed.reachability_refresh_errors, 1);
assert.ok(reachabilityFailed.reachability_refresh_error);
assert.equal(exitCodeForRegistryOutcome(reachabilityFailed), 0);

const dashboardFailed = buildRegistryOutcome({ ...base, dashboard: failure, cacheHealthAfter: health(false, true) });
assert.equal(dashboardFailed.overall_status, 'INGEST_SUCCEEDED_DASHBOARD_STALE_REACHABILITY_FRESH');
assert.equal(exitCodeForRegistryOutcome(dashboardFailed), 0);

const bothFailed = buildRegistryOutcome({ ...base, dashboard: failure, reachability: failure,
  cacheHealthAfter: health(false, false) });
assert.equal(bothFailed.overall_status, 'INGEST_SUCCEEDED_BOTH_CACHES_STALE');
assert.equal(exitCodeForRegistryOutcome(bothFailed), 0);

const missingHealth = buildRegistryOutcome({ ...base, cacheHealthAfter: null });
assert.equal(missingHealth.overall_status, 'INGEST_SUCCEEDED_BOTH_CACHES_STALE');
assert.equal(exitCodeForRegistryOutcome(missingHealth), 0);

/* NOT EVALUATED IS NOT FALSE. A skipped refresh, or one whose health could not
   be read, reports null. Only a completed evaluation may say false. `?? false`
   used to publish four confident negatives derived from one absent read. */
assert.equal(missingHealth.cache_fallback_available, null);
assert.equal(missingHealth.cache_fallback_active, null);
assert.equal(missingHealth.cache_fallback_recent_healthy, null);
assert.equal(missingHealth.cache_fallback_alert_enforced, null);
assert.equal(missingHealth.dashboard_cache_fresh, null, 'attempted but unverifiable is UNKNOWN, not stale');

const skippedCaches = buildRegistryOutcome({
  ...base,
  dashboard: { ...success, status: 'SKIPPED', durationMs: 0, startedAt: null, finishedAt: null },
  reachability: { ...success, status: 'SKIPPED', durationMs: 0, startedAt: null, finishedAt: null },
  cacheHealthAfter: null,
});
assert.equal(skippedCaches.dashboard_cache_fresh, null, 'a refresh never attempted cannot report false');
assert.equal(skippedCaches.reachability_cache_fresh, null);

// A refresh that ran and demonstrably did not land is a real false, not UNKNOWN,
// and a health read that DID happen still reports its own booleans.
assert.equal(bothFailed.dashboard_cache_fresh, false);
assert.equal(bothFailed.cache_fallback_available, true);

/* PARTIAL: pagination stopped early but servers were committed. Distinct from
   both neighbours and distinct in the exit code, so a slice is never reported
   as a completed census. */
const partial = buildRegistryOutcome({ ...base, ingestPartial: true, ingestFirstError: 'partial scan: Registry 500' });
assert.equal(partial.ingest_status, 'PARTIAL');
assert.equal(partial.overall_status, 'INGEST_PARTIAL');
assert.equal(partial.ingest_errors, 0, 'a partial scan is not an errored scan');
assert.equal(partial.ingest_first_error, 'partial scan: Registry 500', 'a partial run keeps the reason it stopped');
assert.equal(exitCodeForRegistryOutcome(partial), 2);

const ingestFailed = buildRegistryOutcome({ ...base, ingestErrors: 1, ingestFirstError: null,
  dashboard: { ...success, status: 'SKIPPED', durationMs: 0, startedAt: null, finishedAt: null },
  reachability: { ...success, status: 'SKIPPED', durationMs: 0, startedAt: null, finishedAt: null } });
assert.equal(ingestFailed.overall_status, 'INGEST_FAILED');
assert.equal(ingestFailed.ingest_first_error, 'INGEST_ERROR_WITHOUT_RECORDED_DETAIL');
assert.equal(exitCodeForRegistryOutcome(ingestFailed), 1);
assert.equal(classifyCacheRefreshError({ code: '57014', message: 'query canceled' }), 'STATEMENT_TIMEOUT');
assert.equal(classifyCacheRefreshError(new Error('canceling statement due to statement timeout')), 'STATEMENT_TIMEOUT');
assert.equal(classifyCacheRefreshError({ code: '55P03', message: 'lock unavailable' }), 'LOCK_CONTENTION');
assert.equal(classifyCacheRefreshError(new Error('other database failure')), 'DATABASE_ERROR');

const worker = fs.readFileSync(path.join(process.cwd(), 'workers/ingest-mcp-registry.ts'), 'utf8');
const workflow = fs.readFileSync(path.join(process.cwd(), '.github/workflows/ingest-mcp-registry.yml'), 'utf8');
assert.equal((worker.match(/SELECT public\.trigger_mcp_dashboard_refresh\(\) AS computed_at/g) ?? []).length, 1,
  'dashboard refresh executes once');
assert.equal((worker.match(/SELECT public\.trigger_mcp_reachability_refresh\(\) AS computed_at/g) ?? []).length, 1,
  'reachability refresh executes once');
assert.ok(worker.indexOf('trigger_mcp_dashboard_refresh') < worker.indexOf('trigger_mcp_reachability_refresh'),
  'dashboard commits before reachability starts');
assert.doesNotMatch(worker, /errors\+\+[\s\S]{0,300}dashboard cache refresh FAILED/);
assert.match(worker, /finally \{[\s\S]*attempt\.durationMs = Date\.now\(\) - startedMs/);
assert.match(worker, /record_mcp_cache_refresh_failure/);
assert.match(workflow, /cron: '0 5 \* \* \*'/, 'registry schedule remains unchanged');
assert.match(workflow, /group: mcp-registry-ingest[\s\S]*cancel-in-progress: false/, 'serial non-canceling execution remains intact');
assert.equal((workflow.match(/npx tsx workers\/ingest-mcp-registry\.ts/g) ?? []).length, 1,
  'workflow invokes the full ingest exactly once');

console.log('MCP registry outcome and cache-refresh contract: passed');
