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

const reachabilityFailed = buildRegistryOutcome({ ...base, reachability: failure, cacheHealthAfter: health(true, false) });
assert.equal(reachabilityFailed.overall_status, 'INGEST_SUCCEEDED_DASHBOARD_FRESH_REACHABILITY_STALE');
assert.equal(reachabilityFailed.reachability_refresh_errors, 1);
assert.ok(reachabilityFailed.reachability_refresh_error);
assert.equal(exitCodeForRegistryOutcome(reachabilityFailed), 1);

const dashboardFailed = buildRegistryOutcome({ ...base, dashboard: failure, cacheHealthAfter: health(false, true) });
assert.equal(dashboardFailed.overall_status, 'INGEST_SUCCEEDED_DASHBOARD_STALE_REACHABILITY_FRESH');
assert.equal(exitCodeForRegistryOutcome(dashboardFailed), 1);

const bothFailed = buildRegistryOutcome({ ...base, dashboard: failure, reachability: failure,
  cacheHealthAfter: health(false, false) });
assert.equal(bothFailed.overall_status, 'INGEST_SUCCEEDED_BOTH_CACHES_STALE');
assert.equal(exitCodeForRegistryOutcome(bothFailed), 1);

const missingHealth = buildRegistryOutcome({ ...base, cacheHealthAfter: null });
assert.equal(missingHealth.overall_status, 'INGEST_SUCCEEDED_BOTH_CACHES_STALE');
assert.equal(exitCodeForRegistryOutcome(missingHealth), 1);

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
