export const MCP_DASHBOARD_CACHE_SLA_HOURS = 168;
export const MCP_REACHABILITY_CACHE_SLA_HOURS = 6;

/* PARTIAL added 2026-09-02. Pagination that dies part-way used to report
   FAILED and discard everything it had already fetched; it now commits the
   servers it did get. That is a third outcome, not a shade of either
   neighbour: some data landed, but the sweep is not a complete census, so
   last_seen was refreshed for only part of the corpus. */
export type IngestStatus = 'SUCCEEDED' | 'PARTIAL' | 'FAILED';
export type CacheRefreshStatus = 'SUCCEEDED' | 'FAILED' | 'SKIPPED';
export type CacheRefreshErrorClass = 'STATEMENT_TIMEOUT' | 'LOCK_CONTENTION' | 'DATABASE_ERROR' | null;
export type RegistryOverallStatus =
  | 'COMPLETE'
  | 'INGEST_SUCCEEDED_DASHBOARD_FRESH_REACHABILITY_STALE'
  | 'INGEST_SUCCEEDED_DASHBOARD_STALE_REACHABILITY_FRESH'
  | 'INGEST_SUCCEEDED_BOTH_CACHES_STALE'
  | 'INGEST_PARTIAL'
  | 'INGEST_FAILED';

export interface CacheComponentHealth {
  status: 'SUCCEEDED' | 'FAILED' | 'UNKNOWN';
  computed_at: string | null;
  age_hours: number | null;
  sla_hours: number;
  fresh: boolean;
  started_at: string | null;
  finished_at: string | null;
  duration_ms: number | null;
  error: string | null;
  error_class: CacheRefreshErrorClass;
}

export interface CacheRefreshHealth {
  cache_computed_at: string | null;
  cache_age_hours: number | null;
  cache_sla_hours: number;
  fallback_available: boolean;
  fallback_active: boolean;
  fallback_recent_run_count: number;
  fallback_recent_healthy: boolean;
  fallback_last_succeeded_at: string | null;
  fallback_alert_enforced: boolean;
  dashboard_fresh: boolean;
  reachability_fresh: boolean;
  dashboard: CacheComponentHealth | null;
  reachability: CacheComponentHealth | null;
}

export interface CacheAttemptInput {
  status: CacheRefreshStatus;
  error: string | null;
  errorClass: CacheRefreshErrorClass;
  durationMs: number;
  startedAt: string | null;
  finishedAt: string | null;
}

export interface RegistryOutcomeInput {
  ingestErrors: number;
  ingestFirstError: string | null;
  dashboard: CacheAttemptInput;
  reachability: CacheAttemptInput;
  cacheHealthAfter: CacheRefreshHealth | null;
  /** True when pagination ended early but some servers were still committed. */
  ingestPartial?: boolean;
}

export interface RegistryOutcome {
  ingest_status: IngestStatus;
  ingest_errors: number;
  ingest_first_error: string | null;
  dashboard_refresh_status: CacheRefreshStatus;
  dashboard_refresh_errors: number;
  dashboard_refresh_error: string | null;
  dashboard_refresh_error_class: CacheRefreshErrorClass;
  dashboard_refresh_ms: number;
  dashboard_refresh_started_at: string | null;
  dashboard_refresh_finished_at: string | null;
  /* null = NOT EVALUATED, and it is a distinct answer from false. These read
     false whenever the refresh was skipped or the health read was unavailable,
     which reports "this cache is stale" on a run that never looked. Only a
     completed evaluation may say false. */
  dashboard_cache_fresh: boolean | null;
  dashboard_cache_age_hours: number | null;
  reachability_refresh_status: CacheRefreshStatus;
  reachability_refresh_errors: number;
  reachability_refresh_error: string | null;
  reachability_refresh_error_class: CacheRefreshErrorClass;
  reachability_refresh_ms: number;
  reachability_refresh_started_at: string | null;
  reachability_refresh_finished_at: string | null;
  reachability_cache_fresh: boolean | null;
  reachability_cache_age_hours: number | null;
  /* Same rule. `?? false` here meant a run that could not read cache health
     published "no fallback available, nothing recently healthy, no alert
     enforced" -- four confident negatives derived from one absent read. */
  cache_fallback_available: boolean | null;
  cache_fallback_active: boolean | null;
  cache_fallback_recent_healthy: boolean | null;
  cache_fallback_alert_enforced: boolean | null;
  overall_status: RegistryOverallStatus;
}

export function classifyCacheRefreshError(error: unknown): CacheRefreshErrorClass {
  const record = error && typeof error === 'object' ? error as { code?: unknown; message?: unknown } : null;
  const code = typeof record?.code === 'string' ? record.code : '';
  const message = typeof record?.message === 'string' ? record.message.toLowerCase() : String(error).toLowerCase();
  if (code === '57014' || message.includes('statement timeout')) return 'STATEMENT_TIMEOUT';
  if (code === '55P03' || code === '40P01' || message.includes('lock timeout') || message.includes('could not obtain lock')) {
    return 'LOCK_CONTENTION';
  }
  return 'DATABASE_ERROR';
}

function normalizeAttempt(attempt: CacheAttemptInput): CacheAttemptInput & { errors: number } {
  if (attempt.status !== 'FAILED') return { ...attempt, error: null, errorClass: null, errors: 0 };
  return {
    ...attempt,
    error: attempt.error?.trim() || 'CACHE_REFRESH_FAILED_WITHOUT_RECORDED_DETAIL',
    errorClass: attempt.errorClass ?? 'DATABASE_ERROR',
    errors: 1,
  };
}

export function buildRegistryOutcome(input: RegistryOutcomeInput): RegistryOutcome {
  const ingestStatus: IngestStatus =
    input.ingestErrors > 0 ? 'FAILED' : input.ingestPartial ? 'PARTIAL' : 'SUCCEEDED';
  const ingestFirstError = input.ingestErrors > 0
    ? input.ingestFirstError ?? 'INGEST_ERROR_WITHOUT_RECORDED_DETAIL'
    : input.ingestFirstError; // a PARTIAL run keeps the reason it stopped
  const dashboard = normalizeAttempt(input.dashboard);
  const reachability = normalizeAttempt(input.reachability);
  const health = input.cacheHealthAfter;

  /* Freshness is a three-state answer. A refresh that was never attempted, or
     a health read that came back empty, yields null -- UNKNOWN -- and only a
     completed evaluation returns true or false. Returning false for "we did
     not look" is what made a skipped refresh indistinguishable from a stale
     cache in the run payload. */
  const evaluateFresh = (
    attempt: CacheAttemptInput,
    componentFresh: boolean | undefined,
    component: CacheComponentHealth | null | undefined,
  ): boolean | null => {
    if (attempt.status === 'SKIPPED') return null;   // never attempted
    if (attempt.status === 'FAILED') return false;   // attempted, did not land
    if (!health || !component) return null;          // attempted, could not verify
    // A successful SQL call is not enough: the independent health read must
    // confirm that the matching cache row recorded a fresh successful publish.
    return componentFresh === true && component.status === 'SUCCEEDED' && component.fresh === true;
  };

  const dashboardFreshState = evaluateFresh(dashboard, health?.dashboard_fresh, health?.dashboard);
  const reachabilityFreshState = evaluateFresh(reachability, health?.reachability_fresh, health?.reachability);

  // Only a confirmed true counts as fresh for the status; UNKNOWN fails closed.
  const dashboardFresh = dashboardFreshState === true;
  const reachabilityFresh = reachabilityFreshState === true;

  let overallStatus: RegistryOverallStatus;
  if (ingestStatus === 'FAILED') overallStatus = 'INGEST_FAILED';
  else if (ingestStatus === 'PARTIAL') overallStatus = 'INGEST_PARTIAL';
  else if (dashboardFresh && reachabilityFresh) overallStatus = 'COMPLETE';
  else if (dashboardFresh) overallStatus = 'INGEST_SUCCEEDED_DASHBOARD_FRESH_REACHABILITY_STALE';
  else if (reachabilityFresh) overallStatus = 'INGEST_SUCCEEDED_DASHBOARD_STALE_REACHABILITY_FRESH';
  else overallStatus = 'INGEST_SUCCEEDED_BOTH_CACHES_STALE';

  return {
    ingest_status: ingestStatus,
    ingest_errors: input.ingestErrors,
    ingest_first_error: ingestFirstError,
    dashboard_refresh_status: dashboard.status,
    dashboard_refresh_errors: dashboard.errors,
    dashboard_refresh_error: dashboard.error,
    dashboard_refresh_error_class: dashboard.errorClass,
    dashboard_refresh_ms: dashboard.durationMs,
    dashboard_refresh_started_at: dashboard.startedAt,
    dashboard_refresh_finished_at: dashboard.finishedAt,
    dashboard_cache_fresh: dashboardFreshState,
    dashboard_cache_age_hours: health?.dashboard?.age_hours ?? null,
    reachability_refresh_status: reachability.status,
    reachability_refresh_errors: reachability.errors,
    reachability_refresh_error: reachability.error,
    reachability_refresh_error_class: reachability.errorClass,
    reachability_refresh_ms: reachability.durationMs,
    reachability_refresh_started_at: reachability.startedAt,
    reachability_refresh_finished_at: reachability.finishedAt,
    reachability_cache_fresh: reachabilityFreshState,
    reachability_cache_age_hours: health?.reachability?.age_hours ?? null,
    cache_fallback_available: health?.fallback_available ?? null,
    cache_fallback_active: health?.fallback_active ?? null,
    cache_fallback_recent_healthy: health?.fallback_recent_healthy ?? null,
    cache_fallback_alert_enforced: health?.fallback_alert_enforced ?? null,
    overall_status: overallStatus,
  };
}

/**
 * Exit codes, distinct per failure mode (2026-09-02).
 *
 *   0  the registry data landed. Includes the case where a cache refresh did
 *      not: the ingest is the job, the cache is a derived view of it, and a
 *      stale cache with good underlying data is not the same event as a failed
 *      scan. Every non-COMPLETE outcome used to exit 1, so four consecutive
 *      cache-only failures showed up as four red ingest runs -- which is how a
 *      red run stops meaning anything.
 *   1  INGEST_FAILED -- nothing was committed.
 *   2  INGEST_PARTIAL -- some servers committed, the sweep is not a census.
 *
 * THIS TRADE IS ONLY SAFE WITH THE WATCHDOG. Returning 0 on a stale cache
 * means the daily run no longer alerts on it, so the compensating signal is
 * workers/ingest-watchdog.ts, which reads the database on its own schedule and
 * goes red on repeated cache-refresh failure. Do not weaken one without the
 * other -- together they move the alert to where the evidence is; alone, this
 * function just deletes an alert.
 */
export function exitCodeForRegistryOutcome(outcome: RegistryOutcome): 0 | 1 | 2 {
  if (outcome.ingest_status === 'FAILED') return 1;
  if (outcome.ingest_status === 'PARTIAL') return 2;
  return 0;
}
