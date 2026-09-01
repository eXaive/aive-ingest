export const MCP_DASHBOARD_CACHE_SLA_HOURS = 168;
export const MCP_REACHABILITY_CACHE_SLA_HOURS = 6;

export type IngestStatus = 'SUCCEEDED' | 'FAILED';
export type CacheRefreshStatus = 'SUCCEEDED' | 'FAILED' | 'SKIPPED';
export type CacheRefreshErrorClass = 'STATEMENT_TIMEOUT' | 'LOCK_CONTENTION' | 'DATABASE_ERROR' | null;
export type RegistryOverallStatus =
  | 'COMPLETE'
  | 'INGEST_SUCCEEDED_DASHBOARD_FRESH_REACHABILITY_STALE'
  | 'INGEST_SUCCEEDED_DASHBOARD_STALE_REACHABILITY_FRESH'
  | 'INGEST_SUCCEEDED_BOTH_CACHES_STALE'
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
  dashboard_cache_fresh: boolean;
  dashboard_cache_age_hours: number | null;
  reachability_refresh_status: CacheRefreshStatus;
  reachability_refresh_errors: number;
  reachability_refresh_error: string | null;
  reachability_refresh_error_class: CacheRefreshErrorClass;
  reachability_refresh_ms: number;
  reachability_refresh_started_at: string | null;
  reachability_refresh_finished_at: string | null;
  reachability_cache_fresh: boolean;
  reachability_cache_age_hours: number | null;
  cache_fallback_available: boolean;
  cache_fallback_active: boolean;
  cache_fallback_recent_healthy: boolean;
  cache_fallback_alert_enforced: boolean;
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
  const ingestStatus: IngestStatus = input.ingestErrors > 0 ? 'FAILED' : 'SUCCEEDED';
  const ingestFirstError = input.ingestErrors > 0
    ? input.ingestFirstError ?? 'INGEST_ERROR_WITHOUT_RECORDED_DETAIL'
    : null;
  const dashboard = normalizeAttempt(input.dashboard);
  const reachability = normalizeAttempt(input.reachability);
  const health = input.cacheHealthAfter;

  // A successful SQL call is not enough: the independent health read must
  // confirm that the matching cache row recorded a fresh successful publish.
  const dashboardFresh = Boolean(
    dashboard.status === 'SUCCEEDED'
    && health?.dashboard_fresh === true
    && health.dashboard?.status === 'SUCCEEDED'
    && health.dashboard.fresh === true,
  );
  const reachabilityFresh = Boolean(
    reachability.status === 'SUCCEEDED'
    && health?.reachability_fresh === true
    && health.reachability?.status === 'SUCCEEDED'
    && health.reachability.fresh === true,
  );

  let overallStatus: RegistryOverallStatus;
  if (ingestStatus === 'FAILED') overallStatus = 'INGEST_FAILED';
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
    dashboard_cache_fresh: dashboardFresh,
    dashboard_cache_age_hours: health?.dashboard?.age_hours ?? null,
    reachability_refresh_status: reachability.status,
    reachability_refresh_errors: reachability.errors,
    reachability_refresh_error: reachability.error,
    reachability_refresh_error_class: reachability.errorClass,
    reachability_refresh_ms: reachability.durationMs,
    reachability_refresh_started_at: reachability.startedAt,
    reachability_refresh_finished_at: reachability.finishedAt,
    reachability_cache_fresh: reachabilityFresh,
    reachability_cache_age_hours: health?.reachability?.age_hours ?? null,
    cache_fallback_available: health?.fallback_available ?? false,
    cache_fallback_active: health?.fallback_active ?? false,
    cache_fallback_recent_healthy: health?.fallback_recent_healthy ?? false,
    cache_fallback_alert_enforced: health?.fallback_alert_enforced ?? false,
    overall_status: overallStatus,
  };
}

export function exitCodeForRegistryOutcome(outcome: RegistryOutcome): 0 | 1 {
  return outcome.overall_status === 'COMPLETE' ? 0 : 1;
}
