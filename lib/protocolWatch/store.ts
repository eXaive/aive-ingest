/**
 * Protocol watch evidence stores (Phase 2). Same contract, two implementations:
 *   MemoryWatchStore  verifiers and local dry runs (no database at all);
 *   PgWatchStore      the gated production path, against the tables in the platform proposal
 *                     docs/database/proposals/protocol-watch-evidence-v1.sql.proposal (NOT applied anywhere yet).
 *
 * Contract:
 *  - beginRun is idempotent per run_key: a finished key is a no-op; an unfinished key resumes from its checkpoint.
 *  - recordSuccess writes a snapshot ONLY when the state hash changed (write-on-change), inserts observations
 *    idempotently (observation_id), and appends the source to the run checkpoint, all in one transaction.
 *  - recordFailure updates the subject's attempt fields only. History is never updated or deleted.
 */
import type { PoolClient } from 'pg';
import type { CatalogueSource } from './catalogue';
import type { CollectResult, Observation } from './collectors';

export type RunStatus = 'RUNNING' | 'SUCCEEDED' | 'PARTIAL' | 'FAILED' | 'ABANDONED';
export interface RunCounters { succeeded: number; failed: number; skipped: number; snapshots: number; observations: number; requests: number }
export interface BeganRun { runId: string; checkpoint: Set<string>; resumed: boolean; alreadyFinished: boolean }
export type SuccessResult = Extract<CollectResult, { outcome: 'SUCCESS' }>;

export interface WatchStore {
  beginRun(input: { runKey: string; catalogueVersion: string; trigger: 'schedule' | 'manual'; planned: number }): Promise<BeganRun>;
  previousState(sourceId: string): Promise<Record<string, unknown> | null>;
  recordSuccess(input: { runId: string; source: CatalogueSource; result: SuccessResult }): Promise<{ snapshotWritten: boolean; observationsWritten: number }>;
  recordFailure(input: { runId: string; source: CatalogueSource; outcome: 'FAILED' | 'UNAVAILABLE'; httpStatus: number | null; errorCode: string }): Promise<void>;
  heartbeat(runId: string, delta: Partial<RunCounters>): Promise<void>;
  finishRun(runId: string, status: Exclude<RunStatus, 'RUNNING'>, errorSummary: string | null): Promise<void>;
}

/** A RUNNING receipt older than this (no heartbeat) is abandoned; the job timeout is 20 minutes. */
export const ABANDON_AFTER_MINUTES = 45;

// ─── Memory ─────────────────────────────────────────────────────────────────────────────────────────────────────
interface MemRun { runId: string; runKey: string; catalogueVersion: string; status: RunStatus; checkpoint: string[]; heartbeatAt: number; finishedAt: number | null; counters: RunCounters; errorSummary: string | null }
interface MemSubject { subjectId: string; protocolId: string; lastAttemptAt: number | null; lastSuccessAt: number | null; lastOutcome: string | null; consecutiveFailures: number; lastHttpStatus: number | null; lastErrorCode: string | null; stateHash: string | null }

export class MemoryWatchStore implements WatchStore {
  runs: MemRun[] = [];
  subjects = new Map<string, MemSubject>();
  snapshots: { subjectId: string; runId: string; stateHash: string; previousHash: string | null; state: Record<string, unknown> }[] = [];
  observations = new Map<string, Observation & { subjectId: string; runId: string }>();
  private seq = 0;
  constructor(private readonly clock: () => number = Date.now) {}

  async beginRun({ runKey, catalogueVersion, planned }: { runKey: string; catalogueVersion: string; trigger: 'schedule' | 'manual'; planned: number }): Promise<BeganRun> {
    const now = this.clock();
    for (const r of this.runs) if (r.status === 'RUNNING' && r.runKey !== runKey && now - r.heartbeatAt > ABANDON_AFTER_MINUTES * 60_000) { r.status = 'ABANDONED'; r.finishedAt = now; }
    const existing = this.runs.find((r) => r.runKey === runKey);
    if (existing) {
      if (existing.status !== 'RUNNING') return { runId: existing.runId, checkpoint: new Set(existing.checkpoint), resumed: false, alreadyFinished: true };
      if (existing.catalogueVersion !== catalogueVersion) throw new Error('protocol-watch: cannot resume a run under a different catalogue version');
      existing.heartbeatAt = now;
      return { runId: existing.runId, checkpoint: new Set(existing.checkpoint), resumed: true, alreadyFinished: false };
    }
    const run: MemRun = { runId: `run-${++this.seq}`, runKey, catalogueVersion, status: 'RUNNING', checkpoint: [], heartbeatAt: now, finishedAt: null, counters: { succeeded: 0, failed: 0, skipped: 0, snapshots: 0, observations: 0, requests: 0 }, errorSummary: null };
    void planned;
    this.runs.push(run);
    return { runId: run.runId, checkpoint: new Set(), resumed: false, alreadyFinished: false };
  }
  private subject(source: CatalogueSource): MemSubject {
    let s = this.subjects.get(source.sourceId);
    if (!s) { s = { subjectId: source.sourceId, protocolId: source.protocolId, lastAttemptAt: null, lastSuccessAt: null, lastOutcome: null, consecutiveFailures: 0, lastHttpStatus: null, lastErrorCode: null, stateHash: null }; this.subjects.set(source.sourceId, s); }
    return s;
  }
  private run(runId: string): MemRun { const r = this.runs.find((x) => x.runId === runId); if (!r) throw new Error('unknown run'); return r; }
  async previousState(sourceId: string) { for (let i = this.snapshots.length - 1; i >= 0; i--) if (this.snapshots[i].subjectId === sourceId) return this.snapshots[i].state; return null; }
  async recordSuccess({ runId, source, result }: { runId: string; source: CatalogueSource; result: SuccessResult }) {
    const s = this.subject(source); const r = this.run(runId); const now = this.clock();
    let snapshotWritten = false;
    if (s.stateHash !== result.stateHash && !this.snapshots.some((x) => x.subjectId === s.subjectId && x.runId === runId)) {
      this.snapshots.push({ subjectId: s.subjectId, runId, stateHash: result.stateHash, previousHash: s.stateHash, state: result.state });
      s.stateHash = result.stateHash; snapshotWritten = true;
    }
    let observationsWritten = 0;
    for (const o of result.observations) if (!this.observations.has(o.observationId)) { this.observations.set(o.observationId, { ...o, subjectId: s.subjectId, runId }); observationsWritten++; }
    Object.assign(s, { lastAttemptAt: now, lastSuccessAt: now, lastOutcome: 'SUCCESS', consecutiveFailures: 0, lastHttpStatus: result.httpStatus, lastErrorCode: null });
    if (!r.checkpoint.includes(s.subjectId)) r.checkpoint.push(s.subjectId);
    r.counters.succeeded++; r.counters.snapshots += snapshotWritten ? 1 : 0; r.counters.observations += observationsWritten; r.heartbeatAt = now;
    return { snapshotWritten, observationsWritten };
  }
  async recordFailure({ runId, source, outcome, httpStatus, errorCode }: { runId: string; source: CatalogueSource; outcome: 'FAILED' | 'UNAVAILABLE'; httpStatus: number | null; errorCode: string }) {
    const s = this.subject(source); const r = this.run(runId); const now = this.clock();
    Object.assign(s, { lastAttemptAt: now, lastOutcome: outcome, consecutiveFailures: s.consecutiveFailures + 1, lastHttpStatus: httpStatus, lastErrorCode: errorCode });
    if (!r.checkpoint.includes(s.subjectId)) r.checkpoint.push(s.subjectId);
    r.counters.failed++; r.heartbeatAt = now;
  }
  async heartbeat(runId: string, delta: Partial<RunCounters>) { const r = this.run(runId); for (const [k, v] of Object.entries(delta)) (r.counters as unknown as Record<string, number>)[k] += v ?? 0; r.heartbeatAt = this.clock(); }
  async finishRun(runId: string, status: Exclude<RunStatus, 'RUNNING'>, errorSummary: string | null) { const r = this.run(runId); r.status = status; r.finishedAt = this.clock(); r.errorSummary = errorSummary; }
}

// ─── Postgres ───────────────────────────────────────────────────────────────────────────────────────────────────
export const REQUIRED_PRIVILEGES: readonly [string, string][] = [
  ['public.protocol_watch_runs', 'INSERT'], ['public.protocol_watch_runs', 'UPDATE'], ['public.protocol_watch_runs', 'SELECT'],
  ['public.protocol_evidence_subjects', 'INSERT'], ['public.protocol_evidence_subjects', 'UPDATE'], ['public.protocol_evidence_subjects', 'SELECT'],
  ['public.protocol_evidence_snapshots', 'INSERT'], ['public.protocol_evidence_snapshots', 'SELECT'],
  ['public.protocol_evidence_observations', 'INSERT'], ['public.protocol_evidence_observations', 'SELECT'],
];

type Query = <T = Record<string, unknown>>(text: string, params?: unknown[]) => Promise<T[]>;

export class PgWatchStore implements WatchStore {
  constructor(private readonly q: Query, private readonly connect: () => Promise<PoolClient>) {}

  /** Fails fast, naming what is missing, instead of reporting a permission wall as an upstream outage. */
  async preflight(): Promise<void> {
    const missingTables = await this.q<{ t: string }>(`SELECT t FROM unnest($1::text[]) t WHERE to_regclass(t) IS NULL`, [[...new Set(REQUIRED_PRIVILEGES.map(([t]) => t))]]);
    if (missingTables.length) throw new Error(`protocol-watch: evidence tables are not installed (${missingTables.map((r) => r.t).join(', ')}). The migration proposal protocol-watch-evidence-v1 has not been applied.`);
    const rows = await this.q<{ t: string; p: string; ok: boolean }>(`SELECT t, p, has_table_privilege(t, p) AS ok FROM unnest($1::text[], $2::text[]) AS x(t, p)`, [REQUIRED_PRIVILEGES.map(([t]) => t), REQUIRED_PRIVILEGES.map(([, p]) => p)]);
    const missing = rows.filter((r) => !r.ok).map((r) => `${r.p} on ${r.t}`);
    if (missing.length) throw new Error(`protocol-watch: aive_ingest lacks ${missing.join('; ')}`);
  }

  async beginRun({ runKey, catalogueVersion, trigger, planned }: { runKey: string; catalogueVersion: string; trigger: 'schedule' | 'manual'; planned: number }): Promise<BeganRun> {
    await this.q(`UPDATE public.protocol_watch_runs SET status = 'ABANDONED', finished_at = now(), error_summary = coalesce(error_summary, 'no heartbeat')
                  WHERE status = 'RUNNING' AND run_key <> $1 AND heartbeat_at < now() - make_interval(mins => $2)`, [runKey, ABANDON_AFTER_MINUTES]);
    const inserted = await this.q<{ run_id: string }>(`INSERT INTO public.protocol_watch_runs (run_key, catalogue_version, trigger, sources_planned) VALUES ($1, $2, $3, $4)
                  ON CONFLICT (run_key) DO NOTHING RETURNING run_id`, [runKey, catalogueVersion, trigger, planned]);
    if (inserted[0]) return { runId: inserted[0].run_id, checkpoint: new Set(), resumed: false, alreadyFinished: false };
    const [run] = await this.q<{ run_id: string; status: RunStatus; checkpoint: string[]; catalogue_version: string }>(
      `SELECT run_id, status, checkpoint, catalogue_version FROM public.protocol_watch_runs WHERE run_key = $1`, [runKey]);
    if (!run) throw new Error('protocol-watch: run receipt vanished');
    if (run.status !== 'RUNNING') return { runId: run.run_id, checkpoint: new Set(run.checkpoint), resumed: false, alreadyFinished: true };
    if (run.catalogue_version !== catalogueVersion) throw new Error('protocol-watch: cannot resume a run under a different catalogue version');
    await this.q(`UPDATE public.protocol_watch_runs SET heartbeat_at = now() WHERE run_id = $1`, [run.run_id]);
    return { runId: run.run_id, checkpoint: new Set(run.checkpoint), resumed: true, alreadyFinished: false };
  }

  async previousState(sourceId: string) {
    const rows = await this.q<{ state: Record<string, unknown> }>(`SELECT state FROM public.protocol_evidence_snapshots WHERE subject_id = $1 ORDER BY snapshot_id DESC LIMIT 1`, [sourceId]);
    return rows[0]?.state ?? null;
  }

  private async tx<T>(fn: (c: PoolClient) => Promise<T>): Promise<T> {
    const c = await this.connect();
    try { await c.query('BEGIN'); const out = await fn(c); await c.query('COMMIT'); return out; }
    catch (err) { await c.query('ROLLBACK').catch(() => undefined); throw err; }
    finally { c.release(); }
  }

  private async ensureSubject(c: PoolClient, source: CatalogueSource): Promise<string | null> {
    await c.query(`INSERT INTO public.protocol_evidence_subjects (subject_id, protocol_id, method) VALUES ($1, $2, $3) ON CONFLICT (subject_id) DO NOTHING`, [source.sourceId, source.protocolId, source.method]);
    const { rows } = await c.query(`SELECT state_hash FROM public.protocol_evidence_subjects WHERE subject_id = $1 FOR UPDATE`, [source.sourceId]);
    return (rows[0]?.state_hash as string | null) ?? null;
  }

  private checkpointSql = `UPDATE public.protocol_watch_runs SET
      checkpoint = CASE WHEN checkpoint @> to_jsonb(ARRAY[$2::text]) THEN checkpoint ELSE checkpoint || to_jsonb(ARRAY[$2::text]) END,
      sources_succeeded = sources_succeeded + $3, sources_failed = sources_failed + $4, snapshots_written = snapshots_written + $5,
      observations_written = observations_written + $6, heartbeat_at = now()
    WHERE run_id = $1 AND status = 'RUNNING'`;

  async recordSuccess({ runId, source, result }: { runId: string; source: CatalogueSource; result: SuccessResult }) {
    return this.tx(async (c) => {
      const current = await this.ensureSubject(c, source);
      let snapshotWritten = false;
      if (current !== result.stateHash) {
        const ins = await c.query(`INSERT INTO public.protocol_evidence_snapshots (subject_id, run_id, state_hash, previous_hash, state) VALUES ($1, $2, $3, $4, $5::jsonb)
                                   ON CONFLICT (subject_id, run_id) DO NOTHING`, [source.sourceId, runId, result.stateHash, current, JSON.stringify(result.state)]);
        snapshotWritten = (ins.rowCount ?? 0) > 0;
      }
      let observationsWritten = 0;
      for (const o of result.observations) {
        const ins = await c.query(`INSERT INTO public.protocol_evidence_observations (observation_id, subject_id, protocol_id, run_id, kind, title, url, source_published_at, detail)
                                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb) ON CONFLICT (observation_id) DO NOTHING`,
          [o.observationId, source.sourceId, source.protocolId, runId, o.kind, o.title, o.url, o.sourcePublishedAt, JSON.stringify(o.detail)]);
        observationsWritten += ins.rowCount ?? 0;
      }
      await c.query(`UPDATE public.protocol_evidence_subjects SET last_attempt_at = now(), last_success_at = now(), last_outcome = 'SUCCESS', consecutive_failures = 0,
                     last_http_status = $2, last_error_code = NULL, last_run_id = $3, updated_at = now() WHERE subject_id = $1`, [source.sourceId, result.httpStatus, runId]);
      const upd = await c.query(this.checkpointSql, [runId, source.sourceId, 1, 0, snapshotWritten ? 1 : 0, observationsWritten]);
      if ((upd.rowCount ?? 0) !== 1) throw new Error('protocol-watch: run is no longer RUNNING (abandoned or finished elsewhere)');
      return { snapshotWritten, observationsWritten };
    });
  }

  async recordFailure({ runId, source, outcome, httpStatus, errorCode }: { runId: string; source: CatalogueSource; outcome: 'FAILED' | 'UNAVAILABLE'; httpStatus: number | null; errorCode: string }) {
    await this.tx(async (c) => {
      await this.ensureSubject(c, source);
      await c.query(`UPDATE public.protocol_evidence_subjects SET last_attempt_at = now(), last_outcome = $2, consecutive_failures = consecutive_failures + 1,
                     last_http_status = $3, last_error_code = $4, last_run_id = $5, updated_at = now() WHERE subject_id = $1`,
        [source.sourceId, outcome, httpStatus, errorCode.replace(/[^A-Z0-9_]/g, '_').slice(0, 80), runId]);
      const upd = await c.query(this.checkpointSql, [runId, source.sourceId, 0, 1, 0, 0]);
      if ((upd.rowCount ?? 0) !== 1) throw new Error('protocol-watch: run is no longer RUNNING (abandoned or finished elsewhere)');
    });
  }

  async heartbeat(runId: string, delta: Partial<RunCounters>) {
    await this.q(`UPDATE public.protocol_watch_runs SET sources_skipped = sources_skipped + $2, request_count = request_count + $3, heartbeat_at = now() WHERE run_id = $1 AND status = 'RUNNING'`,
      [runId, delta.skipped ?? 0, delta.requests ?? 0]);
  }

  async finishRun(runId: string, status: Exclude<RunStatus, 'RUNNING'>, errorSummary: string | null) {
    await this.q(`UPDATE public.protocol_watch_runs SET status = $2, finished_at = now(), heartbeat_at = now(), error_summary = $3 WHERE run_id = $1 AND status = 'RUNNING'`,
      [runId, status, errorSummary ? errorSummary.slice(0, 2000) : null]);
  }
}
