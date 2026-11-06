/**
 * The ONE shared protocol watch engine (Phase 2). One scheduler, one bounded run over the reviewed catalogue,
 * instead of a job per protocol.
 *
 *  - Concurrency 2 (and the HTTP layer serialises per host), a request budget, a wall-clock deadline.
 *  - Idempotent per slot: the run_key identifies the 12-hour slot (or the manual dispatch); re-running a finished
 *    slot does nothing, re-running an interrupted one resumes after its checkpoint.
 *  - A source skipped for budget, deadline or GitHub rate floor is SKIPPED (not attempted, not failed, not deleted).
 *  - A 404/410 is UNAVAILABLE; any other error is FAILED. Neither ever removes evidence.
 *
 * Phase 3 extension point: collectorFor() in collectors.ts is the only place a new method is added; the store,
 * receipts, checkpointing and write-on-change history are reused unchanged.
 */
import { automatedSources, type CatalogueSource, type WatchCatalogue } from './catalogue';
import { collectorFor } from './collectors';
import { WatchHttp, WatchHttpError } from './http';
import type { RunStatus, WatchStore } from './store';

export const RUN_CONCURRENCY = 2;
export const RUN_DEADLINE_MS = 15 * 60_000;
export const SLOT_HOURS = 12;

/** One key per 12-hour UTC slot: schedule:2026-11-06t00 / schedule:2026-11-06t12. */
export function scheduleSlotKey(now: Date): string {
  const slot = Math.floor(now.getUTCHours() / SLOT_HOURS) * SLOT_HOURS;
  return `schedule:${now.toISOString().slice(0, 10)}t${String(slot).padStart(2, '0')}`;
}
export function manualRunKey(githubRunId: string | undefined, now: Date): string {
  const id = (githubRunId ?? '').replace(/[^0-9]/g, '').slice(0, 30);
  return id ? `manual:gh-${id}` : `manual:local-${now.toISOString().replace(/[^0-9]/g, '').slice(0, 14)}`;
}

export interface SourceOutcome { sourceId: string; outcome: 'SUCCESS' | 'FAILED' | 'UNAVAILABLE' | 'SKIPPED' | 'ALREADY_DONE'; detail?: string; snapshotWritten?: boolean; observations?: number }
export interface RunReport {
  runKey: string; runId: string | null; status: RunStatus | 'NOOP_ALREADY_FINISHED'; resumed: boolean;
  planned: number; blocked: number; outcomes: SourceOutcome[]; requests: number; githubRateFloorHit: boolean;
}

export async function runWatch(opts: {
  catalogue: WatchCatalogue; store: WatchStore; http: WatchHttp; runKey: string; trigger: 'schedule' | 'manual';
  now?: () => number; deadlineMs?: number; concurrency?: number; log?: (line: string) => void;
}): Promise<RunReport> {
  const now = opts.now ?? Date.now;
  const log = opts.log ?? (() => undefined);
  const sources = automatedSources(opts.catalogue);
  const blocked = opts.catalogue.sources.length - sources.length;
  const began = await opts.store.beginRun({ runKey: opts.runKey, catalogueVersion: opts.catalogue.version, trigger: opts.trigger, planned: sources.length });
  if (began.alreadyFinished) {
    log(`run ${opts.runKey} already finished; nothing to do (idempotent)`);
    return { runKey: opts.runKey, runId: began.runId, status: 'NOOP_ALREADY_FINISHED', resumed: false, planned: sources.length, blocked, outcomes: [], requests: 0, githubRateFloorHit: false };
  }
  const started = now();
  const deadline = started + (opts.deadlineMs ?? RUN_DEADLINE_MS);
  const outcomes: SourceOutcome[] = [];
  const queue = [...sources];
  let skipped = 0;
  let fatal: Error | null = null;

  const one = async (source: CatalogueSource): Promise<void> => {
    if (began.checkpoint.has(source.sourceId)) { outcomes.push({ sourceId: source.sourceId, outcome: 'ALREADY_DONE' }); return; }
    if (now() > deadline) { skipped++; outcomes.push({ sourceId: source.sourceId, outcome: 'SKIPPED', detail: 'DEADLINE' }); return; }
    if (source.method === 'GITHUB_REPOSITORY' && opts.http.githubStopped) { skipped++; outcomes.push({ sourceId: source.sourceId, outcome: 'SKIPPED', detail: 'RATE_LIMIT_FLOOR' }); return; }
    const collect = collectorFor(source);
    if (!collect) { skipped++; outcomes.push({ sourceId: source.sourceId, outcome: 'SKIPPED', detail: 'NO_COLLECTOR' }); return; }
    let result;
    try {
      result = await collect(source, opts.http, await opts.store.previousState(source.sourceId));
    } catch (err) {
      if (err instanceof WatchHttpError && (err.code === 'BUDGET_EXHAUSTED' || err.code === 'RATE_LIMIT_FLOOR')) {
        skipped++; outcomes.push({ sourceId: source.sourceId, outcome: 'SKIPPED', detail: err.code }); return;
      }
      const code = err instanceof WatchHttpError ? err.code : 'COLLECTOR_ERROR';
      await opts.store.recordFailure({ runId: began.runId, source, outcome: 'FAILED', httpStatus: err instanceof WatchHttpError ? err.status : null, errorCode: code });
      outcomes.push({ sourceId: source.sourceId, outcome: 'FAILED', detail: code });
      log(`${source.sourceId}: FAILED ${code}`);
      return;
    }
    if (result.outcome === 'UNAVAILABLE') {
      await opts.store.recordFailure({ runId: began.runId, source, outcome: 'UNAVAILABLE', httpStatus: result.httpStatus, errorCode: result.errorCode });
      outcomes.push({ sourceId: source.sourceId, outcome: 'UNAVAILABLE', detail: result.errorCode });
      log(`${source.sourceId}: UNAVAILABLE ${result.errorCode} (recorded as unavailable, never as deleted)`);
      return;
    }
    const w = await opts.store.recordSuccess({ runId: began.runId, source, result });
    outcomes.push({ sourceId: source.sourceId, outcome: 'SUCCESS', snapshotWritten: w.snapshotWritten, observations: w.observationsWritten });
    log(`${source.sourceId}: SUCCESS${w.snapshotWritten ? ' · state changed' : ' · unchanged'}${w.observationsWritten ? ` · ${w.observationsWritten} new observation(s)` : ''}`);
  };

  const worker = async () => {
    while (queue.length && !fatal) {
      const s = queue.shift()!;
      try { await one(s); } catch (err) { fatal = err as Error; }
    }
  };
  await Promise.all(Array.from({ length: Math.max(1, Math.min(opts.concurrency ?? RUN_CONCURRENCY, 4)) }, worker));

  const counted = outcomes.filter((o) => o.outcome !== 'ALREADY_DONE');
  const ok = counted.filter((o) => o.outcome === 'SUCCESS').length;
  const bad = counted.filter((o) => o.outcome === 'FAILED' || o.outcome === 'UNAVAILABLE').length;
  const status: Exclude<RunStatus, 'RUNNING'> = fatal ? 'FAILED' : bad === 0 && skipped === 0 ? 'SUCCEEDED' : ok === 0 && bad > 0 ? 'FAILED' : 'PARTIAL';
  try {
    await opts.store.heartbeat(began.runId, { skipped, requests: opts.http.requests });
    await opts.store.finishRun(began.runId, status, fatal ? `store error: ${(fatal as Error).message}` : bad ? `${bad} source(s) failed or unavailable` : skipped ? `${skipped} source(s) skipped` : null);
  } catch (err) { if (!fatal) fatal = err as Error; }
  if (fatal) throw fatal;
  return { runKey: opts.runKey, runId: began.runId, status, resumed: began.resumed, planned: sources.length, blocked, outcomes, requests: opts.http.requests, githubRateFloorHit: opts.http.githubStopped };
}
