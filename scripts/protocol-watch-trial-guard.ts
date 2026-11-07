/**
 * Protocol Watch trial guard step (runs before workers/protocol-watch.ts in .github/workflows/protocol-watch.yml).
 * Writes proceed=true|false to $GITHUB_OUTPUT. Fail-closed: anything unexpected => proceed=false.
 * Reads only public.protocol_watch_runs (SELECT, as aive_ingest); writes nothing to the database.
 * Exit codes: 0 = decision made (including "skip"); 1 = run receipts unreadable (skip AND a red run, so it alerts).
 */
import { appendFileSync } from 'node:fs';
import { decideTrial, type TrialDecision, type TrialRun } from '../lib/protocolWatch/trialGuard';

function emit(decision: TrialDecision): void {
  const out = process.env.GITHUB_OUTPUT;
  if (out) appendFileSync(out, `proceed=${decision.proceed ? 'true' : 'false'}\ncode=${decision.code}\n`);
  const summary = process.env.GITHUB_STEP_SUMMARY;
  if (summary) appendFileSync(summary, `### Protocol Watch trial guard\n\n- decision: **${decision.proceed ? 'collect' : 'skip'}** (${decision.code})\n- ${decision.reason}\n`);
  console.log(`[trial-guard] ${decision.proceed ? 'PROCEED' : 'SKIP'} ${decision.code}: ${decision.reason}`);
}

async function main(): Promise<number> {
  const untilRaw = process.env.PROTOCOL_WATCH_TRIAL_UNTIL;
  const ackRunKey = (process.env.PROTOCOL_WATCH_TRIAL_ACK ?? '').trim() || null;
  const now = new Date();
  // Window checks need no database: decide them first so an expired trial never even connects.
  const windowOnly = decideTrial({ now, untilRaw, ackRunKey, runs: [] });
  if (!windowOnly.proceed) { emit(windowOnly); return 0; }
  const db = await import('../lib/ingest/db');
  try {
    const rows = await db.q<{ run_key: string; status: TrialRun['status']; observations_written: number; started_at: string | Date }>(
      'SELECT run_key, status, observations_written, started_at FROM public.protocol_watch_runs ORDER BY started_at DESC LIMIT 20');
    const runs: TrialRun[] = rows.map((r) => ({ runKey: r.run_key, status: r.status, observationsWritten: Number(r.observations_written) || 0,
      startedAt: r.started_at instanceof Date ? r.started_at.toISOString() : String(r.started_at) }));
    emit(decideTrial({ now, untilRaw, ackRunKey, runs }));
    return 0;
  } catch (err) {
    emit({ proceed: false, code: 'LAST_RUN_FAILED', reason: `Run receipts unreadable (${(err as Error).message.slice(0, 120)}); skipping.` });
    return 1;
  } finally {
    await db.endIngestPool().catch(() => undefined);
  }
}

main().then((code) => process.exit(code), (err) => { console.error(`[trial-guard] fatal: ${(err as Error).message}`); process.exit(1); });
