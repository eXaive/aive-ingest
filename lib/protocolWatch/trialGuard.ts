/**
 * Protocol Watch monitoring-trial guard (M1, 2026-11-06). Pure decision logic; no I/O.
 *
 * A GitHub repository variable never expires, so PROTOCOL_WATCH_ENABLED=true alone would keep collecting until a
 * person removes it. This guard runs as the first step of every Protocol Watch job (scheduled or manual) and decides,
 * from the trial window and the run receipts already in protocol_watch_runs, whether the collector may run.
 * It needs no person and no machine: expiry and the automatic stop rules hold even if the owner is unavailable.
 *
 * Fail-closed: no window, an unparseable window, an expired window, or a window longer than MAX_TRIAL_DAYS => skip.
 * Latching stop rules (a stop persists because no further run happens; resuming needs an owner decision):
 *   - the latest finished run FAILED or was ABANDONED;
 *   - the two latest finished runs were both PARTIAL;
 *   - the latest finished run wrote more than OBSERVATION_SURGE_LIMIT observations (review required).
 * PROTOCOL_WATCH_TRIAL_ACK may name the run_key of a reviewed run; that run and older ones are ignored by the stop
 * rules (the window check still applies).
 * Not decidable here (checked by the attended review instead): MCP-lane connection-limit failures in other
 * workflows, writes outside the evidence tables (prevented structurally by the store), and signal wording
 * (platform-side, fixed templates).
 */
export const OBSERVATION_SURGE_LIMIT = 50;
export const MAX_TRIAL_DAYS = 15;
const DAY_MS = 86_400_000;

export type TrialRunStatus = 'RUNNING' | 'SUCCEEDED' | 'PARTIAL' | 'FAILED' | 'ABANDONED';
export interface TrialRun { runKey: string; status: TrialRunStatus; observationsWritten: number; startedAt: string }

export type TrialDecisionCode =
  | 'OK' | 'NO_TRIAL_WINDOW' | 'INVALID_TRIAL_WINDOW' | 'TRIAL_WINDOW_TOO_LONG' | 'TRIAL_EXPIRED'
  | 'LAST_RUN_FAILED' | 'TWO_CONSECUTIVE_PARTIAL' | 'OBSERVATION_SURGE';
export interface TrialDecision { proceed: boolean; code: TrialDecisionCode; reason: string }

/** Strict ISO-8601 UTC instant (e.g. 2026-11-21T00:00:00Z). Anything else is invalid, never guessed. */
export function parseTrialUntil(raw: string | undefined | null): Date | null | 'MISSING' {
  const v = (raw ?? '').trim();
  if (!v) return 'MISSING';
  const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.\d{1,3})?)?Z$/.exec(v);
  if (!m) return null;
  const t = Date.parse(v);
  if (!Number.isFinite(t)) return null;
  const d = new Date(t);
  // Round-trip: Date.parse rolls impossible values over (2026-02-30 -> Mar 2, hour 24 -> next day). Reject them.
  if (d.getUTCFullYear() !== Number(m[1]) || d.getUTCMonth() + 1 !== Number(m[2]) || d.getUTCDate() !== Number(m[3])
    || d.getUTCHours() !== Number(m[4]) || d.getUTCMinutes() !== Number(m[5]) || d.getUTCSeconds() !== Number(m[6] ?? '0')) return null;
  return d;
}

export function decideTrial(input: { now: Date; untilRaw: string | undefined | null; ackRunKey?: string | null; runs: readonly TrialRun[] }): TrialDecision {
  const until = parseTrialUntil(input.untilRaw);
  if (until === 'MISSING') return { proceed: false, code: 'NO_TRIAL_WINDOW', reason: 'PROTOCOL_WATCH_TRIAL_UNTIL is not set; collection runs only inside an explicit trial window.' };
  if (until === null) return { proceed: false, code: 'INVALID_TRIAL_WINDOW', reason: 'PROTOCOL_WATCH_TRIAL_UNTIL is not a strict ISO-8601 UTC instant (e.g. 2026-11-21T00:00:00Z).' };
  if (input.now.getTime() >= until.getTime()) return { proceed: false, code: 'TRIAL_EXPIRED', reason: `Trial window ended at ${until.toISOString()}.` };
  if (until.getTime() - input.now.getTime() > MAX_TRIAL_DAYS * DAY_MS) return { proceed: false, code: 'TRIAL_WINDOW_TOO_LONG', reason: `Trial window ends more than ${MAX_TRIAL_DAYS} days from now; refusing.` };

  const finished = [...input.runs].filter((r) => r.status !== 'RUNNING').sort((a, b) => b.startedAt.localeCompare(a.startedAt));
  const ackIdx = input.ackRunKey ? finished.findIndex((r) => r.runKey === input.ackRunKey) : -1;
  const considered = ackIdx >= 0 ? finished.slice(0, ackIdx) : finished;
  const [latest, previous] = considered;
  if (latest && (latest.status === 'FAILED' || latest.status === 'ABANDONED')) {
    return { proceed: false, code: 'LAST_RUN_FAILED', reason: `Latest run ${latest.runKey} ended ${latest.status}; stopped until reviewed.` };
  }
  if (latest?.status === 'PARTIAL' && previous?.status === 'PARTIAL') {
    return { proceed: false, code: 'TWO_CONSECUTIVE_PARTIAL', reason: `Runs ${previous.runKey} and ${latest.runKey} were both PARTIAL; stopped until reviewed.` };
  }
  if (latest && latest.observationsWritten > OBSERVATION_SURGE_LIMIT) {
    return { proceed: false, code: 'OBSERVATION_SURGE', reason: `Run ${latest.runKey} wrote ${latest.observationsWritten} observations (> ${OBSERVATION_SURGE_LIMIT}); stopped until reviewed against upstream activity.` };
  }
  return { proceed: true, code: 'OK', reason: `Inside the trial window (until ${until.toISOString()}); no stop rule tripped.` };
}
