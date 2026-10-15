/**
 * workers/mcp-watch-evaluation-trigger.ts
 *
 * Run by collect-mcp-tools.yml, step "Evaluate MCP Watches", directly after "Collect tools/list" — and only while
 * repo variable MCP_WATCH_MONITOR_TICK_ENABLED == 'true' (unset = the step is skipped). Coupling rationale:
 * aive-platform docs/mcp/watch-evaluation-ingest-coupling.md.
 *
 * Dumb wake-up for the app's fenced monitor tick (aive-platform runMonitorTick), same shape as mcp-change-scan-trigger.ts.
 * It carries NO evaluation logic and asserts NOTHING about the collection: the body is exactly { trigger: 'MONITOR_TICK' }
 * (the route rejects any other key), and the app claims due watches from its own checkpoint table under a lease.
 *
 * THIS REPO IS PUBLIC. SO ARE ITS ACTION LOGS. Numeric counts and short codes only: never the response body, never a
 * watch/server/owner/event id, never the token. The token travels only in the Authorization header, from a repo secret
 * exposed to this one step through env (GitHub masks it in logs); it is never an argument or printed.
 *
 * Exit (the red Actions run IS the alert, as everywhere here):
 *   0  tick finished: no evaluation failed and no write was uncertain. Watches skipped because their lease went stale
 *      or their authority changed are not failures. Notification outcomes never fail the tick (app contract); failed /
 *      unknown deliveries print a ::warning:: with their codes.
 *   1  missing config (nothing called), network error, any non-2xx (503 uncertain writes / MONITORING_DISABLED /
 *      SETUP_REQUIRED / MONITORING_UNAVAILABLE, 401, 400), an unrecognised 200 body, or failed > 0 (a coverage gap:
 *      the next tick or a manual dispatch is the retry — no automatic retry, consistent with every other trigger here)
 *
 * Env:
 *   AIVE_BASE_URL               deployed app origin (repo SECRET, as for the other app triggers)
 *   MCP_WATCH_EVALUATOR_TOKEN   dedicated evaluator bearer — NOT AIVE_CRON_SECRET, NOT the database URL
 */

type Notifications = { sent?: number; failed?: number; unknown?: number; skipped?: number; failures?: unknown[] };
type TickSummary = {
  status?: string; evaluated?: number; events?: number; failed?: number; uncertainWrites?: number; skipped?: number;
  durationMs?: number; backlog?: unknown; notifications?: Notifications;
};

const n = (v: unknown) => (typeof v === 'number' && Number.isFinite(v) ? v : null);
/** Only short upper-case codes reach the public log; anything else is counted as OTHER. */
const codeCounts = (codes: unknown[] | undefined) => {
  const counts: Record<string, number> = {};
  for (const c of codes ?? []) { const k = typeof c === 'string' && /^[A-Z0-9_]{1,40}$/.test(c) ? c : 'OTHER'; counts[k] = (counts[k] ?? 0) + 1; }
  return Object.entries(counts).map(([k, v]) => `${k}=${v}`).join(',') || 'none';
};
/** Backlog numbers only (due / leased / failed / unseeded); timestamps and anything else stay out of the log. */
const backlogLine = (b: unknown) => {
  if (!b || typeof b !== 'object') return 'unknown';
  const r = b as Record<string, unknown>;
  return ['due', 'leased', 'failed', 'unseeded'].map(k => `${k}=${n(r[k]) ?? '?'}`).join(',');
};

export async function triggerWatchEvaluation(env: Record<string, string | undefined>, fetchImpl: typeof fetch = fetch,
  log: (line: string) => void = console.log): Promise<number> {
  const base = env.AIVE_BASE_URL?.trim(), token = env.MCP_WATCH_EVALUATOR_TOKEN?.trim();
  if (!base || !token) { log('[mcp-watch-evaluation-trigger] not configured (AIVE_BASE_URL / MCP_WATCH_EVALUATOR_TOKEN) -- nothing called'); return 1; }
  let res: Response;
  try {
    res = await fetchImpl(`${base.replace(/\/$/, '')}/api/internal/mcp/watch-evaluation`, {
      method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ trigger: 'MONITOR_TICK' }) });
  } catch { log('[mcp-watch-evaluation-trigger] request failed (network)'); return 1; }
  let s: TickSummary = {};
  try { const parsed = JSON.parse(await res.text()); if (parsed && typeof parsed === 'object') s = parsed as TickSummary; } catch { /* status line below still prints */ }
  const status = typeof s.status === 'string' && /^[A-Z_]{1,40}$/.test(s.status) ? s.status : null;
  const notes = s.notifications ?? {};
  log(`[mcp-watch-evaluation-trigger] HTTP ${res.status}${status ? ` status=${status}` : ''} evaluated=${n(s.evaluated) ?? '?'} events=${n(s.events) ?? '?'}` +
    ` failed=${n(s.failed) ?? '?'} uncertain=${n(s.uncertainWrites) ?? '?'} skipped=${n(s.skipped) ?? '?'} durationMs=${n(s.durationMs) ?? '?'}` +
    ` backlog=${backlogLine(s.backlog)} notifications=sent:${n(notes.sent) ?? 0},failed:${n(notes.failed) ?? 0},unknown:${n(notes.unknown) ?? 0},skipped:${n(notes.skipped) ?? 0}`);
  if (!res.ok) return 1;
  const counts = [s.evaluated, s.events, s.failed, s.uncertainWrites, s.skipped].map(n);
  if (counts.some(c => c === null)) { log('[mcp-watch-evaluation-trigger] unrecognised response (not a monitor-tick summary)'); return 1; }
  if ((n(notes.failed) ?? 0) + (n(notes.unknown) ?? 0) > 0) log(`::warning::watch notifications not confirmed: ${codeCounts(notes.failures)}`);
  return counts[2] === 0 && counts[3] === 0 ? 0 : 1;
}

if (require.main === module) {
  triggerWatchEvaluation(process.env).then(code => { process.exitCode = code; }, () => { console.error('[mcp-watch-evaluation-trigger] failed'); process.exitCode = 1; });
}
