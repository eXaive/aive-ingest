/**
 * workers/mcp-watch-evaluation-trigger.ts
 *
 * PREPARED, NOT WIRED. No workflow runs this file. The proposed wiring — one step in collect-mcp-tools.yml directly
 * after "Collect tools/list" — is owner-gated and documented in aive-platform
 * docs/mcp/watch-evaluation-ingest-coupling.md.
 *
 * Dumb trigger for the app's ingest-coupled Watch evaluation batch, same shape as mcp-change-scan-trigger.ts. It
 * carries NO evaluation logic and asserts NOTHING about the collection: it names the workflow run it belongs to
 * (GITHUB_RUN_ID / GITHUB_RUN_ATTEMPT — the tools collector records the same pair on its mcp_tool_collection_runs row)
 * and the app reads that run's terminal state from the database itself.
 *
 * THIS REPO IS PUBLIC. SO ARE ITS ACTION LOGS. Status and aggregate counts only: never the response body, never a
 * watch/server/owner id, never the token. The token travels only in the Authorization header, from a repo secret
 * exposed to this one step through env (GitHub masks it in logs); it is never an argument or printed.
 *
 * Exit (the red Actions run IS the alert, as everywhere here):
 *   0  batch COMPLETE, or SKIPPED (the collection itself did not finish — its own step already reports that)
 *   1  HTTP error / unreadable completion record (503), batch FAILED or PARTIAL (a coverage gap: a later run or a
 *      manual dispatch is the retry — no automatic retry, consistent with every other trigger here), missing config
 *
 * Env:
 *   AIVE_BASE_URL               deployed app origin (repo SECRET, as for the other app triggers)
 *   MCP_WATCH_EVALUATOR_TOKEN   dedicated evaluator bearer — NOT AIVE_CRON_SECRET, NOT the database URL
 *   GITHUB_RUN_ID, GITHUB_RUN_ATTEMPT   provided by Actions
 */

type Summary = {
  version?: string; status?: string; skipReason?: string | null; basis?: { state?: string };
  pages?: number; watchesConsidered?: number; watchesEvaluated?: number; watchesSkipped?: number; eventsRecorded?: number;
  counts?: Record<string, number>; failures?: { code: string; count: number }[]; stoppedBy?: string | null;
};

export async function triggerWatchEvaluation(env: Record<string, string | undefined>, fetchImpl: typeof fetch = fetch,
  log: (line: string) => void = console.log): Promise<number> {
  const base = env.AIVE_BASE_URL?.trim(), token = env.MCP_WATCH_EVALUATOR_TOKEN?.trim();
  if (!base || !token) { log('[mcp-watch-evaluation-trigger] not configured (AIVE_BASE_URL / MCP_WATCH_EVALUATOR_TOKEN) -- nothing called'); return 1; }
  const runId = env.GITHUB_RUN_ID?.trim(), attempt = Number(env.GITHUB_RUN_ATTEMPT ?? '');
  const body: Record<string, unknown> = { trigger: 'INGEST_COMPLETE' };
  if (runId && /^\d{1,20}$/.test(runId)) body.ingestWorkflowRunId = runId;
  if (body.ingestWorkflowRunId && Number.isInteger(attempt) && attempt >= 1 && attempt <= 100) body.ingestWorkflowRunAttempt = attempt;
  let res: Response;
  try {
    res = await fetchImpl(`${base.replace(/\/$/, '')}/api/internal/mcp/watch-evaluation`, {
      method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  } catch { log('[mcp-watch-evaluation-trigger] request failed (network)'); return 1; }
  let s: Summary = {};
  try { s = JSON.parse(await res.text()) as Summary; } catch { /* status line below still prints */ }
  const failures = (s.failures ?? []).map(f => `${f.code}=${f.count}`).join(',') || 'none';
  log(`[mcp-watch-evaluation-trigger] HTTP ${res.status} status=${s.status ?? 'unknown'} basis=${s.basis?.state ?? 'unknown'}` +
    `${s.skipReason ? ` skip=${s.skipReason}` : ''} pages=${s.pages ?? 0} considered=${s.watchesConsidered ?? 0} evaluated=${s.watchesEvaluated ?? 0}` +
    ` skipped=${s.watchesSkipped ?? 0} events=${s.eventsRecorded ?? 0} counts=${JSON.stringify(s.counts ?? {})} failures=${failures} stoppedBy=${s.stoppedBy ?? 'none'}`);
  if (!res.ok || s.version !== 'MCP_WATCH_EVALUATION_BATCH_V1') return 1;
  if (s.status === 'SKIPPED') { log(`::warning::watch evaluation skipped: ${s.skipReason}`); return 0; }
  return s.status === 'COMPLETE' ? 0 : 1;
}

if (require.main === module) {
  triggerWatchEvaluation(process.env).then(code => { process.exitCode = code; }, () => { console.error('[mcp-watch-evaluation-trigger] failed'); process.exitCode = 1; });
}
