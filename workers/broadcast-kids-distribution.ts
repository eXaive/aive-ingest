/**
 * Broadcast Kids Distribution — hourly execution-plane trigger for the 14 kids YouTube channels.
 *
 * THIS WORKER DECIDES NOTHING. It makes one authenticated call to the deployed AIVE app
 * (POST /api/broadcast/distribution/daily-run). AIVE is the control plane: due channels,
 * Ready ordering, frozen publication metadata, destination routing, the one-Short-per-
 * channel-per-day limit, stale-media checks, submission and reconciliation all live in
 * AIVE and its database. No database access, no Blotato or YouTube credentials here.
 *
 * THIS REPO IS PUBLIC, SO THESE LOGS ARE PUBLIC. Only channel slug, outcome code and state
 * are printed, each reduced to a safe identifier charset; never a body, URL or id.
 *
 * Required repo secrets: AIVE_BASE_URL (deployed app origin), AIVE_CRON_SECRET (Bearer).
 * Exit non-zero on any HTTP failure or on an outcome that needs the owner: a red run is the alert.
 */

/** Outcomes that need the owner (same set the AIVE executor reports as attention-worthy). */
export const ALERT_OUTCOMES: ReadonlySet<string> = new Set([
  'CHANNEL_ERROR', 'OUTCOME_UNKNOWN_NEEDS_OWNER', 'PROVIDER_FAILED_NO_RETRY', 'HALTED', 'HALTED_NEEDS_OWNER',
  'SUBMITTED_OUTCOME_UNKNOWN', 'CONFIRMED_NOT_RECORDED', 'NOT_PLANNED', 'VISIBILITY_NOT_RECORDED',
]);
/** Above the route's 300 s maxDuration so the app, not this client, bounds the run. */
export const TIMEOUT_MS = 320_000;
export const ROUTE = '/api/broadcast/distribution/daily-run';

export type Row = { channel: string; outcome: string; state: string | null };
type Env = Record<string, string | undefined>;

function requireEnv(env: Env, name: string): string {
  const value = env[name]?.trim();
  if (!value) throw new Error(`${name} is not set`);
  return value;
}
const safe = (value: unknown) => String(value ?? '').replace(/[^A-Za-z0-9_-]/g, '').slice(0, 64);

export async function runKidsDistribution(fetchImpl: typeof fetch = fetch, env: Env = process.env) {
  const base = requireEnv(env, 'AIVE_BASE_URL').replace(/\/$/, ''), secret = requireEnv(env, 'AIVE_CRON_SECRET');
  const res = await fetchImpl(`${base}${ROUTE}`, {
    method: 'POST', headers: { Authorization: `Bearer ${secret}` }, signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`daily-run failed: HTTP ${res.status}`);
  let body: { today?: unknown; report?: unknown };
  try { body = JSON.parse(await res.text()); } catch { throw new Error('daily-run returned unparseable JSON'); }
  if (!Array.isArray(body.report)) throw new Error('daily-run returned no report');
  const rows: Row[] = body.report.map((r: Record<string, unknown>) => ({
    channel: safe(r.channel), outcome: safe(r.outcome), state: r.state == null ? null : safe(r.state),
  }));
  return { today: safe(body.today), rows, alerts: rows.filter(r => ALERT_OUTCOMES.has(r.outcome)) };
}

async function main(): Promise<void> {
  const { today, rows, alerts } = await runKidsDistribution();
  for (const r of rows) console.log(`[kids-distribution] ${today} ${r.channel} ${r.outcome}${r.state ? ' ' + r.state : ''}`);
  if (alerts.length) {
    console.error(`[kids-distribution] ${alerts.length} outcome(s) need owner attention`);
    process.exit(1);
  }
}

if (require.main === module) {
  main().catch(error => { console.error(`[kids-distribution] ${error instanceof Error ? error.message : 'failed'}`); process.exit(1); });
}
