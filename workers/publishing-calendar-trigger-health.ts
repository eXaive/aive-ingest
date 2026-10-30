/**
 * workers/publishing-calendar-trigger-health.ts — after each GitHub calendar run, reads the health of AIVE's PRIMARY
 * calendar trigger (Supabase pg_cron at :08, which calls the same run endpoint as this workflow's :23 step) and turns
 * this run RED when that trigger needs attention, so the Workflow Failure Alert reports it.
 *
 * Read-only: GET /api/publishing/calendar/trigger-health reads two database functions and publishes nothing.
 * Expected states never alert: NOT_INSTALLED (migration not applied), NOT_ACTIVATED, DEACTIVATED, WARMING_UP (first
 * hours after activation), a single transient delivery failure, and a 404 before the app route is deployed.
 *
 * BEST EFFORT. This check runs only when GitHub runs this workflow, and GitHub's own schedule for this repository is
 * unreliable (about one run in four, often hours late, observed 2026-10-30). It is a second line of detection with a
 * delay of hours, not a guarantee; on publication days the owner still checks the outcome.
 *
 * THIS REPO IS PUBLIC, SO THESE LOGS ARE PUBLIC: only the verdict, the credential state (match/mismatch/...), times and a
 * count are printed.
 */
export type TriggerHealth = {
  verdict?: string; alert?: boolean; credential?: string; lastRunAt?: string | null; lastDeliveredAt?: string | null; consecutiveFailures?: number; error?: string;
};

const VERDICT = /^[A-Z_]{2,24}$/;
const CREDENTIAL = /^(match|mismatch|unset|no_vault_entry|unknown)$/;
const HINT: Record<string, string> = {
  NO_JOB: 'the pg_cron job is missing or switched off outside the reviewed activation path',
  SECRET_MISSING: 'the Vault credential is missing, so the database sends nothing',
  NOT_RUNNING: 'pg_cron has not run the job for 90 minutes',
  DELIVERY_FAILED: 'consecutive requests to the run endpoint did not get HTTP 200',
  STALE: 'no successful delivery for over two hours',
  ATTENTION: 'the executor raised attention on its latest run (check /owner/publishing)',
  HEALTH_UNAVAILABLE: 'the app could not read the trigger health',
};
const time = (v: unknown) => (typeof v === 'string' && /^\d{4}-\d\d-\d\d[T ]\d\d:\d\d/.test(v) ? v.slice(0, 16).replace(' ', 'T') + 'Z' : '-');

/** Public log lines and exit code for one health reply. Fails closed on anything malformed. */
export function summarize(httpStatus: number, body: TriggerHealth | null): { lines: string[]; exitCode: number } {
  if (httpStatus === 404) return { lines: ['publishing-calendar-trigger: health endpoint not deployed yet (expected before the app release; no alert)'], exitCode: 0 };
  if (httpStatus !== 200 || !body) return { lines: [`publishing-calendar-trigger: health unavailable, app responded HTTP ${httpStatus}`], exitCode: 1 };
  const verdict = VERDICT.test(String(body.verdict)) ? String(body.verdict) : 'OTHER';
  const credential = CREDENTIAL.test(String(body.credential)) ? String(body.credential) : 'unknown';
  const failures = Math.max(0, Math.min(999, Number(body.consecutiveFailures) || 0));
  if (typeof body.alert !== 'boolean' || verdict === 'OTHER') return { lines: [`publishing-calendar-trigger: malformed health reply (verdict=${verdict})`], exitCode: 1 };
  const lines = [`publishing-calendar-trigger: verdict=${verdict} alert=${body.alert ? 'YES' : 'no'} credential=${credential} lastRun=${time(body.lastRunAt)} lastHttp200=${time(body.lastDeliveredAt)} consecutiveFailures=${failures}`];
  if (body.alert) lines.push(`  ${HINT[verdict] ?? (credential !== 'match' ? `the app's trigger credential does not match the database (${credential})` : 'see the trigger health in AIVE')}`);
  return { lines, exitCode: body.alert ? 1 : 0 };
}

function requireEnv(name: string): string {
  const v = (process.env[name] || '').trim();
  if (!v) { console.error(`publishing-calendar-trigger: missing ${name}`); process.exit(1); }
  return v;
}

async function main() {
  const base = requireEnv('AIVE_BASE_URL').replace(/\/+$/, ''), secret = requireEnv('AIVE_CRON_SECRET');
  let status = 0, body: TriggerHealth | null = null;
  try {
    const r = await fetch(`${base}/api/publishing/calendar/trigger-health`, { headers: { Authorization: `Bearer ${secret}` }, redirect: 'error', signal: AbortSignal.timeout(30_000) });
    status = r.status; body = await r.json().catch(() => null);
  } catch { console.error('publishing-calendar-trigger: health request failed before a response (network or timeout)'); process.exit(1); }
  const { lines, exitCode } = summarize(status, body);
  for (const l of lines) console.log(l);
  process.exit(exitCode);
}

if (process.argv[1] && /workers[\\/]publishing-calendar-trigger-health\.ts$/.test(process.argv[1])) main();
