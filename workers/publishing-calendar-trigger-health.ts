/**
 * workers/publishing-calendar-trigger-health.ts — after each GitHub calendar run, reads the health of AIVE's PRIMARY
 * calendar trigger (Supabase pg_cron at :08, which calls the same run endpoint as this workflow's :23 step) — or, with
 * TRIGGER=autopilot, the autopilot tick trigger (pg_cron at :38) — and turns
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
  ATTENTION: 'the latest run (calendar executor or autopilot tick) raised attention (check /owner/publishing)',
  HEALTH_UNAVAILABLE: 'the app could not read the trigger health',
};
const time = (v: unknown) => (typeof v === 'string' && /^\d{4}-\d\d-\d\d[T ]\d\d:\d\d/.test(v) ? v.slice(0, 16).replace(' ', 'T') + 'Z' : '-');

/** Public log lines and exit code for one health reply. Fails closed on anything malformed. */
/** Which database trigger to check: the calendar's (pg_cron :08, default) or the autopilot tick's (pg_cron :38). */
export const TRIGGERS = { calendar: 'publishing-calendar-trigger', autopilot: 'publishing-autopilot-trigger' } as const;
export type Trigger = keyof typeof TRIGGERS;

export function summarize(httpStatus: number, body: TriggerHealth | null, trigger: Trigger = 'calendar'): { lines: string[]; exitCode: number } {
  const label = TRIGGERS[trigger];
  if (httpStatus === 404) return { lines: [`${label}: health endpoint not deployed yet (expected before the app release; no alert)`], exitCode: 0 };
  if (httpStatus !== 200 || !body) return { lines: [`${label}: health unavailable, app responded HTTP ${httpStatus}`], exitCode: 1 };
  const verdict = VERDICT.test(String(body.verdict)) ? String(body.verdict) : 'OTHER';
  const credential = CREDENTIAL.test(String(body.credential)) ? String(body.credential) : 'unknown';
  const failures = Math.max(0, Math.min(999, Number(body.consecutiveFailures) || 0));
  if (typeof body.alert !== 'boolean' || verdict === 'OTHER') return { lines: [`${label}: malformed health reply (verdict=${verdict})`], exitCode: 1 };
  const lines = [`${label}: verdict=${verdict} alert=${body.alert ? 'YES' : 'no'} credential=${credential} lastRun=${time(body.lastRunAt)} lastHttp200=${time(body.lastDeliveredAt)} consecutiveFailures=${failures}`];
  if (body.alert) lines.push(`  ${HINT[verdict] ?? (credential !== 'match' ? `the app's trigger credential does not match the database (${credential})` : 'see the trigger health in AIVE')}`);
  return { lines, exitCode: body.alert ? 1 : 0 };
}

function requireEnv(name: string): string {
  const v = (process.env[name] || '').trim();
  if (!v) { console.error(`publishing-trigger-health: missing ${name}`); process.exit(1); }
  return v;
}

async function main() {
  const base = requireEnv('AIVE_BASE_URL').replace(/\/+$/, ''), secret = requireEnv('AIVE_CRON_SECRET');
  const trigger = (process.env.TRIGGER || 'calendar') as Trigger;
  if (!(trigger in TRIGGERS)) { console.error('publishing-trigger-health: TRIGGER must be calendar or autopilot'); process.exit(1); }
  let status = 0, body: TriggerHealth | null = null;
  try {
    const r = await fetch(`${base}/api/publishing/${trigger}/trigger-health`, { headers: { Authorization: `Bearer ${secret}` }, redirect: 'error', signal: AbortSignal.timeout(30_000) });
    status = r.status; body = await r.json().catch(() => null);
  } catch { console.error(`${TRIGGERS[trigger]}: health request failed before a response (network or timeout)`); process.exit(1); }
  const { lines, exitCode } = summarize(status, body, trigger);
  for (const l of lines) console.log(l);
  process.exit(exitCode);
}

if (process.argv[1] && /workers[\\/]publishing-calendar-trigger-health\.ts$/.test(process.argv[1])) main();
