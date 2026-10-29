/**
 * workers/publishing-calendar.ts — hourly trigger for the AIVE publishing calendar executor.
 *
 * AIVE (aive.global) holds the owner-approved weekly batches, the provider credentials and every publication rule
 * (window, pause, revoke, one attempt per item, uncertain-outcome block). This worker only asks it to run and turns
 * anything needing attention into a RED run, which the Workflow Failure Alert emails. It never sees a provider
 * credential, an article or a provider response body.
 *
 * THIS REPO IS PUBLIC, SO THESE LOGS ARE PUBLIC: only platform names, outcome codes and counts are printed.
 */
export type CalendarResult = { platform: string; plannedDate: string; outcome: string; attention: boolean };
export type CalendarSummary = { dryRun?: boolean; paused?: boolean; due?: number; results?: CalendarResult[]; attention?: boolean; error?: string };

const OUTCOME_CODE = /^(PUBLISHED \+ VERIFIED|PUBLISHED, public check (incomplete|failed)|OUTCOME UNKNOWN|REFUSED|PRECHECK_FAILED|NOT_RESERVED|DUE)/;

/** Public log lines and exit code for one run summary. Outcome text is cut to its leading code (no details). */
export function summarize(httpStatus: number, body: CalendarSummary | null): { lines: string[]; exitCode: number } {
  if (httpStatus !== 200 || !body) return { lines: [`publishing-calendar: app responded HTTP ${httpStatus}${body?.error ? ` (${String(body.error).replace(/[^a-z_]/g, '').slice(0, 40)})` : ''}`], exitCode: 1 };
  const lines = [`publishing-calendar: paused=${Boolean(body.paused)} due=${Number(body.due) || 0}${body.dryRun ? ' (dry run)' : ''}`];
  for (const r of body.results || []) lines.push(`  ${String(r.platform).replace(/[^a-z]/g, '')} ${String(r.plannedDate).replace(/[^0-9-]/g, '')}: ${(OUTCOME_CODE.exec(String(r.outcome)) || ['OTHER'])[0]}`);
  return { lines, exitCode: body.attention ? 1 : 0 };
}

function requireEnv(name: string): string {
  const v = (process.env[name] || '').trim();
  if (!v) { console.error(`publishing-calendar: missing ${name}`); process.exit(1); }
  return v;
}

async function main() {
  const base = requireEnv('AIVE_BASE_URL').replace(/\/+$/, ''), secret = requireEnv('AIVE_CRON_SECRET');
  const dryRun = process.env.CALENDAR_DRY_RUN === '1';
  let status = 0, body: CalendarSummary | null = null;
  try {
    const r = await fetch(`${base}/api/publishing/calendar/run${dryRun ? '?dryRun=1' : ''}`, { method: 'POST', headers: { Authorization: `Bearer ${secret}` }, redirect: 'error', signal: AbortSignal.timeout(290_000) });
    status = r.status; body = await r.json().catch(() => null);
  } catch { console.error('publishing-calendar: request failed before a response (network or timeout); the app records any reservation it made'); process.exit(1); }
  const { lines, exitCode } = summarize(status, body);
  for (const l of lines) console.log(l);
  process.exit(exitCode);
}

if (process.argv[1] && /publishing-calendar\.ts$/.test(process.argv[1])) main();
