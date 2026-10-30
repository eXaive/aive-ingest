/**
 * workers/publishing-monitor.ts — persistent, read-only production monitor for AIVE publishing. Polls
 * GET /api/publishing/monitor (aive.global) and turns this run RED when it reports `alert`, so the existing Workflow
 * Failure Alert opens (or comments on) its issue. Works with no Claude session open.
 *
 * The app endpoint reads four database functions only (calendar status, autopilot status, both trigger healths): no
 * provider call, no model call, no write. It alerts only on actionable, recent problems (missed or failing scheduled
 * runs, spend at the $10 operational stop, held / uncertain / unverified articles, missing receipts, duplicates, an
 * acceptance-gate breach, shadow isolation, Week 1 integrity); spend at $5 and owner pauses are warnings, not failures.
 * A 404 means the endpoint is not deployed yet: no alert.
 *
 * THIS REPO IS PUBLIC, SO THESE LOGS ARE PUBLIC: verdict, codes, platforms, dates and amounts only (details are cut to a
 * safe character set and length).
 */
export type MonitorReply = { verdict?: string; alert?: boolean; exceptions?: Array<{ code?: string; detail?: string }>; warnings?: Array<{ code?: string }>; spendMonthUsd?: number; acceptance?: string };

const CODE = /^[A-Z][A-Z0-9_]{2,60}$/;
const safe = (s: unknown) => String(s ?? '').replace(/[^A-Za-z0-9 _:$.,()-]/g, '').slice(0, 140);
const ACCEPTANCE = new Set(['n/a', 'waiting', 'open']);

/** Public log lines and exit code for one monitor reply. Fails closed on anything unexpected. */
export function summarize(httpStatus: number, body: MonitorReply | null): { lines: string[]; exitCode: number } {
  if (httpStatus === 404) return { lines: ['publishing-monitor: endpoint not deployed yet (no alert)'], exitCode: 0 };
  if (!body || typeof body.alert !== 'boolean' || !['OK', 'WARN', 'ALERT'].includes(String(body.verdict))) {
    return { lines: [`publishing-monitor: unusable reply (HTTP ${httpStatus})`], exitCode: 1 };
  }
  const exceptions = (body.exceptions || []).filter((f) => CODE.test(String(f.code)));
  const warnings = (body.warnings || []).map((w) => String(w.code)).filter((c) => CODE.test(c));
  const spend = Number(body.spendMonthUsd);
  const lines = [`publishing-monitor: verdict=${body.verdict} alert=${body.alert ? 'YES' : 'no'} spendMonth=$${Number.isFinite(spend) ? spend.toFixed(2) : '?'} acceptance=${ACCEPTANCE.has(String(body.acceptance)) ? body.acceptance : '-'}${warnings.length ? ` warnings=${warnings.join(',')}` : ''}`];
  for (const f of exceptions) lines.push(`  ${f.code}: ${safe(f.detail)}`);
  // A 500 with a well-formed ALERT body (MONITOR_UNAVAILABLE) is still a failure.
  return { lines, exitCode: body.alert || httpStatus !== 200 ? 1 : 0 };
}

function requireEnv(name: string): string {
  const v = (process.env[name] || '').trim();
  if (!v) { console.error(`publishing-monitor: missing ${name}`); process.exit(1); }
  return v;
}

async function main() {
  const base = requireEnv('AIVE_BASE_URL').replace(/\/+$/, ''), secret = requireEnv('AIVE_CRON_SECRET');
  let status = 0, body: MonitorReply | null = null;
  try {
    const r = await fetch(`${base}/api/publishing/monitor`, { headers: { Authorization: `Bearer ${secret}` }, redirect: 'error', signal: AbortSignal.timeout(60_000) });
    status = r.status; body = await r.json().catch(() => null);
  } catch { console.error('publishing-monitor: request failed before a response (network or timeout)'); process.exit(1); }
  const { lines, exitCode } = summarize(status, body);
  for (const l of lines) console.log(l);
  process.exit(exitCode);
}

if (process.argv[1] && /workers[\\/]publishing-monitor\.ts$/.test(process.argv[1])) main();
