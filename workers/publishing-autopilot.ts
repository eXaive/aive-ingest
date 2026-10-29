/**
 * workers/publishing-autopilot.ts — hourly trigger for the AIVE publishing autopilot (runs before the calendar step).
 *
 * AIVE holds the owner's standing publishing policy, the evidence, the model and provider credentials and every
 * check. This worker only asks it to advance the pipeline and turns anything needing attention (an article held by a
 * failed check, an uncertain provider outcome, a budget cap) into a RED run, which the Workflow Failure Alert emails.
 * It never sees a credential, an article, evidence or a model response.
 *
 * THIS REPO IS PUBLIC, SO THESE LOGS ARE PUBLIC: only platform names, slot dates and state codes are printed.
 */
export type AutopilotStage = { platform: string; slot: string; from: string; to: string; attention: boolean; detail?: string };
export type AutopilotSummary = { dryRun?: boolean; paused?: boolean; planned?: number; stages?: AutopilotStage[]; counts?: Record<string, number>; attention?: boolean; error?: string };

const STATE = /^[A-Z_]{3,20}$/;
const CODE = /^([A-Z][A-Z_]{2,40})/;

/** Public log lines and exit code for one tick summary. Reasons are cut to their leading code (no details). */
export function summarize(httpStatus: number, body: AutopilotSummary | null): { lines: string[]; exitCode: number } {
  if (httpStatus !== 200 || !body) return { lines: [`publishing-autopilot: app responded HTTP ${httpStatus}${body?.error ? ` (${String(body.error).replace(/[^a-z_]/g, '').slice(0, 40)})` : ''}`], exitCode: 1 };
  if (body.dryRun) {
    const counts = Object.entries(body.counts || {}).filter(([k]) => STATE.test(k)).map(([k, v]) => `${k}=${Number(v) || 0}`).join(' ');
    return { lines: [`publishing-autopilot: dry run paused=${Boolean(body.paused)} ${counts || 'no articles'}`], exitCode: 0 };
  }
  const lines = [`publishing-autopilot: planned=${Number(body.planned) || 0} stages=${(body.stages || []).length}`];
  for (const s of body.stages || []) {
    const from = STATE.test(s.from) ? s.from : 'OTHER', to = STATE.test(s.to) ? s.to : 'OTHER';
    const code = s.attention ? ` ${(CODE.exec(String(s.detail || '')) || ['ATTENTION'])[0]}` : '';
    lines.push(`  ${String(s.platform).replace(/[^a-z]/g, '')} ${String(s.slot).replace(/[^0-9-]/g, '')}: ${from} -> ${to}${code}`);
  }
  return { lines, exitCode: body.attention ? 1 : 0 };
}

function requireEnv(name: string): string {
  const v = (process.env[name] || '').trim();
  if (!v) { console.error(`publishing-autopilot: missing ${name}`); process.exit(1); }
  return v;
}

async function main() {
  const base = requireEnv('AIVE_BASE_URL').replace(/\/+$/, ''), secret = requireEnv('AIVE_CRON_SECRET');
  const dryRun = process.env.AUTOPILOT_DRY_RUN === '1';
  let status = 0, body: AutopilotSummary | null = null;
  try {
    const r = await fetch(`${base}/api/publishing/autopilot/tick${dryRun ? '?dryRun=1' : ''}`, { method: 'POST', headers: { Authorization: `Bearer ${secret}` }, redirect: 'error', signal: AbortSignal.timeout(295_000) });
    status = r.status; body = await r.json().catch(() => null);
  } catch { console.error('publishing-autopilot: request failed before a response (network or timeout); the app resumes the same stage next run'); process.exit(1); }
  const { lines, exitCode } = summarize(status, body);
  for (const l of lines) console.log(l);
  process.exit(exitCode);
}

if (process.argv[1] && /publishing-autopilot\.ts$/.test(process.argv[1])) main();
