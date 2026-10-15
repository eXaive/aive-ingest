/**
 * Workflow Failure Alert — a GitHub issue per failing scheduled workflow.
 *
 * Runs from .github/workflows/workflow-failure-alert.yml on `workflow_run: completed`.
 * It never touches the job it reports on: the 15 scheduled workflows are unchanged
 * and know nothing about it.
 *
 * WHAT IS AN ALERT. Conclusion failure, timed_out or startup_failure. success,
 * cancelled, skipped and neutral produce nothing — the workflow's job-level `if`
 * already skips them, and this worker re-checks so it is safe on its own.
 *
 * ONE ISSUE PER WORKFLOW, ONE ENTRY PER RUN. While a workflow has an OPEN issue
 * labelled `workflow-failure`, each new failed run of it becomes a comment on that
 * issue rather than a new issue. Closing the issue re-arms it: the next failure
 * opens a fresh one. The issue is assigned to the repository owner, so GitHub
 * delivers it as a notification email.
 *
 * A RE-RUN NEVER ALERTS TWICE. Each issue body or comment carries a per-run marker
 * (run id, not attempt). Before writing, recent labelled issues and recent comments
 * are searched for it — open or closed — and a hit means nothing is sent. The
 * workflow's concurrency group per workflow name serialises alerts for one workflow,
 * so two failures cannot race to open two issues.
 *
 * EMAIL IS DISABLED. The Resend path (sendEmail) is kept but EMAIL_ENABLED is
 * false: aive.global has no verified Resend sending domain (verify.aive.global
 * status "failed", 2026-10-15). Issues are the channel even if Resend secrets exist.
 *
 * THIS REPO IS PUBLIC, SO THESE LOGS ARE PUBLIC. Printed: run id, workflow name,
 * channel and outcome. Never a token, an address or a response body.
 */

export const ALERT_CONCLUSIONS: ReadonlySet<string> = new Set(['failure', 'timed_out', 'startup_failure']);
export const ALERT_LABEL = 'workflow-failure';
/** Off until a Resend sending domain is verified for aive.global. */
export const EMAIL_ENABLED = false;

const FAILED_JOB = new Set(['failure', 'timed_out', 'startup_failure']);
const GH_API = 'https://api.github.com';
const RESEND_API = 'https://api.resend.com/emails';
/* resend.dev is Resend's shared test sender; it only delivers to the Resend
   account owner. Set ALERT_EMAIL_FROM once a sending domain is verified. */
const DEFAULT_FROM = 'AIVE Ingest Alerts <onboarding@resend.dev>';

type Env = Record<string, string | undefined>;
type Fetch = typeof fetch;
type Alert = { subject: string; text: string };

export interface RunFacts {
  id: number; name: string; runNumber: number; attempt: number; event: string;
  conclusion: string; url: string; headSha: string; failures: { job: string; step: string }[];
}

export const runMarker = (runId: number) => `<!-- workflow-failure-alert run_id=${runId} -->`;
export const workflowMarker = (name: string) => `<!-- workflow-failure-alert workflow=${JSON.stringify(name)} -->`;

function requireEnv(env: Env, name: string): string {
  const v = env[name]?.trim();
  if (!v) throw new Error(`${name} is not set`);
  return v;
}

async function gh(f: Fetch, env: Env, path: string, init: RequestInit = {}, allow404 = false): Promise<any> {
  const res = await f(`${GH_API}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${requireEnv(env, 'GITHUB_TOKEN')}`,
      Accept: 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      ...(init.body ? { 'Content-Type': 'application/json' } : {}),
    },
  });
  if (allow404 && res.status === 404) return null;
  if (!res.ok) throw new Error(`GitHub ${init.method ?? 'GET'} ${path.split('?')[0]} failed: HTTP ${res.status}`);
  return res.status === 204 ? null : res.json();
}

/** Run metadata plus every failed job and its first failed step, for this attempt. */
export async function readRun(f: Fetch, env: Env, repo: string, runId: number): Promise<RunFacts> {
  const run = await gh(f, env, `/repos/${repo}/actions/runs/${runId}`);
  const attempt = Number(run.run_attempt ?? 1);
  const jobs = await gh(f, env, `/repos/${repo}/actions/runs/${runId}/attempts/${attempt}/jobs?per_page=100`);
  const failures = (jobs.jobs ?? [])
    .filter((j: any) => FAILED_JOB.has(j.conclusion))
    .map((j: any) => ({
      job: String(j.name),
      step: String((j.steps ?? []).find((s: any) => FAILED_JOB.has(s.conclusion))?.name ?? '(no failing step reported)'),
    }));
  return {
    id: runId, name: String(run.name), runNumber: Number(run.run_number), attempt, event: String(run.event),
    conclusion: String(run.conclusion), url: String(run.html_url), headSha: String(run.head_sha).slice(0, 8),
    failures: failures.length ? failures : [{ job: '(none reported)', step: '(no failing step reported)' }],
  };
}

export function composeAlert(r: RunFacts): Alert {
  const subject = `[aive-ingest] ${r.conclusion.toUpperCase()}: ${r.name} #${r.runNumber}`;
  const text = [
    `Workflow: ${r.name}`,
    `Run: #${r.runNumber} (attempt ${r.attempt}, ${r.event}, ${r.headSha})`,
    `Conclusion: ${r.conclusion}`,
    ...r.failures.map((x) => `Failing step: ${x.job} › ${x.step}`),
    `Run URL: ${r.url}`,
  ].join('\n');
  return { subject, text };
}

/** DISABLED (EMAIL_ENABLED = false). Kept so email can be switched on without new code. */
export async function sendEmail(f: Fetch, env: Env, r: RunFacts, alert: Alert): Promise<void> {
  const res = await f(RESEND_API, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${requireEnv(env, 'RESEND_API_KEY')}`,
      'Content-Type': 'application/json',
      'Idempotency-Key': `aive-ingest-run-${r.id}`,
    },
    body: JSON.stringify({
      from: env.ALERT_EMAIL_FROM?.trim() || DEFAULT_FROM,
      to: [requireEnv(env, 'ALERT_EMAIL_TO')],
      subject: alert.subject,
      text: alert.text,
    }),
  });
  if (!res.ok) throw new Error(`Resend send failed: HTTP ${res.status}`);
}

async function ensureLabel(f: Fetch, env: Env, repo: string): Promise<void> {
  if (await gh(f, env, `/repos/${repo}/labels/${ALERT_LABEL}`, {}, true)) return;
  await gh(f, env, `/repos/${repo}/labels`, {
    method: 'POST',
    body: JSON.stringify({ name: ALERT_LABEL, color: 'B60205', description: 'A scheduled workflow is failing (workflow-failure-alert)' }),
  });
}

/** 'issue' = opened, 'comment' = added to the workflow's open issue, 'duplicate' = this run already recorded. */
export async function recordInIssue(f: Fetch, env: Env, repo: string, r: RunFacts, alert: Alert): Promise<'issue' | 'comment' | 'duplicate'> {
  const mark = runMarker(r.id);
  const labelled: any[] = await gh(f, env,
    `/repos/${repo}/issues?state=all&labels=${ALERT_LABEL}&sort=created&direction=desc&per_page=100`) ?? [];
  if (labelled.some((i) => String(i.body ?? '').includes(mark))) return 'duplicate';
  const comments: any[] = await gh(f, env,
    `/repos/${repo}/issues/comments?sort=created&direction=desc&per_page=100`) ?? [];
  if (comments.some((c) => String(c.body ?? '').includes(mark))) return 'duplicate';

  const open = labelled.find((i) => i.state === 'open' && !i.pull_request && String(i.body ?? '').includes(workflowMarker(r.name)));
  if (open) {
    await gh(f, env, `/repos/${repo}/issues/${open.number}/comments`, {
      method: 'POST',
      body: JSON.stringify({ body: `Failed again.\n\n${alert.text}\n\n${mark}` }),
    });
    return 'comment';
  }
  await ensureLabel(f, env, repo);
  await gh(f, env, `/repos/${repo}/issues`, {
    method: 'POST',
    body: JSON.stringify({
      title: `[aive-ingest] Failing: ${r.name}`,
      body: `${alert.text}\n\nLater failures of this workflow are added here as comments until this issue is closed.\n\n${workflowMarker(r.name)}\n${mark}`,
      labels: [ALERT_LABEL],
      assignees: [repo.split('/')[0]],
    }),
  });
  return 'issue';
}

export async function run(env: Env = process.env, f: Fetch = fetch): Promise<'none' | 'email' | 'issue' | 'comment' | 'duplicate'> {
  const conclusion = env.RUN_CONCLUSION?.trim() ?? '';
  const runId = Number(requireEnv(env, 'RUN_ID'));
  if (!ALERT_CONCLUSIONS.has(conclusion)) {
    console.log(`[failure-alert] run ${runId} conclusion=${conclusion || '(none)'} — no alert`);
    return 'none';
  }
  const repo = requireEnv(env, 'GITHUB_REPOSITORY');
  const facts = await readRun(f, env, repo, runId);
  const alert = composeAlert(facts);
  if (EMAIL_ENABLED && env.RESEND_API_KEY?.trim() && env.ALERT_EMAIL_TO?.trim()) {
    await sendEmail(f, env, facts, alert);
    console.log(`[failure-alert] run ${runId} "${facts.name}" #${facts.runNumber} — email sent`);
    return 'email';
  }
  const outcome = await recordInIssue(f, env, repo, facts, alert);
  const said = { issue: 'issue opened', comment: 'comment added to open issue', duplicate: 'already recorded, nothing sent' }[outcome];
  console.log(`[failure-alert] run ${runId} "${facts.name}" #${facts.runNumber} — ${said}`);
  return outcome;
}

if (require.main === module) {
  run().catch((e) => {
    console.error(`[failure-alert] FAILED: ${e instanceof Error ? e.message : String(e)}`);
    process.exit(1);
  });
}
