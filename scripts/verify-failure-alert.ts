/**
 * Workflow Failure Alert (workers/workflow-failure-alert.ts + its workflow).
 *
 *   1. COVERAGE: every workflow with a `schedule:` trigger is listed, by exact
 *      name, in workflow-failure-alert.yml — and nothing stale is listed.
 *   2. BEHAVIOUR, against an in-memory GitHub (issues, comments, labels):
 *      - success / cancelled / skipped / neutral: no request of any kind;
 *      - first failure of a workflow opens ONE labelled issue, assigned to the
 *        owner, naming workflow, run number, failing step and run URL;
 *      - a different failed run of the same workflow COMMENTS on that issue;
 *      - a re-delivery or re-run of an already recorded run sends nothing, even
 *        after its issue was closed;
 *      - after the issue is closed, the next failure opens a NEW issue;
 *      - another workflow gets its own issue;
 *      - email stays off even with Resend secrets set; the kept Resend path
 *        still builds the right request when called directly.
 *
 * NO NETWORK. fetch is injected.
 *
 * Run: npx tsx scripts/verify-failure-alert.ts
 */

import assert from 'node:assert/strict';
import { readdirSync, readFileSync } from 'node:fs';
import { run, runMarker, workflowMarker, sendEmail, composeAlert, EMAIL_ENABLED, ALERT_LABEL, type RunFacts } from '../workers/workflow-failure-alert';

let checks = 0;
const ok = (label: string) => { checks++; console.log(`  PASS  ${label}`); };

// ── 1. coverage ──────────────────────────────────────────────────────────────
const DIR = '.github/workflows';
const ALERT_FILE = 'workflow-failure-alert.yml';
const scheduled = readdirSync(DIR)
  .filter((f) => /\.ya?ml$/.test(f) && f !== ALERT_FILE)
  .map((f) => ({ f, src: readFileSync(`${DIR}/${f}`, 'utf8') }))
  .filter(({ src }) => /^\s+schedule:\s*$/m.test(src))
  .map(({ f, src }) => {
    const m = /^name:\s*(.+?)\s*$/m.exec(src);
    assert.ok(m, `${f} has no top-level name:`);
    return m[1].replace(/^['"]|['"]$/g, '');
  });
const alertSrc = readFileSync(`${DIR}/${ALERT_FILE}`, 'utf8');
const listBlock = /workflows:\s*\n((?:\s+- .+\n)+)/.exec(alertSrc);
assert.ok(listBlock, 'workflow_run.workflows list not found');
const listed = listBlock[1].trim().split('\n').map((l) => l.trim().replace(/^- /, '').replace(/^['"]|['"]$/g, ''));

// ── 2. in-memory GitHub ──────────────────────────────────────────────────────
const LISTING = 'MCP Listing Collection (tools, resources, prompts)';
const RUNS: Record<number, { name: string; run_number: number; jobs: any[] }> = {
  1001: { name: LISTING, run_number: 63, jobs: [{ name: 'collect', conclusion: 'failure', steps: [
    { name: 'Collect resources/list', conclusion: 'success' }, { name: 'Fail if any collector failed', conclusion: 'failure' }] }] },
  1002: { name: LISTING, run_number: 64, jobs: [{ name: 'collect', conclusion: 'failure', steps: [{ name: 'Fail if any collector failed', conclusion: 'failure' }] }] },
  1003: { name: LISTING, run_number: 65, jobs: [{ name: 'collect', conclusion: 'failure', steps: [{ name: 'Fail if any collector failed', conclusion: 'failure' }] }] },
  2001: { name: 'Broadcast Daily Post', run_number: 40, jobs: [{ name: 'post', conclusion: 'failure', steps: [{ name: 'Generate and post', conclusion: 'failure' }] }] },
  3001: { name: 'Ingest Watchdog', run_number: 9, jobs: [] },
  3002: { name: 'Broadcast Text Card', run_number: 7, jobs: [{ name: 'post', conclusion: 'timed_out', steps: [{ name: 'Post', conclusion: 'cancelled' }] }] },
};

function fakeGitHub() {
  const issues: { number: number; state: string; title: string; body: string; labels: string[]; assignees: string[] }[] = [];
  const comments: { issue: number; body: string }[] = [];
  const labels = new Set<string>();
  const writes: string[] = [];
  let calls = 0;
  const f = (async (url: string, init: RequestInit = {}) => {
    calls++;
    const method = init.method ?? 'GET';
    const body = init.body ? JSON.parse(String(init.body)) : null;
    const path = url.replace('https://api.github.com/repos/eXaive/aive-ingest', '');
    const json = (status: number, data: unknown) => new Response(status === 204 ? null : JSON.stringify(data), { status });
    let m: RegExpExecArray | null;
    if (url.startsWith('https://api.resend.com/')) { writes.push('EMAIL'); return json(200, { id: 'e' }); }
    if ((m = /^\/actions\/runs\/(\d+)$/.exec(path))) {
      const r = RUNS[Number(m[1])];
      return json(200, { name: r.name, run_number: r.run_number, run_attempt: 1, event: 'schedule', conclusion: 'failure',
        head_sha: 'a2170b956ec7', html_url: `https://github.com/eXaive/aive-ingest/actions/runs/${m[1]}` });
    }
    if ((m = /^\/actions\/runs\/(\d+)\/attempts\/1\/jobs/.exec(path))) return json(200, { jobs: RUNS[Number(m[1])].jobs });
    if (path.startsWith('/issues?') && method === 'GET') {
      assert.match(path, new RegExp(`labels=${ALERT_LABEL}`));
      return json(200, [...issues].reverse().filter((i) => i.labels.includes(ALERT_LABEL)));
    }
    if (path.startsWith('/issues/comments?')) return json(200, [...comments].reverse().map((c) => ({ body: c.body })));
    if ((m = /^\/issues\/(\d+)\/comments$/.exec(path)) && method === 'POST') {
      writes.push(`COMMENT#${m[1]}`); comments.push({ issue: Number(m[1]), body: body.body }); return json(201, {});
    }
    if (path === '/issues' && method === 'POST') {
      const n = issues.length + 1;
      writes.push(`ISSUE#${n}`);
      issues.push({ number: n, state: 'open', title: body.title, body: body.body, labels: body.labels, assignees: body.assignees });
      return json(201, { number: n });
    }
    if (path === `/labels/${ALERT_LABEL}`) return labels.has(ALERT_LABEL) ? json(200, { name: ALERT_LABEL }) : json(404, {});
    if (path === '/labels' && method === 'POST') { writes.push('LABEL'); labels.add(body.name); return json(201, {}); }
    throw new Error(`unexpected request ${method} ${path}`);
  }) as typeof fetch;
  return { f, issues, comments, writes, get calls() { return calls; }, close: (n: number) => { issues[n - 1].state = 'closed'; } };
}

const env = (runId: number, conclusion = 'failure', extra: Record<string, string> = {}) =>
  ({ RUN_ID: String(runId), RUN_CONCLUSION: conclusion, GITHUB_REPOSITORY: 'eXaive/aive-ingest', GITHUB_TOKEN: 'gh-test-token', ...extra });
const RESEND = { RESEND_API_KEY: 'k', ALERT_EMAIL_TO: 'to@example.test' };

async function main(): Promise<void> {
  assert.equal(scheduled.length, 18, `expected 18 scheduled workflows, found ${scheduled.length}`);
  for (const n of scheduled) assert.ok(listed.includes(n), `scheduled workflow not covered by ${ALERT_FILE}: "${n}"`);
  for (const n of listed) assert.ok(scheduled.includes(n), `${ALERT_FILE} lists a workflow that is not scheduled or no longer exists: "${n}"`);
  assert.equal(new Set(listed).size, listed.length, 'duplicate entry in workflows list');
  ok(`all ${scheduled.length} scheduled workflows covered, nothing stale`);

  for (const conclusion of ['success', 'cancelled', 'skipped', 'neutral', '']) {
    const g = fakeGitHub();
    assert.equal(await run(env(1001, conclusion, RESEND), g.f), 'none');
    assert.equal(g.calls, 0, `${conclusion || '(empty)'} made a request`);
  }
  ok('success, cancelled, skipped, neutral: no alert and no request');

  const g = fakeGitHub();
  // First failure: one labelled issue, label created once, assigned to owner.
  assert.equal(await run(env(1001, 'failure', RESEND), g.f), 'issue');
  assert.deepEqual(g.writes, ['LABEL', 'ISSUE#1'], 'Resend secrets set, but email stays off');
  const [first] = g.issues;
  assert.equal(first.title, `[aive-ingest] Failing: ${LISTING}`);
  assert.deepEqual(first.labels, [ALERT_LABEL]);
  assert.deepEqual(first.assignees, ['eXaive']);
  assert.match(first.body, new RegExp(`^Workflow: ${LISTING.replace(/[()]/g, '\\$&')}$`, 'm'));
  assert.match(first.body, /^Run: #63 \(attempt 1, schedule, a2170b95\)$/m);
  assert.match(first.body, /^Failing step: collect › Fail if any collector failed$/m);
  assert.match(first.body, /^Run URL: https:\/\/github\.com\/eXaive\/aive-ingest\/actions\/runs\/1001$/m);
  assert.ok(first.body.includes(workflowMarker(LISTING)) && first.body.includes(runMarker(1001)));
  ok('first failure: one labelled issue, assigned to the owner, with workflow, run number, failing step and URL');

  // A different failed run of the same workflow: a comment, not a second issue.
  assert.equal(await run(env(1002), g.f), 'comment');
  assert.equal(g.issues.length, 1, 'still one issue');
  assert.deepEqual(g.writes.slice(2), ['COMMENT#1']);
  assert.match(g.comments[0].body, /^Run: #64 /m);
  assert.ok(g.comments[0].body.includes(runMarker(1002)));
  ok('two different failed runs of the same workflow: 1 issue + 1 comment');

  // Re-deliveries / re-runs of recorded runs: nothing.
  assert.equal(await run(env(1001), g.f), 'duplicate', 're-run of the run that opened the issue');
  assert.equal(await run(env(1002), g.f), 'duplicate', 're-run of the run recorded as a comment');
  assert.equal(g.writes.length, 3, 'no writes for re-deliveries');
  ok('a re-run of a recorded run writes nothing (issue-body and comment markers)');

  // Another workflow gets its own issue; the label is not recreated.
  assert.equal(await run(env(2001), g.f), 'issue');
  assert.deepEqual(g.writes.slice(3), ['ISSUE#2']);
  assert.equal(g.issues[1].title, '[aive-ingest] Failing: Broadcast Daily Post');
  ok('a different workflow opens its own issue; the label is created only once');

  // Close the listing issue: the next failure opens a new one; old runs stay recorded.
  g.close(1);
  assert.equal(await run(env(1003), g.f), 'issue');
  assert.deepEqual(g.writes.slice(4), ['ISSUE#3']);
  assert.equal(g.issues[2].title, `[aive-ingest] Failing: ${LISTING}`);
  assert.match(g.issues[2].body, /^Run: #65 /m);
  assert.equal(await run(env(1002), g.f), 'duplicate', 'run recorded on the closed issue still counts');
  assert.equal(await run(env(1001), g.f), 'duplicate');
  assert.equal(g.writes.length, 5);
  ok('after closing, the next failure opens a new issue; runs on the closed issue still never re-alert');

  // startup_failure / timed_out still alert, with an explicit fallback step.
  const s = fakeGitHub();
  assert.equal(await run(env(3001, 'startup_failure'), s.f), 'issue');
  assert.match(s.issues[0].body, /Failing step: \(none reported\) › \(no failing step reported\)/);
  assert.equal(await run(env(3002, 'timed_out'), s.f), 'issue');
  assert.match(s.issues[1].body, /Failing step: post › \(no failing step reported\)/);
  ok('startup_failure and timed_out alert, with an explicit "no failing step reported"');

  // Email: disabled in run(); the kept path still builds the right request.
  assert.equal(EMAIL_ENABLED, false);
  const sent: { url: string; headers: any; body: any }[] = [];
  const facts: RunFacts = { id: 1002, name: LISTING, runNumber: 64, attempt: 1, event: 'schedule', conclusion: 'failure',
    url: 'https://github.com/eXaive/aive-ingest/actions/runs/1002', headSha: 'a2170b95', failures: [{ job: 'collect', step: 'Fail if any collector failed' }] };
  await sendEmail((async (url: string, init: RequestInit) => { sent.push({ url, headers: init.headers, body: JSON.parse(String(init.body)) }); return new Response('{}', { status: 200 }); }) as typeof fetch,
    RESEND, facts, composeAlert(facts));
  assert.equal(sent.length, 1);
  assert.equal(sent[0].url, 'https://api.resend.com/emails');
  assert.equal(sent[0].headers['Idempotency-Key'], 'aive-ingest-run-1002');
  assert.deepEqual(sent[0].body.to, ['to@example.test']);
  ok('email disabled (EMAIL_ENABLED=false); the kept Resend path still builds a per-run idempotent request');

  // A GitHub API failure throws, so the alert run itself goes red.
  const bad = (async () => new Response('{}', { status: 500 })) as unknown as typeof fetch;
  await assert.rejects(run(env(1001), bad), /HTTP 500/);
  ok('a GitHub API failure throws, so the alert run goes red rather than silently green');

  console.log(`\nworkflow failure alert: ${checks} checks passed`);
}

main().catch((e) => { console.error(e instanceof Error ? e.stack : e); process.exit(1); });
