// Stalls must be loud. (1) A posting worker that finds someone else's attempt older than 6 h on its intent fails the
// run instead of exiting 0. (2) The hourly reconciler confirms published posts, then fails naming any daily account
// with 0 confirmed posts in 24 h. (3) The reconcile workflow is scheduled hourly and covered by Workflow Failure Alert.
// Offline: the database module and fetch are faked; nothing connects or publishes.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';

const H = 3_600_000, NOW = Date.parse('2026-10-16T21:00:00Z');
const src = (f: string) => readFileSync(path.join(__dirname, '..', f), 'utf8').replace(/\r\n/g, '\n');

// Fake the ingest database before anything imports it.
const calls: { text: string; params?: unknown[] }[] = [];
let candidates: { id: string; provider_submission_id: string }[] = [];
let posted24h: { account_id: string; n: number }[] = [];
const dbPath = require.resolve('../lib/ingest/db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: {
  q: async (text: string, params?: unknown[]) => { calls.push({ text, params });
    if (/from broadcast_publication_attempts where state in/.test(text)) return candidates;
    if (/count\(\*\)::int as n/.test(text)) return posted24h;
    return []; },
  endIngestPool: async () => {},
} } as unknown as NodeModule;

// Required (not imported) so they load after the fake database above.
const { stalledAttemptMessage, STALL_AFTER_MS } = require('../lib/broadcast/publicationAttempts') as typeof import('../lib/broadcast/publicationAttempts');
const { DAILY_POSTING_ACCOUNTS, PAUSED_POSTING_ACCOUNTS, POSTED_LAST_24H_SQL, postedNothingAccounts, postedNothingMessage, pausedAccountsNote } = require('../lib/broadcast/dailyPostingAccounts') as typeof import('../lib/broadcast/dailyPostingAccounts');

async function main() {
  // 1. Stall rule.
  assert.equal(STALL_AFTER_MS, 6 * H);
  const at = (h: number) => new Date(NOW - h * H).toISOString();
  assert.equal(stalledAttemptMessage({ id: 'a', state: 'confirmation_pending', created_at: at(5.9) }, NOW), null, 'fresh pending: may still be in flight');
  for (const state of ['confirmation_pending', 'provider_accepted', 'submitting', 'outcome_unknown', 'reserved', 'provider_failed'] as const) {
    const m = stalledAttemptMessage({ id: 'f578909f', state, created_at: at(6.1) }, NOW);
    assert.ok(m && m.startsWith('STALLED_PUBLICATION:') && m.includes('f578909f') && m.includes(state), state);
  }
  assert.match(stalledAttemptMessage({ id: 'c', state: 'confirmation_pending', created_at: at(170) }, NOW)!, /for 170h \(limit 6h\)/);
  assert.equal(stalledAttemptMessage({ id: 'd', state: 'confirmed_published', created_at: at(500) }, NOW), null, 'confirmed is never a stall');
  assert.match(stalledAttemptMessage({ id: 'e', state: 'confirmation_pending', created_at: null }, NOW)!, /no readable created_at/, 'unknown age fails loud');
  console.log('PASS stall rule: non-confirmed attempt older than 6 h → STALLED_PUBLICATION; younger or confirmed → no stall');

  // 2. Every posting worker throws the stall before its "existing attempt" exit-0 path.
  for (const [file, tag] of [['workers/broadcast-daily-post.ts', 'broadcast-daily'], ['workers/broadcast-text-card.ts', 'broadcast-card'], ['workers/broadcast-quiz-card.ts', 'quiz-card']]) {
    const s = src(file);
    assert.match(s, /import \{[^}]*\bstalledAttemptMessage\b[^}]*\} from '\.\.\/lib\/broadcast\/publicationAttempts'/, file);
    const branch = new RegExp(`if \\(attempt\\.state !== 'reserved' \\|\\| attempt\\.lease_owner !== leaseOwner\\) \\{\\n[^\\n]*\\n\\s*const stalled = stalledAttemptMessage\\(attempt\\);\\n\\s*if \\(stalled\\) throw new Error\\(stalled\\);\\n\\s*console\\.log\\(\`\\[${tag}\\] existing attempt`);
    assert.match(s, branch, file + ': stall check precedes the exit-0 log');
    assert.match(s, /process\.exit\(1\)/, file + ': a thrown error exits non-zero');
  }
  console.log('PASS workers: Daily Post, Text Card and Quiz Card fail the run on a stalled existing attempt');

  // 3. Daily accounts match the worker constants.
  const quiz = src('workers/broadcast-quiz-card.ts'), daily = src('workers/broadcast-daily-post.ts'), text = src('workers/broadcast-text-card.ts');
  const byHandle = Object.fromEntries(DAILY_POSTING_ACCOUNTS.map(a => [a.handle, a.accountId]));
  assert.ok(daily.includes(`const MISTER_MCP_A2A = '${byHandle['mr.agent.infrastructure']}'`) && text.includes(`const MISTER_MCP_A2A = '${byHandle['mr.agent.infrastructure']}'`));
  assert.ok(quiz.includes(`accountId: '${byHandle['vice.marshal.kyli']}', // vice.marshal.kyli`));
  // Both active TikTok accounts stay monitored; Cat is paused, not forgotten: its id still matches the worker constant.
  assert.deepEqual(DAILY_POSTING_ACCOUNTS.map(a => a.handle), ['mr.agent.infrastructure', 'vice.marshal.kyli']);
  const cat = PAUSED_POSTING_ACCOUNTS.find(a => a.handle === 'wwwsnowbunnymafiacom')!;
  assert.ok(cat && cat.status === 'PAUSED / DISCONNECTED', 'Cat is recorded as PAUSED / DISCONNECTED');
  assert.ok(quiz.includes(`accountId: '${cat.accountId}', // wwwsnowbunnymafiacom`));
  assert.ok(!PAUSED_POSTING_ACCOUNTS.some(p => DAILY_POSTING_ACCOUNTS.some(a => a.accountId === p.accountId)), 'an account is either monitored or paused, never both');
  assert.match(POSTED_LAST_24H_SQL, /state = 'confirmed_published' and confirmed_at >= now\(\) - interval '24 hours'/);
  // A real stall on an active account still alarms, by name.
  const none = postedNothingAccounts([{ account_id: byHandle['mr.agent.infrastructure'], n: 2 }]);
  assert.deepEqual(none.map(a => a.handle), ['vice.marshal.kyli']);
  assert.equal(postedNothingMessage(none), 'POSTED_NOTHING_24H: vice.marshal.kyli (fed by Broadcast Quiz Card (GTA)) had 0 confirmed posts in the last 24 hours');
  const both = postedNothingAccounts([]);
  assert.equal(postedNothingMessage(both), 'POSTED_NOTHING_24H: mr.agent.infrastructure (fed by Broadcast Daily Post + Broadcast Text Card); vice.marshal.kyli (fed by Broadcast Quiz Card (GTA)) had 0 confirmed posts in the last 24 hours');
  // The paused account posting nothing does not alarm while the active accounts post.
  assert.equal(postedNothingMessage(postedNothingAccounts([...DAILY_POSTING_ACCOUNTS.map(a => ({ account_id: a.accountId, n: 1 })), { account_id: cat.accountId, n: 0 }])), null);
  assert.equal(postedNothingMessage(postedNothingAccounts(DAILY_POSTING_ACCOUNTS.map(a => ({ account_id: a.accountId, n: 1 })))), null);
  assert.equal(pausedAccountsNote(), 'PAUSED_NOT_MONITORED: wwwsnowbunnymafiacom (Broadcast Quiz Card (Cat)) PAUSED / DISCONNECTED since 2026-11-08');
  console.log('PASS daily accounts: ids match the worker constants; an active account with 0 confirmed posts in 24 h is named; the paused Cat account is reported, not alarmed');

  // 4. Reconciler behaviour: confirms a published post, then reports the silent account; GET reads only.
  process.env.BLOTATO_API_KEY = 'offline-test-key';
  const fetched: string[] = [];
  globalThis.fetch = (async (url: string, init?: RequestInit) => { fetched.push(`${init?.method ?? 'GET'} ${url}`);
    return new Response(JSON.stringify({ status: 'published', publicUrl: 'https://www.tiktok.com/@wwwsnowbunnymafiacom' }), { status: 200 }); }) as typeof fetch;
  const { main: reconcile } = require('../workers/reconcile-broadcast-publications');
  candidates = [{ id: 'f578909f', provider_submission_id: '76777274' }];
  posted24h = [{ account_id: byHandle['mr.agent.infrastructure'], n: 3 }];
  const logged: string[] = [], log = console.log; console.log = (...a: unknown[]) => { logged.push(a.join(' ')); };
  const failure = await reconcile();
  console.log = log;
  assert.ok(logged.includes(pausedAccountsNote()!), 'each run names the paused account');
  assert.ok(calls.some(c => /confirm_broadcast_publication/.test(c.text) && (c.params as unknown[])[0] === 'f578909f'), 'published post confirmed');
  assert.deepEqual(fetched, ['GET https://backend.blotato.com/v2/posts/76777274'], 'status read only, no provider create call');
  assert.deepEqual((calls.find(c => /count\(\*\)::int as n/.test(c.text))!.params as unknown[][])[0], DAILY_POSTING_ACCOUNTS.map(a => a.accountId));
  assert.match(failure, /^POSTED_NOTHING_24H: vice\.marshal\.kyli \(fed by Broadcast Quiz Card \(GTA\)\) had 0/);
  posted24h = DAILY_POSTING_ACCOUNTS.map(a => ({ account_id: a.accountId, n: 1 })); candidates = [];
  assert.equal(await reconcile(), null, 'every account posted: run succeeds');
  assert.match(src('workers/reconcile-broadcast-publications.ts'), /main\(\)\.then\(async failure=>\{await endIngestPool\(\);if\(failure\)\{console\.error\(failure\);process\.exit\(1\)\}\}\)/);
  console.log('PASS reconciler: confirms published posts (GET only), names paused accounts, then exits non-zero naming any active account silent for 24 h');

  // 5. Hourly schedule and failure-alert coverage.
  const wf = src('.github/workflows/broadcast-reconcile.yml');
  assert.match(wf, /^name: Broadcast Publication Reconcile$/m);
  assert.match(wf, /- cron: '41 \* \* \* \*'/);
  assert.match(wf, /run: npm run broadcast:reconcile/);
  assert.doesNotMatch(wf.split('\n').filter(l => !l.trimStart().startsWith('#')).join('\n'), /pull_request/, 'no pull_request trigger');
  assert.match(src('.github/workflows/workflow-failure-alert.yml'), /^\s+- 'Broadcast Publication Reconcile'$/m);
  assert.match(src('package.json'), /"broadcast:reconcile": "tsx workers\/reconcile-broadcast-publications\.ts"/);
  console.log('PASS workflow: hourly at :41, runs npm run broadcast:reconcile, listed in Workflow Failure Alert');
}
main().catch(e => { console.error(e); process.exit(1); });
