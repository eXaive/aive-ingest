/**
 * NEWS publication gate on the existing Daily Post path (lib/broadcast/newsApproval.ts, promoRotation.ts).
 *
 * Drives the REAL workers/broadcast-daily-post.ts main() and broadcast-text-card.ts main() against an in-memory
 * broadcast_topic_queue (pg.Pool.prototype.query intercepted) and a stubbed fetch (media host, app routes,
 * Blotato). NO DATABASE, NO NETWORK.
 *
 * Asserts: drafts and unapproved renders never publish; approval binds the exact media bytes, caption, URL and
 * account; NEWS rows never enter any automatic rotation (regular, OU1COMES promo, fallback, loop-back, cards);
 * the daily cap still stands a run down first; a news episode has one intent, so a retry or second run never
 * creates another submission; news never generates from a prompt; the status line keeps every step separate.
 *
 * Run: npx tsx scripts/verify-news-publication.ts
 */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import pg from 'pg';

const approvalDir = mkdtempSync(path.join(tmpdir(), 'news-approvals-'));
Object.assign(process.env, {
  AIVE_INGEST_DATABASE_URL: process.env.AIVE_INGEST_DATABASE_URL ?? 'postgres://verify:verify@127.0.0.1:5432/verify',
  BLOTATO_API_KEY: 'verify-key', AIVE_CRON_SECRET: 'verify-secret', AIVE_BASE_URL: 'https://app.verify', NEWS_APPROVAL_DIR: approvalDir,
});
delete process.env.BROADCAST_SKIP_TODAY;

import { NEWS_TARGET, newsStatus, newsTopicName, parseNewsApproval, assertNewsPublishable, planApprovalRequest, type NewsApproval } from '../lib/broadcast/newsApproval';
import { topicPoolPredicate, PROMO_TOPIC_PREFIX, NEWS_TOPIC_PREFIX } from '../lib/broadcast/promoRotation';
import { main as dailyMain, nextTopic as nextVideoTopic } from '../workers/broadcast-daily-post';
import { main as cardMain, nextTopic as nextCardTopic } from '../workers/broadcast-text-card';

// ---------------------------------------------------------------- fixtures
const STORY = 'aive-mcp-news-0cd507c2b79848c947bbaac77a3ee35726d25a3ed8b498a730dd4b9058adcacc';
const TOPIC = newsTopicName(STORY);
const MEDIA = Buffer.from('approved-render-bytes'.repeat(1000));
const MEDIA_SHA = createHash('sha256').update(MEDIA).digest('hex');
const MEDIA_URL = 'https://media.verify/approved.mp4';
const CAPTION = 'Ticker Scout went from 97 MCP tools to 20. #MCP';
const NOW = Date.now();
const hoursFrom = (h: number) => new Date(NOW + h * 3_600_000).toISOString();

const approval = (over: Partial<NewsApproval> = {}): NewsApproval => ({
  schema: 'AIVE_NEWS_PUBLICATION_APPROVAL_V1', story_id: STORY, topic_name: TOPIC,
  render: { sha256: MEDIA_SHA, bytes: MEDIA.length, duration_s: 33.3, width: 1080, height: 1920, fps: 30, label: 'v2_1' },
  caption: CAPTION, target: { ...NEWS_TARGET }, media_url: MEDIA_URL, state: 'PUBLICATION_AUTHORIZED',
  reviewed_at: hoursFrom(-3), approved_at: hoursFrom(-2), authorized_at: hoursFrom(-1), authorization_expires_at: hoursFrom(23), rejected_reason: null, ...over,
});
const writeApproval = (a: NewsApproval | null) => { const f = path.join(approvalDir, `${STORY}.json`); if (a) writeFileSync(f, JSON.stringify(a)); else rmSync(f, { force: true }); };

interface Row { id: string; topic_name: string; queue_position: number; status: string; used_as_card: boolean; caption: string | null; pregenerated_media_url: string | null; prompt_seed: string }
let table: Row[] = [];
const row = (topic_name: string, queue_position: number, over: Partial<Row> = {}): Row =>
  ({ id: topic_name, topic_name, queue_position, status: 'pending', used_as_card: false, caption: `caption ${topic_name}`, pregenerated_media_url: null, prompt_seed: 'seed', ...over });
const newsRow = (over: Partial<Row> = {}) => row(TOPIC, 900, { id: '11111111-1111-4111-8111-111111111111', caption: CAPTION, pregenerated_media_url: MEDIA_URL, prompt_seed: 'PRE-RENDERED NEWS', ...over });

let db = { jobsToday: 0, videosToday: 0, cardsToday: 0, claim: null as null | ((v: unknown[]) => any), prior: null as null | { state: string; failed_at: string | null } };
const calls: { claims: unknown[][]; fetches: { url: string; body: any }[]; writes: string[] } = { claims: [], fetches: [], writes: [] };
const reset = () => { db = { jobsToday: 0, videosToday: 0, cardsToday: 0, claim: null, prior: null }; calls.claims = []; calls.fetches = []; calls.writes = []; };

const REGULAR_SQL = topicPoolPredicate(false), PROMO_SQL = topicPoolPredicate(true);
const inPool = (s: string, r: Row) => {
  const promo = r.topic_name.startsWith(PROMO_TOPIC_PREFIX), news = r.topic_name.startsWith(NEWS_TOPIC_PREFIX);
  if (s.includes(REGULAR_SQL)) return !promo && !news;
  if (s.includes(PROMO_SQL)) return promo;
  return true;
};
const first = (rows: Row[]) => [...rows].sort((a, b) => a.queue_position - b.queue_position).slice(0, 1);

// eslint-disable-next-line @typescript-eslint/no-explicit-any
(pg.Pool.prototype as any).query = async function fake(sql: string, values: unknown[] = []) {
  const s = sql.replace(/\s+/g, ' ');
  let rows: unknown[] = [];
  if (/count\(\*\)::text AS n FROM \(/.test(s)) rows = [{ n: String(db.jobsToday) }];
  else if (/AS videos/.test(s)) rows = [{ videos: String(db.videosToday), cards: String(db.cardsToday), total: String(db.jobsToday) }];
  else if (/AS cards/.test(s)) rows = [{ cards: String(db.cardsToday), total: String(db.jobsToday) }];
  else if (/WHERE topic_name = \$1/.test(s)) rows = table.filter(r => r.topic_name === values[0] && (!/status <> 'skipped'/.test(s) || r.status !== 'skipped')).slice(0, 1);
  else if (/^SELECT .* WHERE status = 'pending'/.test(s)) rows = first(table.filter(r => r.status === 'pending' && inPool(s, r)));
  else if (/^UPDATE .* SET status = 'pending'/.test(s)) { calls.writes.push(s); rows = table.filter(r => r.status === 'used' && inPool(s, r)).map(r => { r.status = 'pending'; return { id: r.id }; }); }
  else if (/^SELECT .* WHERE used_as_card = false/.test(s)) rows = first(table.filter(r => !r.used_as_card && r.status !== 'skipped' && inPool(s, r)));
  else if (/^UPDATE .* SET used_as_card = false/.test(s)) { calls.writes.push(s); rows = table.filter(r => r.used_as_card && r.status !== 'skipped' && inPool(s, r)).map(r => { r.used_as_card = false; return { id: r.id }; }); }
  else if (/FROM broadcast_publication_attempts WHERE intent_key = \$1/.test(s)) rows = db.prior ? [db.prior] : [];
  else if (/claim_broadcast_publication/.test(s)) { calls.claims.push(values); rows = [db.claim ? db.claim(values) : { id: 'attempt-1', state: 'reserved', lease_owner: values[6], created_at: new Date().toISOString() }]; }
  else if (/confirmation_pending|provider_failed/.test(s)) { calls.writes.push(s); }
  else if (/confirm_broadcast_publication/.test(s)) { calls.writes.push(s); }
  else if (/^UPDATE broadcast_topic_queue SET status = 'used'/.test(s)) { calls.writes.push(s); const r = table.find(x => x.id === values[0]); if (r) { r.status = 'used'; r.pregenerated_media_url = null; } }
  else throw new Error(`unexpected statement: ${s.slice(0, 140)}`);
  return { rows, rowCount: rows.length };
};

let mediaBytes = MEDIA;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
(globalThis as any).fetch = async (url: string, init: any = {}) => {
  const body = init.body ? JSON.parse(init.body) : null;
  calls.fetches.push({ url, body });
  if (url === MEDIA_URL) return new Response(new Uint8Array(mediaBytes), { status: 200 });
  if (url.endsWith('/api/broadcast/jobs')) return Response.json({ job: { id: 'job-1' } });
  if (url.endsWith('/api/broadcast/dispatch')) return Response.json({ job: { status: 'posted', submission_ids: { [NEWS_TARGET.account_id]: { status: 'posted', submission_id: 'sub-1' } } } });
  if (url.includes('/v2/posts/')) return Response.json({ status: 'published', publicUrl: 'https://www.tiktok.com/@mr.agent.infrastructure/video/1' });
  if (url.includes('/v2/videos/from-templates')) throw new Error('GENERATION CALLED');
  throw new Error(`unexpected fetch ${url}`);
};

const runDaily = async (force: string | null) => { if (force) process.env.BROADCAST_FORCE_TOPIC = force; else delete process.env.BROADCAST_FORCE_TOPIC; return dailyMain(); };
const rejectsCode = async (force: string | null, code: string) => {
  await assert.rejects(runDaily(force), (e: Error) => e.message.includes(code), code);
  assert.equal(calls.claims.length, 0, `${code}: no capacity claim`);
  assert.ok(!calls.fetches.some(f => f.url.startsWith('https://app.verify')), `${code}: no app call`);
};
const generationCalled = () => calls.fetches.some(f => f.url.includes('from-templates'));
const quiet = async <T>(fn: () => Promise<T>) => { const log = console.log; console.log = () => {}; try { return await fn(); } finally { console.log = log; } };

async function run() {
  // 1. Draft (no approval at all) cannot publish.
  reset(); table = [newsRow()]; writeApproval(null);
  await quiet(() => rejectsCode(TOPIC, 'NEWS_NOT_AUTHORIZED'));

  // 2. Rendered but not authorized: every earlier state is refused.
  for (const state of ['REVIEW_REQUESTED', 'OWNER_REVIEWED', 'OWNER_APPROVED'] as const) {
    reset(); table = [newsRow()];
    writeApproval(approval({ state, authorized_at: null, authorization_expires_at: null, ...(state === 'REVIEW_REQUESTED' ? { reviewed_at: null, approved_at: null } : state === 'OWNER_REVIEWED' ? { approved_at: null } : {}) }));
    await quiet(() => rejectsCode(TOPIC, 'NEWS_NOT_AUTHORIZED'));
  }
  reset(); table = [newsRow()]; writeApproval(approval({ authorized_at: hoursFrom(-30), authorization_expires_at: hoursFrom(-1) }));
  await quiet(() => rejectsCode(TOPIC, 'NEWS_APPROVAL_EXPIRED'));

  // 3. Approval binds the exact caption, URL and bytes.
  reset(); table = [newsRow({ caption: CAPTION + ' edited' })]; writeApproval(approval());
  await quiet(() => rejectsCode(TOPIC, 'NEWS_CAPTION_MISMATCH'));
  reset(); table = [newsRow({ pregenerated_media_url: 'https://media.verify/other.mp4' })]; writeApproval(approval());
  await quiet(() => rejectsCode(TOPIC, 'NEWS_MEDIA_URL_MISMATCH'));
  reset(); table = [newsRow()]; writeApproval(approval()); mediaBytes = Buffer.concat([MEDIA.subarray(0, MEDIA.length - 1), Buffer.from('X')]);
  await quiet(() => rejectsCode(TOPIC, 'NEWS_MEDIA_MISMATCH')); mediaBytes = MEDIA;
  reset(); table = [newsRow({ pregenerated_media_url: null })]; writeApproval(approval());
  await quiet(() => rejectsCode(TOPIC, 'NEWS_NO_MEDIA'));
  assert.ok(!generationCalled(), 'news never generates from a prompt');

  // 8. Correct account only.
  reset(); table = [newsRow()]; writeApproval(approval({ target: { platform: 'tiktok', account_id: 'bd7d3219-0d23-4523-9f07-7b8bd4e55e74', handle: 'vice.marshal.kyli' } }));
  await quiet(() => rejectsCode(TOPIC, 'NEWS_ACCOUNT_MISMATCH'));

  // 7. The daily cap and the one-video-a-day rule stand the run down before anything is read or fetched.
  for (const d of [{ jobsToday: 3 }, { videosToday: 1 }]) {
    reset(); Object.assign(db, d); table = [newsRow()]; writeApproval(approval());
    await quiet(() => runDaily(TOPIC));
    assert.equal(calls.claims.length, 0); assert.equal(calls.fetches.length, 0, 'stand-down fetches nothing, not even the media');
  }

  // Happy path: one authorized news post through the existing staging + dispatch + confirmation.
  reset(); table = [newsRow()]; writeApproval(approval());
  await quiet(() => runDaily(TOPIC));
  assert.equal(calls.claims.length, 1);
  const [intent, kind, sourceId, purpose, slot, account, , lease, cap] = calls.claims[0] as [string, string, string, string, string, string, string, number, number];
  assert.equal(intent, `topic-video:${newsRow().id}:news-once`); assert.equal(kind, 'broadcast_topic'); assert.equal(sourceId, newsRow().id);
  assert.equal(purpose, 'daily-video'); assert.equal(slot, 'news-once'); assert.equal(account, NEWS_TARGET.account_id); assert.equal(lease, 900); assert.equal(cap, 3);
  const staged = calls.fetches.find(f => f.url.endsWith('/api/broadcast/jobs'))!.body;
  assert.equal(staged.source_video_url, MEDIA_URL); assert.equal(staged.caption, CAPTION);
  assert.deepEqual(staged.platform_targets, ['tiktok']); assert.deepEqual(staged.accounts, [{ account_id: NEWS_TARGET.account_id }]);
  assert.equal(calls.fetches.find(f => f.url.endsWith('/api/broadcast/dispatch'))!.body.targetOptions.isAiGenerated, true);
  assert.ok(calls.fetches.findIndex(f => f.url === MEDIA_URL) < calls.fetches.findIndex(f => f.url.endsWith('/api/broadcast/jobs')), 'media verified before staging');
  assert.ok(!generationCalled());
  assert.equal(table[0].status, 'used'); assert.equal(table[0].pregenerated_media_url, null);

  // 9. Duplicates: a second force run is refused (used); a crash-recovered intent never re-submits; an in-flight one waits.
  reset(); writeApproval(approval());
  await quiet(() => rejectsCode(TOPIC, 'NEWS_ALREADY_USED'));
  reset(); table = [newsRow()]; writeApproval(approval()); db.claim = () => ({ id: 'attempt-1', state: 'confirmed_published', lease_owner: 'earlier', created_at: new Date().toISOString() });
  await quiet(() => runDaily(TOPIC));
  assert.ok(!calls.fetches.some(f => f.url.startsWith('https://app.verify')), 'recovered intent: no staging or dispatch');
  assert.equal(table[0].status, 'used');
  reset(); table = [newsRow()]; writeApproval(approval()); db.claim = () => ({ id: 'attempt-1', state: 'confirmation_pending', lease_owner: 'other', created_at: new Date().toISOString() });
  await quiet(() => runDaily(TOPIC));
  assert.ok(!calls.fetches.some(f => f.url.startsWith('https://app.verify')), 'in-flight intent: no second submission');

  // Failed attempts: a provider_failed news intent never retries on the authorization that preceded the failure.
  reset(); table = [newsRow()]; writeApproval(approval()); db.prior = { state: 'provider_failed', failed_at: hoursFrom(-0.5) };
  await quiet(() => rejectsCode(TOPIC, 'NEWS_REAUTHORIZE_AFTER_FAILURE'));
  assert.ok(!generationCalled(), 'a failed news attempt never regenerates');
  reset(); table = [newsRow()]; writeApproval(approval({ authorized_at: hoursFrom(-0.2), authorization_expires_at: hoursFrom(20) })); db.prior = { state: 'provider_failed', failed_at: hoursFrom(-0.5) };
  await quiet(() => runDaily(TOPIC));
  assert.equal(calls.claims.length, 1, 'an explicit re-authorization after the failure may retry the SAME approved render once');
  assert.equal(calls.fetches.find(f => f.url.endsWith('/api/broadcast/jobs'))!.body.source_video_url, MEDIA_URL);

  // A new render supersedes any prior render-specific approval, even an authorized one; nothing carries over.
  const v21 = approval();
  const v22: NewsApproval = { ...approval(), render: { ...approval().render, sha256: 'c'.repeat(64), bytes: 7_000_000, label: 'v2_2' }, media_url: null, state: 'REVIEW_REQUESTED', reviewed_at: null, approved_at: null, authorized_at: null, authorization_expires_at: null };
  const plan = planApprovalRequest(v21, v22);
  assert.equal(plan.archive?.render.sha256, MEDIA_SHA); assert.equal(plan.write.state, 'REVIEW_REQUESTED'); assert.equal(plan.write.media_url, null);
  assert.throws(() => planApprovalRequest(v21, { ...v22, render: v21.render }), /already PUBLICATION_AUTHORIZED/);
  reset(); table = [newsRow()]; writeApproval(plan.write);
  await quiet(() => rejectsCode(TOPIC, 'NEWS_NOT_AUTHORIZED'));

  // 4–6. NEWS never enters automatic rotation; regular and OU1COMES behaviour unchanged.
  const seed = () => { table = [row('MCP', 1), row('A2A', 2), row('OU1COMES: A', 101), row('OU1COMES: B', 102), newsRow({ queue_position: 0 }), row(newsTopicName('aive-mcp-news-' + 'a'.repeat(64)), 0, { status: 'used' })]; };
  const days = ['2026-10-19T14:00:00Z', '2026-10-20T14:00:00Z', '2026-10-21T14:00:00Z', '2026-10-22T14:00:00Z', '2026-10-23T14:00:00Z', '2026-10-24T14:00:00Z', '2026-10-25T14:00:00Z'];
  await quiet(async () => {
    seed();
    const picked: string[] = [];
    for (let i = 0; i < 14; i++) {
      const d = new Date(days[i % 7]); const t = await nextVideoTopic(null, d);
      assert.ok(t && !t.topic_name.startsWith(NEWS_TOPIC_PREFIX), 'video rotation never picks NEWS');
      picked.push(t!.topic_name); t!.topic_name && (table.find(r => r.topic_name === t!.topic_name)!.status = 'used');
    }
    assert.deepEqual(picked.slice(0, 7), ['OU1COMES: A', 'MCP', 'OU1COMES: B', 'A2A', 'OU1COMES: A', 'MCP', 'A2A'], 'Mon/Wed/Fri promo, other days regular, each pool loops on its own');
    assert.ok(table.filter(r => r.topic_name.startsWith(NEWS_TOPIC_PREFIX)).every(r => r.id === newsRow().id ? r.status === 'pending' : r.status === 'used'), 'loop-backs never touch NEWS rows');
    seed(); table = table.filter(r => !r.topic_name.startsWith(PROMO_TOPIC_PREFIX));
    const fallback = await nextVideoTopic(null, new Date(days[0]));
    assert.equal(fallback?.topic_name, 'MCP', 'promo-day fallback goes to the regular pool, never NEWS');
    seed();
    for (let i = 0; i < 6; i++) { const c = await nextCardTopic(null); assert.ok(c && !c.topic_name.startsWith(NEWS_TOPIC_PREFIX) && !c.topic_name.startsWith(PROMO_TOPIC_PREFIX)); table.find(r => r.topic_name === c!.topic_name)!.used_as_card = true; }
  });
  assert.throws(() => assertNewsPublishable({ approval: approval(), row: newsRow(), accountId: NEWS_TARGET.account_id, forced: false, now: new Date() }), /NEWS_NOT_FORCED/);

  // Cards: a forced NEWS topic is refused before any render, claim or post.
  reset(); table = [newsRow()]; process.env.BROADCAST_FORCE_TOPIC = TOPIC;
  await quiet(() => assert.rejects(cardMain(), /never text cards/));
  assert.equal(calls.claims.length, 0); assert.equal(calls.fetches.length, 0);
  delete process.env.BROADCAST_FORCE_TOPIC;

  // Approval file contract: no skipped steps, short authorization window, exact fields.
  assert.throws(() => parseNewsApproval({ ...approval(), approved_at: null }), /requires approved_at/);
  assert.throws(() => parseNewsApproval({ ...approval(), authorization_expires_at: hoursFrom(200) }), /authorization window/);
  assert.throws(() => parseNewsApproval({ ...approval(), extra: 1 }), /fields must be exactly/);
  assert.throws(() => parseNewsApproval({ ...approval(), topic_name: 'NEWS:other' }), /topic_name/);
  assert.throws(() => parseNewsApproval({ ...approval(), render: { ...approval().render, width: 720 } }), /render/);

  // Status line: each step owned by its own record.
  const render = { sha256: MEDIA_SHA, failures: [] as unknown[] };
  assert.equal(newsStatus({ render: null, approval: null, attemptState: null }), 'STORY_GENERATED');
  assert.equal(newsStatus({ render, approval: null, attemptState: null }), 'RENDER_COMPLETED');
  assert.equal(newsStatus({ render, approval: approval({ state: 'OWNER_REVIEWED', approved_at: null, authorized_at: null, authorization_expires_at: null }), attemptState: null }), 'OWNER_REVIEWED');
  assert.equal(newsStatus({ render, approval: approval({ state: 'OWNER_APPROVED', authorized_at: null, authorization_expires_at: null }), attemptState: null }), 'OWNER_APPROVED');
  assert.equal(newsStatus({ render, approval: approval(), attemptState: null }), 'PUBLICATION_AUTHORIZED');
  assert.equal(newsStatus({ render: { ...render, sha256: 'b'.repeat(64) }, approval: approval(), attemptState: null }), 'RENDER_COMPLETED', 'approval for another render does not carry over');
  assert.equal(newsStatus({ render, approval: approval(), attemptState: 'confirmation_pending' }), 'PUBLICATION_SUBMITTED');
  assert.equal(newsStatus({ render, approval: approval(), attemptState: 'confirmed_published' }), 'PUBLICATION_CONFIRMED');
  assert.equal(newsStatus({ render, approval: approval(), attemptState: 'provider_failed' }), 'PUBLICATION_FAILED');

  rmSync(approvalDir, { recursive: true, force: true });
  console.log('verify-news-publication: PASS');
}

run().then(() => process.exit(0)).catch(e => { console.error(e); rmSync(approvalDir, { recursive: true, force: true }); process.exit(1); });
