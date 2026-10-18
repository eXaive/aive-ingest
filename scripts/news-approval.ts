/**
 * Owner approval file for ONE news video (lib/broadcast/newsApproval.ts). Local; touches no database, no network.
 *
 *   request   --story <story.json> --render <brief.mp4> --report <report.json>    → REVIEW_REQUESTED (no authority)
 *   review    --story-id <id> --sha256 <hex>                                       → OWNER_REVIEWED
 *   approve   --story-id <id> --sha256 <hex>                                       → OWNER_APPROVED
 *   authorize --story-id <id> --sha256 <hex> --media-url <https> [--hours 24]      → PUBLICATION_AUTHORIZED
 *   reject    --story-id <id> --reason <text>                                      → REJECTED
 *   status    --story-id <id> [--report <report.json>]
 *   topic-sql --story-id <id> [--position 1001]   → scripts/data/news-<id>.sql.proposal (review material, never run here)
 *
 * review/approve/authorize are owner acts: each requires the owner to retype the approved render's exact SHA-256, and
 * each requires the previous state. A rendered video never approves itself; approving one render never covers another.
 * The file lands in data/news-approvals/<story_id>.json and takes effect only once committed and pushed.
 */
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { MAX_AUTHORIZATION_HOURS, NEWS_APPROVAL_SCHEMA, NEWS_STORY_ID, NEWS_TARGET, newsApprovalDir, newsStatus, newsTopicName, parseNewsApproval, planApprovalRequest, supersededFile, type NewsApproval, type NewsApprovalState } from '../lib/broadcast/newsApproval';

const [cmd] = process.argv.slice(2);
const arg = (n: string) => { const i = process.argv.indexOf(`--${n}`); return i > 0 ? process.argv[i + 1] : undefined; };
const need = (n: string) => { const v = arg(n); if (!v) throw new Error(`--${n} is required`); return v; };
const sha = (f: string) => createHash('sha256').update(readFileSync(f)).digest('hex');
const fileFor = (id: string) => { if (!NEWS_STORY_ID.test(id)) throw new Error('invalid story id'); return path.join(newsApprovalDir(), `${id}.json`); };
const load = (id: string) => parseNewsApproval(JSON.parse(readFileSync(fileFor(id), 'utf8')));
const save = (a: NewsApproval) => { mkdirSync(newsApprovalDir(), { recursive: true }); writeFileSync(fileFor(a.story_id), JSON.stringify(parseNewsApproval(a), null, 2) + '\n'); console.log(`[news-approval] ${a.story_id} → ${a.state}`); };
const sqlText = (s: string) => `'${s.replace(/'/g, "''")}'`;

function transition(from: NewsApprovalState, to: NewsApprovalState, patch: (a: NewsApproval, now: string) => void) {
  const a = load(need('story-id'));
  if (a.state !== from) throw new Error(`state is ${a.state}; ${to} requires ${from}`);
  if (need('sha256') !== a.render.sha256) throw new Error('the retyped SHA-256 is not the render under review');
  const now = new Date().toISOString();
  a.state = to; patch(a, now); save(a);
}

function main() {
  if (cmd === 'request') {
    const story = JSON.parse(readFileSync(need('story'), 'utf8'));
    const report = JSON.parse(readFileSync(need('report'), 'utf8'));
    const mp4 = need('render');
    if (story.schema !== 'AIVE_MCP_NEWS_STORY_V1' || !NEWS_STORY_ID.test(story.story_id)) throw new Error('not an AIVE_MCP_NEWS_STORY_V1');
    if (story.destination?.intended !== `tiktok:@${NEWS_TARGET.handle}`) throw new Error('story is not intended for this account');
    if (report.story_id !== story.story_id || (report.failures ?? ['missing']).length) throw new Error('render report is for another story or did not pass validation');
    const digest = sha(mp4), bytes = readFileSync(mp4).length;
    if (digest !== report.sha256 || bytes !== report.bytes) throw new Error('MP4 does not match its render report');
    const [w, h] = String(report.video).split(' ')[0].split('x').map(Number);
    // A DIFFERENT render archives the active approval (any state) to superseded/: authorization never carries over.
    const next: NewsApproval = { schema: NEWS_APPROVAL_SCHEMA, story_id: story.story_id, topic_name: newsTopicName(story.story_id),
      render: { sha256: digest, bytes, duration_s: report.duration_s, width: w, height: h, fps: 30, label: arg('label') ?? path.basename(path.dirname(mp4)) },
      caption: arg('caption') ?? story.editorial.post_caption, target: { ...NEWS_TARGET }, media_url: null, state: 'REVIEW_REQUESTED',
      reviewed_at: null, approved_at: null, authorized_at: null, authorization_expires_at: null, rejected_reason: null };
    const plan = planApprovalRequest(existsSync(fileFor(story.story_id)) ? load(story.story_id) : null, next);
    if (plan.archive) {
      const dest = supersededFile(newsApprovalDir(), plan.archive);
      mkdirSync(path.dirname(dest), { recursive: true });
      writeFileSync(dest, JSON.stringify(plan.archive, null, 2) + '\n');
      console.log(`[news-approval] superseded ${plan.archive.state} approval for render ${plan.archive.render.sha256.slice(0, 12)} → ${dest}`);
    }
    save(plan.write);
  } else if (cmd === 'review') transition('REVIEW_REQUESTED', 'OWNER_REVIEWED', (a, now) => { a.reviewed_at = now; });
  else if (cmd === 'approve') transition('OWNER_REVIEWED', 'OWNER_APPROVED', (a, now) => { a.approved_at = now; });
  else if (cmd === 'authorize') {
    const hours = Number(arg('hours') ?? 24);
    if (!(hours > 0 && hours <= MAX_AUTHORIZATION_HOURS)) throw new Error(`--hours must be 0-${MAX_AUTHORIZATION_HOURS}`);
    transition('OWNER_APPROVED', 'PUBLICATION_AUTHORIZED', (a, now) => {
      a.media_url = need('media-url'); a.authorized_at = now; a.authorization_expires_at = new Date(Date.parse(now) + hours * 3_600_000).toISOString();
    });
  } else if (cmd === 'reject') {
    const a = load(need('story-id')); a.state = 'REJECTED'; a.rejected_reason = need('reason'); save(a);
  } else if (cmd === 'status') {
    const id = need('story-id'), a = existsSync(fileFor(id)) ? load(id) : null;
    const report = arg('report') ? JSON.parse(readFileSync(arg('report')!, 'utf8')) : null;
    console.log(JSON.stringify({ story_id: id, status: newsStatus({ render: report, approval: a, attemptState: null }), approval_state: a?.state ?? null,
      note: 'Submission, confirmation and failure live in broadcast_publication_attempts (intent topic-video:<row id>:news-once); not read here.' }, null, 2));
  } else if (cmd === 'topic-sql') {
    const a = load(need('story-id')), pos = Number(arg('position') ?? 1001);
    const out = path.join(__dirname, 'data', `news-${a.story_id}.sql.proposal`);
    writeFileSync(out, [
      `-- NEWS topic row for ${a.story_id} — PROPOSAL. Review material only; never run by an agent.`,
      `-- Apply only after the approval file is PUBLICATION_AUTHORIZED, with media_url set to that file's exact media_url.`,
      `-- NEWS rows are in no automatic pool (lib/broadcast/promoRotation.ts); this row posts only via Daily Post force_topic=${a.topic_name}.`,
      `-- On confirmed publication, confirm_broadcast_publication marks it used and clears pregenerated_media_url, so it can never repost.`,
      `INSERT INTO broadcast_topic_queue (topic_name, layer, evidence_status, prompt_seed, status, queue_position, caption, pregenerated_media_url)`,
      `VALUES (${sqlText(a.topic_name)}, 'tools-websites', 'VERIFIED - PRE-RENDERED NEWS', 'PRE-RENDERED NEWS. Never generated from this text.', 'pending', ${pos},`,
      `        ${sqlText(a.caption)}, ${a.media_url ? sqlText(a.media_url) : ':approved_media_url'})`,
      `ON CONFLICT (topic_name) DO NOTHING;`, ''].join('\n'));
    console.log(`[news-approval] wrote ${out}`);
  } else throw new Error('usage: request | review | approve | authorize | reject | status | topic-sql');
}

try { main(); } catch (e) { console.error(`[news-approval] FAILED: ${e instanceof Error ? e.message : String(e)}`); process.exitCode = 1; }
