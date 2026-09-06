/**
 * workers/broadcast-daily-post.ts
 *
 * One generated explainer video per day to the mister.mcp.a2a TikTok account.
 * Picks the oldest pending row from broadcast_topic_queue, asks Blotato to
 * generate an ai-story-video from that row's prompt_seed, waits for the render,
 * then stages and dispatches it through the DEPLOYED APP's owner-authenticated
 * broadcast routes.
 *
 * ── THIS REPO IS PUBLIC. SO ARE ITS ACTIONS LOGS. ───────────────────────────
 * Every console line here is readable by anyone. GitHub masks the literal
 * secret string behind ${{ secrets.X }} — it does NOT mask a secret that
 * arrives inside a response body we then print. So: status codes and named
 * fields only, never a whole response object, never a whole job row.
 * broadcast_jobs.submission_ids in particular can carry a raw_response blob
 * (the adapter stores one when it cannot find the post id), which is exactly
 * the kind of thing that must not be echoed here.
 *
 * GET /v2/users/me IS NEVER CALLED, here or anywhere in the publish path: it
 * returns the account's full Blotato API key in its response body.
 *
 * ── WHY IT CALLS THE APP RATHER THAN POSTING DIRECTLY ───────────────────────
 * Staging and dispatch go through https://<app>/api/broadcast/{jobs,dispatch},
 * which run the already-tested BlotatoAdapter — buildTarget()'s TikTok
 * required-field block, the caption resolution order, per-account submission
 * recording. Reimplementing that here would fork the publish logic across two
 * repos and let them drift. The worker therefore holds NO write access to
 * broadcast_jobs; its database role can only read the queue and mark a row
 * used.
 *
 * ── isAiGenerated ───────────────────────────────────────────────────────────
 * Set explicitly TRUE on every dispatch from this pipeline. buildTarget()
 * defaults it false, which is right for a human uploading their own footage
 * and wrong here: this content IS machine-generated and TikTok's disclosure
 * expects the flag to say so. It is passed per-call, never left to a default.
 *
 * Env (all via workflow secrets/vars):
 *   AIVE_INGEST_DATABASE_URL  scoped aive_ingest role — SELECT/UPDATE on the
 *                             queue, SELECT on the job tables. No writes.
 *   BLOTATO_API_KEY           video generation
 *   AIVE_BASE_URL             deployed app origin, e.g. https://aive.global
 *   AIVE_CRON_SECRET          Bearer for the app's requireOwnerAuth
 * Inputs (workflow_dispatch):
 *   BROADCAST_SKIP_TODAY=1        no-op this run
 *   BROADCAST_FORCE_TOPIC=<name>  use this topic instead of the queue order
 */

import { q, endIngestPool } from '../lib/ingest/db';
import { confirmPublished, submissionIdFor } from '../lib/broadcast/publishConfirm';

/* mister.mcp.a2a — broadcast_accounts.id. The OTHER TWO CONNECTED ACCOUNTS
   (vice.marshal.kyli, wwwsnowbunnymafiacom) ARE OUT OF SCOPE for this
   pipeline and must never appear here. This is the only account id this file
   is permitted to name. */
const MISTER_MCP_A2A = '09726d10-857d-468f-8e01-b70361c17fa1';

const BLOTATO_BASE = 'https://backend.blotato.com';
/* Confirmed against Blotato's OpenAPI reference 2026-09-02. A top-level
   `prompt` auto-fills the template's inputs, so the caller does not build the
   scenes array — which is what lets one prompt_seed drive a whole video. */
const AI_STORY_TEMPLATE = '/base/v2/ai-story-video/5903fe43-514d-40ee-a060-0d6628c5f8fd/v1';

/* Generation ran to several minutes in manual testing. 15s floor per the
   brief; 40 polls * 15s = 10 minutes of waiting, inside the job's 30-minute
   timeout with room for the render to finish and the post to go out. */
const POLL_INTERVAL_MS = 15_000;
const POLL_MAX_ATTEMPTS = 40;

const TERMINAL_OK = 'done';
const TERMINAL_FAIL = 'creation-from-template-failed';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function requireEnv(name: string): string {
  const v = process.env[name]?.trim();
  if (!v) throw new Error(`${name} is not set`);
  return v;
}

/** Headers for Blotato. Never logged. */
function blotatoHeaders(): Record<string, string> {
  return { 'blotato-api-key': requireEnv('BLOTATO_API_KEY'), 'Content-Type': 'application/json' };
}

interface TopicRow {
  id: string;
  topic_name: string;
  layer: string;
  evidence_status: string;
  prompt_seed: string;
  /** The exact published caption. Reviewed in the queue; never composed here. */
  caption: string | null;
  /** An already-rendered video to publish INSTEAD of generating one. Single-use:
      cleared when the topic is marked used, so the queue loop cannot repost it. */
  pregenerated_media_url: string | null;
}

/**
 * Oldest pending topic. When every topic has been used the queue LOOPS: all
 * used rows go back to pending and the rotation starts again. Running out of
 * topics is the expected steady state after 17 days, not an error.
 */
async function nextTopic(forceName: string | null): Promise<TopicRow | null> {
  if (forceName) {
    const rows = await q<TopicRow>(
      `SELECT id, topic_name, layer, evidence_status, prompt_seed, caption,
              pregenerated_media_url
         FROM broadcast_topic_queue WHERE topic_name = $1 LIMIT 1`,
      [forceName],
    );
    return rows[0] ?? null;
  }

  /* ORDER BY queue_position — the explicit rotation set in migration
     20260902000004. NOT created_at: the whole seed shares a single timestamp,
     so that ordering fell through to an alphabetical tiebreak on topic_name.
     It happened to be a reasonable sequence, but it was an accident a rename
     or a newly inserted row would silently change, and posting order on a
     public channel should be a decision. */
  const pick = async () =>
    (await q<TopicRow>(
      `SELECT id, topic_name, layer, evidence_status, prompt_seed, caption,
              pregenerated_media_url
         FROM broadcast_topic_queue
        WHERE status = 'pending'
        ORDER BY queue_position
        LIMIT 1`,
    ))[0] ?? null;

  const first = await pick();
  if (first) return first;

  // Loop back. 'skipped' rows are deliberately NOT revived — a human skipped
  // them and that decision should survive the rotation.
  const reset = await q<{ id: string }>(
    `UPDATE broadcast_topic_queue
        SET status = 'pending', used_at = NULL
      WHERE status = 'used'
      RETURNING id`,
  );
  console.log(`[broadcast-daily] queue exhausted — looped ${reset.length} topic(s) back to pending`);
  return pick();
}

/* Hard ceiling across BOTH formats for this account, per UTC day. Kept
   identical to the same constant in broadcast-text-card.ts. */
const MAX_POSTS_PER_DAY = 3;

/**
 * Should this run stand down? Two separate reasons, and they are not the same
 * question.
 *
 * THIS USED TO COUNT EVERY JOB FOR THE ACCOUNT and stop if there was one. That
 * was correct while video was the only format. Once text cards began posting
 * at 10:00 UTC, a card would have made this true and SILENTLY SUPPRESSED the
 * 14:00 video every single day — the daily video would simply have stopped,
 * with a green run saying "daily cap holds". Scheduling around it is not a fix
 * either: GitHub's dispatch on this repo drifts by hours, so no ordering of
 * cron times can be relied on.
 *
 * So the two questions are asked separately:
 *   - has a VIDEO gone out today? That is this worker's own cadence, and
 *     broadcast_topic_queue.used_at is the marker, written only on a confirmed
 *     post. A card does not touch it.
 *   - is the account at its overall ceiling? broadcast_jobs is authoritative
 *     for that: it counts posts of any format, including anything sent by
 *     hand, which the queue columns cannot see.
 */
async function standDownReason(): Promise<string | null> {
  const [jobs] = await q<{ n: string }>(
    `SELECT count(*)::text AS n
       FROM broadcast_job_accounts ja
       JOIN broadcast_jobs j ON j.id = ja.job_id
      WHERE ja.account_id = $1
        AND j.created_at >= date_trunc('day', now() AT TIME ZONE 'UTC')
        AND j.status IN ('posted', 'pending_api')`,
    [MISTER_MCP_A2A],
  );
  const [queue] = await q<{ videos: string; cards: string }>(
    `SELECT
       count(*) FILTER (WHERE used_at        >= date_trunc('day', now() AT TIME ZONE 'UTC'))::text AS videos,
       count(*) FILTER (WHERE card_posted_at >= date_trunc('day', now() AT TIME ZONE 'UTC'))::text AS cards
       FROM broadcast_topic_queue`,
  );
  const total = Number(jobs?.n ?? '0');
  const videos = Number(queue?.videos ?? '0');
  const cards = Number(queue?.cards ?? '0');
  console.log(`[broadcast-daily] today so far — videos=${videos} cards=${cards} total_jobs=${total} (ceiling ${MAX_POSTS_PER_DAY})`);

  if (videos > 0) return 'a video has already gone out today';
  if (total >= MAX_POSTS_PER_DAY) return `account is at its ${MAX_POSTS_PER_DAY}-post daily ceiling`;
  return null;
}

/** Start generation. Returns the creation id. */
async function startGeneration(promptSeed: string): Promise<string> {
  const res = await fetch(`${BLOTATO_BASE}/v2/videos/from-templates`, {
    method: 'POST',
    headers: blotatoHeaders(),
    body: JSON.stringify({
      templateId: AI_STORY_TEMPLATE,
      inputs: {},
      prompt: promptSeed,
      render: true,
    }),
  });
  const body = await res.text();
  // Status only. The body may echo request context; it is not printed.
  if (!res.ok) throw new Error(`generation start failed: HTTP ${res.status}`);
  let parsed: Record<string, any>;
  try { parsed = JSON.parse(body); } catch { throw new Error('generation start returned unparseable JSON'); }
  // Confirmed live 2026-09-02: the 201 body is { item: { id, status, ... } },
  // so the id is at item.id. The other shapes stay as fallbacks rather than
  // being removed -- a single measured response is not a contract.
  const id = parsed.item?.id ?? parsed.id ?? parsed.creationId ?? null;
  if (!id) throw new Error('generation start returned no creation id');
  return String(id);
}

/** Poll to a terminal state. Returns the rendered mediaUrl. */
async function awaitRender(creationId: string): Promise<string> {
  let lastStatus = '(none)';
  for (let attempt = 1; attempt <= POLL_MAX_ATTEMPTS; attempt++) {
    await sleep(POLL_INTERVAL_MS);
    const res = await fetch(`${BLOTATO_BASE}/v2/videos/creations/${encodeURIComponent(creationId)}`, {
      headers: blotatoHeaders(),
    });
    const body = await res.text();
    if (!res.ok) {
      // A transient read while rendering is not fatal; keep polling.
      console.log(`[broadcast-daily] poll ${attempt}/${POLL_MAX_ATTEMPTS} — HTTP ${res.status}, retrying`);
      continue;
    }
    let parsed: Record<string, any>;
    try { parsed = JSON.parse(body); } catch { continue; }
    const item = parsed.item ?? parsed;
    lastStatus = String(item.status ?? 'unknown');
    console.log(`[broadcast-daily] poll ${attempt}/${POLL_MAX_ATTEMPTS} — status=${lastStatus}`);

    if (lastStatus === TERMINAL_FAIL) throw new Error('generation failed upstream (creation-from-template-failed)');
    if (lastStatus === TERMINAL_OK) {
      const url = item.mediaUrl ?? null;
      if (!url) throw new Error('generation reported done but carried no mediaUrl');
      return String(url);
    }
  }
  throw new Error(`generation did not finish within ${POLL_MAX_ATTEMPTS} polls (last status ${lastStatus})`);
}

/**
 * The caption, read straight from the row.
 *
 * IT IS STORED, NOT BUILT. The previous version composed one from
 * `evidence_status` plus the words "Part of the agentic ecosystem index" — two
 * pieces of AIVE framing on every post, in the most visible field there is.
 * Composing a caption from columns is what let that happen, so composition is
 * gone: broadcast_topic_queue.caption holds the exact published text and is
 * reviewed there before anything can use it.
 *
 * NO FALLBACK, BY DESIGN. A missing caption THROWS and fails the run. A
 * generated stand-in is precisely how the framing would creep back, and a post
 * that goes out with the wrong caption cannot be recalled — a red run can.
 */
function buildCaption(topic: TopicRow): string {
  const caption = topic.caption?.trim();
  if (!caption) {
    throw new Error(`topic "${topic.topic_name}" has no caption — refusing to post without reviewed copy`);
  }
  return caption;
}

async function appPost(path: string, payload: unknown): Promise<Record<string, any>> {
  const res = await fetch(`${requireEnv('AIVE_BASE_URL').replace(/\/$/, '')}${path}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${requireEnv('AIVE_CRON_SECRET')}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
  const body = await res.text();
  if (!res.ok) throw new Error(`${path} failed: HTTP ${res.status}`);
  try { return JSON.parse(body); } catch { throw new Error(`${path} returned unparseable JSON`); }
}

async function main(): Promise<void> {
  const startedAt = Date.now();

  if (process.env.BROADCAST_SKIP_TODAY?.trim() === '1') {
    console.log('[broadcast-daily] BROADCAST_SKIP_TODAY=1 — no post this run, queue untouched');
    return;
  }

  const standDown = await standDownReason();
  if (standDown) {
    // Not a failure: the cap did its job. Exit 0 so a double-fire is quiet.
    console.log(`[broadcast-daily] standing down — ${standDown}; nothing sent`);
    return;
  }

  const forced = process.env.BROADCAST_FORCE_TOPIC?.trim() || null;
  const topic = await nextTopic(forced);
  if (!topic) throw new Error(forced ? `forced topic not found: ${forced}` : 'no topic available');
  console.log(`[broadcast-daily] topic=${topic.topic_name} layer=${topic.layer} evidence=${topic.evidence_status}${forced ? ' (forced)' : ''}`);

  /* A topic can carry an already-rendered video, in which case generation is
     skipped entirely. That is how a render produced during review gets
     published instead of paying for a second one. The URL is consumed once --
     cleared in the same statement that marks the topic used -- because the
     rotation loops and a leftover URL would republish the same video forever. */
  let mediaUrl: string;
  if (topic.pregenerated_media_url?.trim()) {
    mediaUrl = topic.pregenerated_media_url.trim();
    console.log('[broadcast-daily] using pre-rendered video for this topic — generation skipped');
  } else {
    const creationId = await startGeneration(topic.prompt_seed);
    console.log(`[broadcast-daily] generation started — creation ${creationId}`);
    mediaUrl = await awaitRender(creationId);
    console.log('[broadcast-daily] render complete');
  }

  const caption = buildCaption(topic);
  const staged = await appPost('/api/broadcast/jobs', {
    source_video_url: mediaUrl,
    caption,
    platform_targets: ['tiktok'],
    accounts: [{ account_id: MISTER_MCP_A2A }],
  });
  const jobId = staged.job?.id;
  if (!jobId) throw new Error('staging returned no job id');
  console.log(`[broadcast-daily] staged job ${jobId}`);

  const dispatched = await appPost('/api/broadcast/dispatch', {
    jobId,
    // Explicit, not inherited: this content is machine-generated.
    targetOptions: { isAiGenerated: true },
  });

  const status = dispatched.job?.status ?? 'unknown';
  // The per-account outcome, reduced to a count. submission_ids is NOT printed:
  // it can carry a raw response blob and this log is public.
  const submissions = dispatched.job?.submission_ids ?? {};
  const accepted = Object.values(submissions).filter((s: any) => s?.status === 'posted').length;
  const total = Object.keys(submissions).length;
  console.log(`[broadcast-daily] dispatch status=${status} accepted=${accepted}/${total}`);

  if (status !== 'posted') {
    // Loud, and the topic stays pending so tomorrow retries it.
    throw new Error(`dispatch did not post (status=${status}, accepted=${accepted}/${total})`);
  }

  /* ACCEPTED IS NOT PUBLISHED -- see lib/broadcast/publishConfirm.ts. Confirm
     the video is actually live before marking the topic used and clearing
     pregenerated_media_url; a throw here leaves BOTH intact so tomorrow
     retries the same topic with the same pre-rendered video. */
  const submissionId = submissionIdFor(dispatched, MISTER_MCP_A2A);
  if (!submissionId) {
    throw new Error('dispatch reported posted but carried no submission id — cannot confirm the post went live');
  }
  const confirmed = await confirmPublished(
    submissionId, requireEnv('BLOTATO_API_KEY'), (m) => console.log(`[broadcast-daily] ${m}`));
  console.log(`[broadcast-daily] publish confirmed${confirmed.publicUrl ? ` — ${confirmed.publicUrl}` : ''}`);

  // Clearing pregenerated_media_url here, in the same statement, is what makes
  // it single-use. Doing it separately would leave a window where a crash
  // between the two writes republishes the same video on the next loop.
  await q(
    `UPDATE broadcast_topic_queue
        SET status = 'used', used_at = now(), pregenerated_media_url = NULL
      WHERE id = $1`,
    [topic.id],
  );
  console.log(`[broadcast-daily] DONE topic=${topic.topic_name} job=${jobId} elapsed_s=${((Date.now() - startedAt) / 1000).toFixed(0)}`);
}

main()
  .then(async () => { await endIngestPool(); })
  .catch(async (err) => {
    // Message only. Never the stack's captured values, never a response body.
    console.error(`[broadcast-daily] FAILED: ${err instanceof Error ? err.message : String(err)}`);
    await endIngestPool().catch(() => {});
    process.exit(1);
  });
