/**
 * workers/broadcast-text-card.ts
 *
 * Twice-daily TEXT CARD to the mister.mcp.a2a TikTok account — a rendered
 * quote image, not a video. Runs on its own schedule and its own rotation
 * cursor, so a topic's card and its video advance independently.
 *
 * ── THIS REPO IS PUBLIC. SO ARE ITS ACTIONS LOGS. ───────────────────────────
 * Status codes and named fields only. Never a whole response object, never a
 * whole job row — broadcast_jobs.submission_ids can carry a raw_response blob.
 * GET /v2/users/me is NEVER called here or anywhere in the publish path: it
 * returns the account's full Blotato API key in its response body.
 *
 * ── ZERO CREDIT COST ────────────────────────────────────────────────────────
 * The Single Centered Text Quote template is pure text rendering — no AI
 * image, video or voice generation. Measured 2026-09-03: a full render took
 * the account from 1520 credits to 1520. That is the whole reason this format
 * can run twice a day when the video runs once.
 *
 * ── SHAPES, ALL MEASURED RATHER THAN ASSUMED (2026-09-03) ───────────────────
 *   templateId must be the BARE UUID. The '/base/v2/<slug>/<uuid>/v1' path
 *     form used by the ai-story-video template returns 404 "Unknown template
 *     ID" here.
 *   The finished creation carries imageUrls[0] and NO mediaUrl — the opposite
 *     of the video template. Reading mediaUrl would yield null forever.
 *   autoAddMusic is a TikTok TARGET field, alongside isAiGenerated, so it
 *     rides through targetOptions and needs no adapter change.
 *
 * Env:
 *   AIVE_INGEST_DATABASE_URL  scoped aive_ingest role — SELECT/UPDATE on the
 *                             queue, SELECT on the job tables. No writes.
 *   BLOTATO_API_KEY           card rendering
 *   AIVE_BASE_URL             deployed app origin
 *   AIVE_CRON_SECRET          Bearer for the app's requireOwnerAuth
 * Inputs (workflow_dispatch):
 *   BROADCAST_SKIP_TODAY=1        no-op this run
 *   BROADCAST_FORCE_TOPIC=<name>  use this topic instead of the rotation
 */

import { q, endIngestPool } from '../lib/ingest/db';
import { confirmPublished, submissionIdFor } from '../lib/broadcast/publishConfirm';

/* mister.mcp.a2a. The other two connected accounts (vice.marshal.kyli,
   wwwsnowbunnymafiacom) are out of scope and must never appear here. */
const MISTER_MCP_A2A = '09726d10-857d-468f-8e01-b70361c17fa1';

const BLOTATO_BASE = 'https://backend.blotato.com';
/* "Single Centered Text Quote". BARE UUID — see the shapes note above. */
const QUOTE_TEMPLATE_ID = '9f4e66cd-b784-4c02-b2ce-e6d0765fd4c0';
/* The template caps each quote string at 350 characters. Every current
   caption lands near 130, but the cap is enforced here rather than trusted:
   an over-long card is otherwise rejected by Blotato mid-run, after the
   rotation has already been consulted. */
const QUOTE_MAX_CHARS = 350;

/* Text rendering finishes in seconds, not minutes — the measured render was
   done on the first poll. Kept generous anyway: the cost of waiting is a few
   idle seconds, the cost of giving up early is a wasted slot. */
const POLL_INTERVAL_MS = 5_000;
const POLL_MAX_ATTEMPTS = 24;
const TERMINAL_OK = 'done';
const TERMINAL_FAIL = 'creation-from-template-failed';

/* Hard ceiling across BOTH formats for this account, per UTC day. */
const MAX_POSTS_PER_DAY = 3;
/* And at most this many cards, matching the twice-daily schedule. */
const MAX_CARDS_PER_DAY = 2;

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
  caption: string | null;
}

const TOPIC_COLS = 'id, topic_name, layer, evidence_status, caption';

/**
 * Oldest topic not yet used as a card. When every active topic has had one the
 * rotation LOOPS: used_as_card goes back to false and the cycle restarts.
 * Running out is the expected steady state after nine days at two a day, not
 * an error.
 *
 * ORDER BY queue_position, matching the video worker. NOT created_at: the
 * whole seed shares one timestamp, so that ordering falls through to an
 * alphabetical tiebreak on topic_name — an accident a rename would silently
 * change, and posting order on a public channel should be a decision.
 *
 * status <> 'skipped' is honoured here too. One skip flag, both formats: the
 * AIVE topic is withheld from the channel entirely, not merely from video.
 */
async function nextTopic(forceName: string | null): Promise<TopicRow | null> {
  if (forceName) {
    const rows = await q<TopicRow>(
      `SELECT ${TOPIC_COLS} FROM broadcast_topic_queue
        WHERE topic_name = $1 AND status <> 'skipped' LIMIT 1`,
      [forceName],
    );
    return rows[0] ?? null;
  }

  const pick = async () =>
    (await q<TopicRow>(
      `SELECT ${TOPIC_COLS} FROM broadcast_topic_queue
        WHERE used_as_card = false AND status <> 'skipped'
        ORDER BY queue_position
        LIMIT 1`,
    ))[0] ?? null;

  const first = await pick();
  if (first) return first;

  const reset = await q<{ id: string }>(
    `UPDATE broadcast_topic_queue SET used_as_card = false
      WHERE used_as_card = true AND status <> 'skipped' RETURNING id`,
  );
  console.log(`[broadcast-card] card rotation exhausted — looped ${reset.length} topic(s) back`);
  return pick();
}

/**
 * Today's posting counts for this account, by format.
 *
 * WHY TWO SOURCES. broadcast_jobs is the authoritative record that a post
 * happened — it also catches anything posted by hand, which the queue columns
 * cannot see — so it enforces the hard ceiling. But a job row does not say
 * which FORMAT it was, and the two formats need separate cadences. So the
 * queue's own markers (used_at for video, card_posted_at for cards) do the
 * attribution. Both are written only after a confirmed post.
 */
async function postingCountsToday(): Promise<{ total: number; cards: number; videos: number }> {
  const [jobs] = await q<{ n: string }>(
    `SELECT count(*)::text AS n
       FROM broadcast_job_accounts ja
       JOIN broadcast_jobs j ON j.id = ja.job_id
      WHERE ja.account_id = $1
        AND j.created_at >= date_trunc('day', now() AT TIME ZONE 'UTC')
        AND j.status IN ('posted', 'pending_api')`,
    [MISTER_MCP_A2A],
  );
  const [queue] = await q<{ cards: string; videos: string }>(
    `SELECT
       count(*) FILTER (WHERE card_posted_at >= date_trunc('day', now() AT TIME ZONE 'UTC'))::text AS cards,
       count(*) FILTER (WHERE used_at        >= date_trunc('day', now() AT TIME ZONE 'UTC'))::text AS videos
       FROM broadcast_topic_queue`,
  );
  return {
    total: Number(jobs?.n ?? '0'),
    cards: Number(queue?.cards ?? '0'),
    videos: Number(queue?.videos ?? '0'),
  };
}

/**
 * The card's rendered text, derived from the reviewed caption.
 *
 * THE CAPTION ITSELF IS NEVER REWRITTEN — it is posted verbatim as the post
 * text, hashtags and all. What is derived here is only what gets DRAWN on the
 * image, and two things differ there:
 *
 *   1. The question becomes a headline. Splitting on the first "? " puts the
 *      question on its own line and the explanation below it, which is the
 *      shape the template renders well and the shape that was reviewed.
 *   2. Trailing hashtags are dropped. "#A2A #AI #AIagents" is discovery
 *      metadata for the feed; rendered inside a quote card it is visual noise,
 *      and it was not present in the approved sample card.
 *
 * NO FALLBACK. A caption with no "? " does not get posted as one undivided
 * blob — it throws, the rotation is untouched, and the run goes red. A card
 * that goes out wrong cannot be recalled; a red run can.
 */
export function buildCardText(topic: { topic_name: string; caption: string | null }): string {
  const caption = topic.caption?.trim();
  if (!caption) {
    throw new Error(`topic "${topic.topic_name}" has no caption — refusing to render without reviewed copy`);
  }

  const at = caption.indexOf('? ');
  if (at === -1) {
    throw new Error(
      `topic "${topic.topic_name}" caption has no "? " split point — refusing to render an unsplit card`,
    );
  }

  const headline = caption.slice(0, at + 1).trim();
  /* Drop a TRAILING run of hashtags; a hashtag inside the sentence is left be.
     The run may begin at the very start of the body — "What is A2A? #A2A #AI"
     has no prose at all. An earlier version required whitespace before the
     first tag, so that case stripped all but the first and produced a card
     reading "#A2A", which is precisely the kind of thing that renders, posts,
     and cannot be recalled. It now strips to empty and the check below throws. */
  const body = caption
    .slice(at + 2)
    .replace(/(?:^|\s+)#[\p{L}\p{N}_-]+(?:\s+#[\p{L}\p{N}_-]+)*\s*$/u, '')
    .trim();

  if (!body) {
    throw new Error(`topic "${topic.topic_name}" caption has no body after the question — nothing to render`);
  }

  const text = `${headline}\n\n${body}`;
  if (text.length > QUOTE_MAX_CHARS) {
    throw new Error(
      `topic "${topic.topic_name}" card text is ${text.length} chars, over the template's ${QUOTE_MAX_CHARS} limit`,
    );
  }
  return text;
}

/** Start the card render. Returns the creation id. */
async function startCardRender(cardText: string): Promise<string> {
  const res = await fetch(`${BLOTATO_BASE}/v2/videos/from-templates`, {
    method: 'POST',
    headers: blotatoHeaders(),
    body: JSON.stringify({
      templateId: QUOTE_TEMPLATE_ID,
      // One string = one card. Several would make it a carousel.
      inputs: { quotes: [cardText] },
      render: true,
    }),
  });
  const body = await res.text();
  // Status only. The body may echo request context; it is not printed.
  if (!res.ok) throw new Error(`card render start failed: HTTP ${res.status}`);
  let parsed: Record<string, any>;
  try { parsed = JSON.parse(body); } catch { throw new Error('card render start returned unparseable JSON'); }
  const id = parsed.item?.id ?? parsed.id ?? parsed.creationId ?? null;
  if (!id) throw new Error('card render start returned no creation id');
  return String(id);
}

/**
 * Poll to a terminal state. Returns the rendered IMAGE url.
 *
 * imageUrls[0], not mediaUrl. Measured: this template finishes with mediaUrl
 * absent and the jpg in imageUrls. mediaUrl is still read as a fallback, since
 * one measured response is not a contract.
 */
async function awaitCard(creationId: string): Promise<string> {
  let lastStatus = '(none)';
  for (let attempt = 1; attempt <= POLL_MAX_ATTEMPTS; attempt++) {
    await sleep(POLL_INTERVAL_MS);
    const res = await fetch(`${BLOTATO_BASE}/v2/videos/creations/${encodeURIComponent(creationId)}`, {
      headers: blotatoHeaders(),
    });
    const body = await res.text();
    if (!res.ok) {
      console.log(`[broadcast-card] poll ${attempt}/${POLL_MAX_ATTEMPTS} — HTTP ${res.status}, retrying`);
      continue;
    }
    let parsed: Record<string, any>;
    try { parsed = JSON.parse(body); } catch { continue; }
    const item = parsed.item ?? parsed;
    lastStatus = String(item.status ?? 'unknown');
    console.log(`[broadcast-card] poll ${attempt}/${POLL_MAX_ATTEMPTS} — status=${lastStatus}`);

    if (lastStatus === TERMINAL_FAIL) throw new Error('card render failed upstream (creation-from-template-failed)');
    if (lastStatus === TERMINAL_OK) {
      const url = (Array.isArray(item.imageUrls) ? item.imageUrls[0] : null) ?? item.mediaUrl ?? null;
      if (!url) throw new Error('card render reported done but carried no image url');
      return String(url);
    }
  }
  throw new Error(`card render did not finish within ${POLL_MAX_ATTEMPTS} polls (last status ${lastStatus})`);
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
    console.log('[broadcast-card] BROADCAST_SKIP_TODAY=1 — no post this run, rotation untouched');
    return;
  }

  /* The caps exiting 0 is the point: a cap doing its job is not a failure, and
     a red run for "we already posted enough today" is how a red run stops
     meaning anything. */
  const counts = await postingCountsToday();
  console.log(
    `[broadcast-card] today so far — videos=${counts.videos} cards=${counts.cards} total_jobs=${counts.total} ` +
    `(caps: ${MAX_CARDS_PER_DAY} cards, ${MAX_POSTS_PER_DAY} total)`,
  );

  if (counts.total >= MAX_POSTS_PER_DAY) {
    console.log(`[broadcast-card] daily ceiling of ${MAX_POSTS_PER_DAY} post(s) reached — nothing sent`);
    return;
  }
  if (counts.cards >= MAX_CARDS_PER_DAY) {
    console.log(`[broadcast-card] ${MAX_CARDS_PER_DAY} card(s) already posted today — nothing sent`);
    return;
  }

  const forced = process.env.BROADCAST_FORCE_TOPIC?.trim() || null;
  const topic = await nextTopic(forced);
  if (!topic) throw new Error(forced ? `forced topic not found or skipped: ${forced}` : 'no topic available for a card');

  const cardText = buildCardText(topic);
  console.log(
    `[broadcast-card] format=text-card topic=${topic.topic_name} layer=${topic.layer} ` +
    `evidence=${topic.evidence_status}${forced ? ' (forced)' : ''} card_chars=${cardText.length}`,
  );

  const creationId = await startCardRender(cardText);
  console.log(`[broadcast-card] render started — creation ${creationId}`);
  const imageUrl = await awaitCard(creationId);
  console.log('[broadcast-card] render complete (0 credits — pure text template)');

  // The caption goes out verbatim, hashtags included. Only the CARD text drops them.
  const caption = topic.caption!.trim();
  const staged = await appPost('/api/broadcast/jobs', {
    // Named for video by history; the route accepts any http media URL and the
    // adapter uploads it the same way. A jpg travels this path unchanged.
    source_video_url: imageUrl,
    caption,
    platform_targets: ['tiktok'],
    accounts: [{ account_id: MISTER_MCP_A2A }],
  });
  const jobId = staged.job?.id;
  if (!jobId) throw new Error('staging returned no job id');
  console.log(`[broadcast-card] staged job ${jobId}`);

  const dispatched = await appPost('/api/broadcast/dispatch', {
    jobId,
    targetOptions: {
      // Explicit, not inherited: this content is machine-generated. The card is
      // Blotato-templated, the same disclosure standard as the videos.
      isAiGenerated: true,
      /* TEXT CARDS ONLY. A still image posts to TikTok with no audio at all;
         this asks TikTok to pick a background track at posting time. It is a
         TikTok-side selection, not a Blotato generation step, so it stays free.
         The videos must NOT carry it — they already have their own AI
         voiceover, and a second track would play over the narration. */
      autoAddMusic: true,
    },
  });

  const status = dispatched.job?.status ?? 'unknown';
  const submissions = dispatched.job?.submission_ids ?? {};
  const accepted = Object.values(submissions).filter((s: any) => s?.status === 'posted').length;
  const total = Object.keys(submissions).length;
  console.log(`[broadcast-card] dispatch status=${status} accepted=${accepted}/${total}`);

  if (status !== 'posted') {
    // Loud, and the rotation stays put so the next slot retries this topic.
    throw new Error(`dispatch did not post (status=${status}, accepted=${accepted}/${total})`);
  }

  /* ACCEPTED IS NOT PUBLISHED -- see lib/broadcast/publishConfirm.ts. Confirm
     the post is actually live before recording the topic as consumed. */
  const submissionId = submissionIdFor(dispatched, MISTER_MCP_A2A);
  if (!submissionId) {
    throw new Error('dispatch reported posted but carried no submission id — cannot confirm the post went live');
  }
  const confirmed = await confirmPublished(
    submissionId, requireEnv('BLOTATO_API_KEY'), (m) => console.log(`[broadcast-card] ${m}`));
  console.log(`[broadcast-card] publish confirmed${confirmed.publicUrl ? ` — ${confirmed.publicUrl}` : ''}`);

  /* Marked ONLY after a CONFIRMED PUBLISH, both columns in one statement.
     card_posted_at is what the per-day cap counts, so writing it separately
     would leave a window where a crash lets an extra card through. */
  await q(
    `UPDATE broadcast_topic_queue
        SET used_as_card = true, card_posted_at = now()
      WHERE id = $1`,
    [topic.id],
  );

  console.log(
    `[broadcast-card] POSTED format=text-card topic=${topic.topic_name} job=${jobId} ` +
    `elapsed_s=${((Date.now() - startedAt) / 1000).toFixed(1)}`,
  );
}

if (require.main === module) {
  main()
    .then(async () => { await endIngestPool(); process.exit(0); })
    .catch(async (e) => {
      console.error('[broadcast-card] FAILED:', e instanceof Error ? e.message : e);
      await endIngestPool();
      process.exit(1);
    });
}
