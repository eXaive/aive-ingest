/**
 * workers/broadcast-quiz-card.ts
 *
 * Three trivia cards a day per pool, to two TikTok accounts. ONE worker,
 * parameterised by QUIZ_CATEGORY, rather than two near-identical files: the
 * only thing that differs between GTA and Cat is the category string, the
 * account and the schedule.
 *
 * THE CARD CARRIES THE QUESTION ONLY. The correct answer is stored (every row
 * has it) and is deliberately NOT rendered, NOT put in the caption, and NOT
 * logged. The reveal happens by hand in the comments. buildCardText cannot
 * leak it: it reads question_text and the four options and never touches
 * correct_answer.
 *
 * ── THIS REPO IS PUBLIC. SO ARE ITS ACTIONS LOGS. ───────────────────────────
 * Status codes and named fields only -- never a whole response object, never a
 * whole job row. The answer key is never printed for the separate reason that
 * these logs are the game's own spoiler surface.
 * GET /v2/users/me is NEVER called: it returns the full Blotato API key.
 *
 * ── ZERO CREDIT COST ────────────────────────────────────────────────────────
 * Same Single Centered Text Quote template as the MCP/A2A cards: pure text
 * rendering, no AI image/video/voice step. Measured 2026-09-03 at 1520 credits
 * before and after a full render.
 *
 * Env:
 *   QUIZ_CATEGORY             GTA | CAT  (required)
 *   AIVE_INGEST_DATABASE_URL  scoped aive_ingest role -- SELECT/UPDATE on
 *                             quiz_questions, SELECT on the job tables
 *   BLOTATO_API_KEY           card rendering
 *   AIVE_BASE_URL             deployed app origin
 *   AIVE_CRON_SECRET          Bearer for the app's requireOwnerAuth
 * Inputs (workflow_dispatch):
 *   BROADCAST_SKIP_TODAY=1    no-op this run
 *   BROADCAST_FORCE_QID=<uuid> post this exact question instead of the rotation
 *   BROADCAST_DRY_RUN=1       render and report, publish NOTHING, touch nothing
 */

import { q, endIngestPool } from '../lib/ingest/db';
import { confirmPublished, submissionIdFor } from '../lib/broadcast/publishConfirm';

/* Confirmed with the account owner 2026-09-05 before wiring. Both accounts had
   zero posting history and Blotato exposes no display name or bio, so nothing
   in any system could have told these apart -- it was asked, not inferred.
   mister.mcp.a2a is NOT here: that account is the protocol channel and is out
   of scope for trivia. */
const POOLS = {
  GTA: {
    accountId: 'bd7d3219-0d23-4523-9f07-7b8bd4e55e74', // vice.marshal.kyli
    handle: 'vice.marshal.kyli',
    hashtags: '#GTA #GTA6 #gaming #trivia',
  },
  CAT: {
    accountId: 'f05c3ccf-630d-4d74-ac28-6bc7cc560ba4', // wwwsnowbunnymafiacom
    handle: 'wwwsnowbunnymafiacom',
    hashtags: '#cats #cattrivia #catsoftiktok #trivia',
  },
} as const;

type QuizCategory = keyof typeof POOLS;

const BLOTATO_BASE = 'https://backend.blotato.com';
/* "Single Centered Text Quote". BARE UUID -- the '/base/v2/<slug>/<uuid>/v1'
   path form used by the ai-story-video template 404s here. */
const QUOTE_TEMPLATE_ID = '9f4e66cd-b784-4c02-b2ce-e6d0765fd4c0';
/* The template caps each quote string at 350 characters. The longest current
   question renders to 213, but the cap is enforced rather than trusted: an
   over-long card is otherwise rejected by Blotato mid-run, after the rotation
   has already been consulted. */
const QUOTE_MAX_CHARS = 350;

const POLL_INTERVAL_MS = 5_000;
const POLL_MAX_ATTEMPTS = 24;
const TERMINAL_OK = 'done';
const TERMINAL_FAIL = 'creation-from-template-failed';

/* Hard ceiling per account per UTC day, across everything this pipeline posts.
   Three slots, three posts. */
const MAX_POSTS_PER_DAY = 3;

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

function requireCategory(): QuizCategory {
  const raw = process.env.QUIZ_CATEGORY?.trim().toUpperCase();
  if (raw !== 'GTA' && raw !== 'CAT') {
    throw new Error(`QUIZ_CATEGORY must be GTA or CAT, got ${JSON.stringify(process.env.QUIZ_CATEGORY ?? null)}`);
  }
  return raw;
}

interface QuizRow {
  id: string;
  question_text: string;
  option_a: string;
  option_b: string;
  option_c: string;
  option_d: string | null;
  category: string;
}

/* correct_answer is deliberately absent from this projection. The worker has
   no reason to read the key, and not selecting it means it cannot reach a log
   line or a caption by accident. */
const QUIZ_COLS = 'id, question_text, option_a, option_b, option_c, option_d, category';

/**
 * Oldest question in this category not yet posted. When the category is
 * exhausted the rotation LOOPS: its rows go back to unposted and the cycle
 * restarts. Running out is the expected steady state -- GTA lasts 14 days at
 * three a day, Cat only 7 -- not an error.
 *
 * Scoped to ONE category on both the pick and the reset, so the two pools
 * advance and exhaust independently.
 */
async function nextQuestion(category: QuizCategory, forceId: string | null): Promise<QuizRow | null> {
  if (forceId) {
    const rows = await q<QuizRow>(
      `SELECT ${QUIZ_COLS} FROM quiz_questions
        WHERE id = $1 AND category = $2 AND active = true LIMIT 1`,
      [forceId, category],
    );
    return rows[0] ?? null;
  }

  const pick = async () =>
    (await q<QuizRow>(
      `SELECT ${QUIZ_COLS} FROM quiz_questions
        WHERE category = $1 AND active = true AND used_as_quiz_card = false
        ORDER BY created_at, id
        LIMIT 1`,
      [category],
    ))[0] ?? null;

  const first = await pick();
  if (first) return first;

  const reset = await q<{ id: string }>(
    `UPDATE quiz_questions SET used_as_quiz_card = false
      WHERE category = $1 AND active = true AND used_as_quiz_card = true RETURNING id`,
    [category],
  );
  console.log(`[quiz-card] ${category} rotation exhausted — looped ${reset.length} question(s) back`);
  return pick();
}

/**
 * Today's post count for this account.
 *
 * broadcast_jobs is the authoritative record that a post happened -- it is
 * what the cap enforces. It cannot say WHICH question a job carried, so
 * quiz_posted_at does that attribution separately; both are reported.
 *
 * KNOWN LIMIT, stated rather than papered over: a post made by hand through
 * Blotato never creates a broadcast_jobs row, so it is invisible here. This
 * cap reliably prevents THIS pipeline from over-posting; it cannot police
 * manual posts to the same account.
 */
async function postCountsToday(accountId: string, category: QuizCategory): Promise<{ jobs: number; cards: number }> {
  const [jobs] = await q<{ n: string }>(
    `SELECT count(*)::text AS n
       FROM broadcast_job_accounts ja
       JOIN broadcast_jobs j ON j.id = ja.job_id
      WHERE ja.account_id = $1
        AND j.created_at >= date_trunc('day', now() AT TIME ZONE 'UTC')
        AND j.status IN ('posted', 'pending_api')`,
    [accountId],
  );
  const [cards] = await q<{ n: string }>(
    `SELECT count(*)::text AS n FROM quiz_questions
      WHERE category = $1 AND quiz_posted_at >= date_trunc('day', now() AT TIME ZONE 'UTC')`,
    [category],
  );
  return { jobs: Number(jobs?.n ?? '0'), cards: Number(cards?.n ?? '0') };
}

/**
 * The rendered card: the question, then the four options as lettered lines.
 *
 * NO ANSWER, ANYWHERE. Not on the card, not in the caption. correct_answer is
 * not even selected from the database (see QUIZ_COLS), so there is no value in
 * scope that could leak into either.
 *
 * NO BRANDING. The card is the question and its options and nothing else -- no
 * appended "part of X", no account name, no system framing. The source rows
 * were audited clean; this function is the other place branding could enter,
 * and it does not compose anything.
 *
 * A row that cannot render a complete four-option card THROWS. The pools were
 * verified to have four non-empty options on all 60 rows, so a failure here
 * means the data changed -- worth a red run, not a three-option card.
 */
export function buildCardText(row: {
  id: string; question_text: string;
  option_a: string; option_b: string; option_c: string; option_d: string | null;
}): string {
  const question = row.question_text?.trim();
  if (!question) throw new Error(`question ${row.id} has no text`);

  const options = [row.option_a, row.option_b, row.option_c, row.option_d];
  const letters = ['A', 'B', 'C', 'D'];
  const lines: string[] = [];
  for (let i = 0; i < 4; i++) {
    const v = options[i]?.trim();
    if (!v) {
      throw new Error(`question ${row.id} is missing option ${letters[i]} — refusing to post a partial card`);
    }
    lines.push(`${letters[i]}) ${v}`);
  }

  const text = `${question}\n\n${lines.join('\n')}`;
  if (text.length > QUOTE_MAX_CHARS) {
    throw new Error(`question ${row.id} renders to ${text.length} chars, over the template's ${QUOTE_MAX_CHARS} limit`);
  }
  return text;
}

/**
 * The post caption. The question plus the pool's hashtags -- discovery
 * metadata, which belongs in the feed but not drawn on the card.
 *
 * Deliberately does NOT carry the options (they are on the image), the answer,
 * or any system/brand framing.
 */
export function buildCaption(row: { question_text: string }, hashtags: string): string {
  const question = row.question_text.trim();
  return `${question}\n\nAnswer in the comments 👇\n\n${hashtags}`;
}

/** Start the card render. Returns the creation id. */
async function startCardRender(cardText: string): Promise<string> {
  const res = await fetch(`${BLOTATO_BASE}/v2/videos/from-templates`, {
    method: 'POST',
    headers: blotatoHeaders(),
    body: JSON.stringify({
      templateId: QUOTE_TEMPLATE_ID,
      inputs: { quotes: [cardText] }, // one string = one card, not a carousel
      render: true,
    }),
  });
  const body = await res.text();
  if (!res.ok) throw new Error(`card render start failed: HTTP ${res.status}`);
  let parsed: Record<string, any>;
  try { parsed = JSON.parse(body); } catch { throw new Error('card render start returned unparseable JSON'); }
  const id = parsed.item?.id ?? parsed.id ?? parsed.creationId ?? null;
  if (!id) throw new Error('card render start returned no creation id');
  return String(id);
}

/**
 * Poll to a terminal state. Returns the rendered IMAGE url.
 * imageUrls[0], not mediaUrl: this template finishes with mediaUrl absent.
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
      console.log(`[quiz-card] poll ${attempt}/${POLL_MAX_ATTEMPTS} — HTTP ${res.status}, retrying`);
      continue;
    }
    let parsed: Record<string, any>;
    try { parsed = JSON.parse(body); } catch { continue; }
    const item = parsed.item ?? parsed;
    lastStatus = String(item.status ?? 'unknown');
    console.log(`[quiz-card] poll ${attempt}/${POLL_MAX_ATTEMPTS} — status=${lastStatus}`);

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
  const category = requireCategory();
  const pool = POOLS[category];
  const dryRun = process.env.BROADCAST_DRY_RUN?.trim() === '1';

  console.log(`[quiz-card] category=${category} account=${pool.handle}${dryRun ? '  DRY RUN — nothing will be published' : ''}`);

  if (process.env.BROADCAST_SKIP_TODAY?.trim() === '1') {
    console.log('[quiz-card] BROADCAST_SKIP_TODAY=1 — no post this run, rotation untouched');
    return;
  }

  /* Caps exiting 0 is the point: a cap doing its job is not a failure, and a
     red run for "already posted enough today" is how a red run stops meaning
     anything. Skipped on a dry run, which publishes nothing and so cannot
     breach a cap. */
  const counts = await postCountsToday(pool.accountId, category);
  console.log(`[quiz-card] today — ${pool.handle} jobs=${counts.jobs}/${MAX_POSTS_PER_DAY}, ${category} cards=${counts.cards}`);
  if (!dryRun && counts.jobs >= MAX_POSTS_PER_DAY) {
    console.log(`[quiz-card] daily ceiling of ${MAX_POSTS_PER_DAY} post(s) reached for ${pool.handle} — nothing sent`);
    return;
  }

  const forceId = process.env.BROADCAST_FORCE_QID?.trim() || null;
  const row = await nextQuestion(category, forceId);
  if (!row) throw new Error(forceId ? `forced question not found in ${category}: ${forceId}` : `no ${category} question available`);

  const cardText = buildCardText(row);
  const caption = buildCaption(row, pool.hashtags);
  console.log(`[quiz-card] question=${row.id} chars=${cardText.length}${forceId ? ' (forced)' : ''}`);

  const creationId = await startCardRender(cardText);
  console.log(`[quiz-card] render started — creation ${creationId}`);
  const imageUrl = await awaitCard(creationId);
  console.log('[quiz-card] render complete (0 credits — pure text template)');

  if (dryRun) {
    console.log('[quiz-card] DRY RUN — card rendered, publishing skipped, rotation untouched');
    console.log(`[quiz-card] DRY RUN card text:\n${cardText}`);
    console.log(`[quiz-card] DRY RUN image: ${imageUrl}`);
    console.log(`[quiz-card] DRY RUN elapsed_s=${((Date.now() - startedAt) / 1000).toFixed(1)}`);
    return;
  }

  const staged = await appPost('/api/broadcast/jobs', {
    // Named for video by history; the route accepts any http media URL and the
    // adapter uploads it the same way. A jpg travels this path unchanged.
    source_video_url: imageUrl,
    caption,
    platform_targets: ['tiktok'],
    accounts: [{ account_id: pool.accountId }],
  });
  const jobId = staged.job?.id;
  if (!jobId) throw new Error('staging returned no job id');
  console.log(`[quiz-card] staged job ${jobId}`);

  const dispatched = await appPost('/api/broadcast/dispatch', {
    jobId,
    targetOptions: {
      // Explicit, not inherited: the card is Blotato-templated, same
      // disclosure standard as every other machine-made post here.
      isAiGenerated: true,
      /* A still image posts to TikTok with no audio; this asks TikTok to pick
         a track at posting time. A TikTok-side selection, not a generation
         step, so it stays free. */
      autoAddMusic: true,
    },
  });

  const status = dispatched.job?.status ?? 'unknown';
  const submissions = dispatched.job?.submission_ids ?? {};
  const accepted = Object.values(submissions).filter((s: any) => s?.status === 'posted').length;
  const total = Object.keys(submissions).length;
  console.log(`[quiz-card] dispatch status=${status} accepted=${accepted}/${total}`);

  if (status !== 'posted') {
    // Loud, and the rotation stays put so the next slot retries this question.
    throw new Error(`dispatch did not post (status=${status}, accepted=${accepted}/${total})`);
  }

  /* ACCEPTED IS NOT PUBLISHED. status='posted' means Blotato took the post,
     not that it went live -- publishing is asynchronous and took about a
     minute when measured. Confirm the live state before recording that this
     question was consumed; anything other than `published` throws, leaving the
     rotation untouched so the next slot retries it. */
  const submissionId = submissionIdFor(dispatched, pool.accountId);
  if (!submissionId) {
    throw new Error('dispatch reported posted but carried no submission id — cannot confirm the post went live');
  }
  const confirmed = await confirmPublished(
    submissionId, requireEnv('BLOTATO_API_KEY'), (m) => console.log(`[quiz-card] ${m}`));
  console.log(`[quiz-card] publish confirmed${confirmed.publicUrl ? ` — ${confirmed.publicUrl}` : ''}`);

  /* Marked ONLY after a CONFIRMED PUBLISH, both columns in one statement.
     quiz_posted_at is what the per-day attribution counts, so writing it
     separately would leave a window where a crash lets an extra card out. */
  await q(
    `UPDATE quiz_questions SET used_as_quiz_card = true, quiz_posted_at = now() WHERE id = $1`,
    [row.id],
  );

  console.log(`[quiz-card] POSTED category=${category} account=${pool.handle} question=${row.id} job=${jobId} elapsed_s=${((Date.now() - startedAt) / 1000).toFixed(1)}`);
}

if (require.main === module) {
  main()
    .then(async () => { await endIngestPool(); process.exit(0); })
    .catch(async (e) => {
      console.error('[quiz-card] FAILED:', e instanceof Error ? e.message : e);
      await endIngestPool();
      process.exit(1);
    });
}
