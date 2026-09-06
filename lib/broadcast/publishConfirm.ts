/**
 * lib/broadcast/publishConfirm.ts
 *
 * Turns "Blotato accepted the post" into "the post is live".
 *
 * WHY THIS EXISTS. All three broadcast workers marked their row consumed the
 * moment /api/broadcast/dispatch returned status='posted'. That status means
 * ACCEPTED, not PUBLISHED: Blotato publishes asynchronously. Measured
 * 2026-09-06 on a real GTA card -- the dispatch returned posted at 03:03:15,
 * GET /v2/posts/:id still said "in-progress" a minute later, and only then
 * flipped to "published". For that whole window the question was already
 * marked used_as_quiz_card with nothing live behind it.
 *
 * Nothing had gone wrong yet in 3 of 3 posts. But the failure it permits is
 * the exact false-healthy shape the rest of this pipeline is built to refuse:
 * a rejected-after-acceptance post would leave a consumed row, a green run,
 * and no post -- and the row would never be retried because it reads as done.
 *
 * So: poll to a terminal state, and let the CALLER decide what to do. This
 * module never writes to the database and never marks anything.
 *
 * THIS REPO IS PUBLIC. publicUrl is a public post URL and is safe to log; the
 * API key is never logged, and errorMessage is truncated.
 */

const BLOTATO_BASE = 'https://backend.blotato.com';

/* Observed: ~1 minute from acceptance to published. 5s x 36 = 3 minutes gives
   generous headroom without letting a stuck submission hold a runner for the
   whole job timeout. */
const POLL_INTERVAL_MS = 5_000;
const POLL_MAX_ATTEMPTS = 36;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export interface PublishConfirmation {
  submissionId: string;
  publicUrl: string | null;
}

/**
 * The submission id for one account, out of a dispatch response.
 *
 * TWO PLACES, DELIBERATELY. lib/broadcast/adapter.ts searches the create
 * response for submissionId / id / post.id / data.id / schedule.id and, on a
 * miss, keeps the whole body in raw_response. Blotato actually returns
 * `postSubmissionId`, which is in none of those, so submission_id has been
 * NULL on all 19 recorded entries and the id has only ever survived inside
 * raw_response. The adapter is being fixed separately; reading both means this
 * works before that fix ships and keeps working after.
 */
export function submissionIdFor(
  dispatched: Record<string, any>,
  accountId: string,
): string | null {
  const entry = dispatched?.job?.submission_ids?.[accountId];
  if (!entry) return null;

  const direct = entry.submission_id;
  if (typeof direct === 'string' && direct.trim()) return direct.trim();

  const raw = entry.raw_response;
  if (typeof raw === 'string' && raw.includes('postSubmissionId')) {
    try {
      const parsed = JSON.parse(raw) as Record<string, unknown>;
      const id = parsed.postSubmissionId;
      if (typeof id === 'string' && id.trim()) return id.trim();
    } catch {
      /* raw_response is truncated to 400 chars by the adapter, so a long body
         may not parse. Fall through to the regex below rather than losing the
         id to a formatting detail. */
    }
    const m = /"postSubmissionId"\s*:\s*"([^"]+)"/.exec(raw);
    if (m) return m[1];
  }
  return null;
}

/** Raised when a submission reaches a terminal state that is not `published`. */
export class PublishNotConfirmed extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PublishNotConfirmed';
  }
}

/**
 * Poll GET /v2/posts/:id until the submission is published.
 *
 * Returns on `published`. THROWS on everything else, because every other
 * outcome means the caller must not record a consumption:
 *   failed     -- the post will never go live; errorMessage is carried out.
 *   scheduled  -- this pipeline never passes scheduledTime, so seeing it means
 *                 something upstream changed. Not marking risks a duplicate if
 *                 it later publishes, but marking on an unconfirmed post is the
 *                 thing this module exists to prevent; a red run puts a human
 *                 on it either way.
 *   timeout    -- unknown, and unknown is not success.
 *
 * A transient HTTP error while polling is NOT terminal: it is logged and the
 * poll continues, the same posture as the render poll in the workers.
 */
export async function confirmPublished(
  submissionId: string,
  apiKey: string,
  log: (msg: string) => void = console.log,
): Promise<PublishConfirmation> {
  let lastStatus = '(none)';

  for (let attempt = 1; attempt <= POLL_MAX_ATTEMPTS; attempt++) {
    const res = await fetch(`${BLOTATO_BASE}/v2/posts/${encodeURIComponent(submissionId)}`, {
      headers: { 'blotato-api-key': apiKey },
    });
    const body = await res.text();

    if (!res.ok) {
      log(`publish poll ${attempt}/${POLL_MAX_ATTEMPTS} — HTTP ${res.status}, retrying`);
      await sleep(POLL_INTERVAL_MS);
      continue;
    }

    let parsed: Record<string, any>;
    try { parsed = JSON.parse(body); } catch {
      log(`publish poll ${attempt}/${POLL_MAX_ATTEMPTS} — unparseable body, retrying`);
      await sleep(POLL_INTERVAL_MS);
      continue;
    }

    lastStatus = String(parsed.status ?? 'unknown');
    log(`publish poll ${attempt}/${POLL_MAX_ATTEMPTS} — status=${lastStatus}`);

    if (lastStatus === 'published') {
      const publicUrl = typeof parsed.publicUrl === 'string' ? parsed.publicUrl : null;
      return { submissionId, publicUrl };
    }
    if (lastStatus === 'failed') {
      const detail = typeof parsed.errorMessage === 'string'
        ? parsed.errorMessage.slice(0, 200) : '(no errorMessage)';
      throw new PublishNotConfirmed(`Blotato reported the post FAILED after accepting it: ${detail}`);
    }
    if (lastStatus === 'scheduled') {
      throw new PublishNotConfirmed(
        'Blotato reports the post as SCHEDULED, but this pipeline never schedules — refusing to record it as published',
      );
    }

    await sleep(POLL_INTERVAL_MS);
  }

  throw new PublishNotConfirmed(
    `post was accepted but never confirmed published within ${(POLL_MAX_ATTEMPTS * POLL_INTERVAL_MS) / 1000}s (last status ${lastStatus})`,
  );
}
