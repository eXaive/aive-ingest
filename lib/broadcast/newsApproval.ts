/**
 * lib/broadcast/newsApproval.ts
 *
 * Owner approval for ONE pre-rendered news video on the Daily Post path. A news topic is an ordinary
 * broadcast_topic_queue row named 'NEWS:<story_id>' (see promoRotation.ts: never in any automatic pool). It can be
 * published only when a reviewed approval file, data/news-approvals/<story_id>.json, says PUBLICATION_AUTHORIZED and
 * binds EXACTLY: the story id, the rendered MP4's SHA-256 and byte size, the caption, the target account and the
 * media URL. Approving a story never authorizes a different render or caption.
 *
 * The approval is editorial state. It never touches the MCP evidence or the story record, and it adds no schema:
 * it is a committed, reviewable file. Submission, confirmation and failure stay where they already live
 * (broadcast_publication_attempts).
 *
 * THIS REPO IS PUBLIC. An approval file holds only what will be public once posted (caption, checksum, ids, URL).
 */
import { createHash } from 'node:crypto';
import { readFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { NEWS_TOPIC_PREFIX } from './promoRotation';

export { NEWS_TOPIC_PREFIX };

export const NEWS_APPROVAL_SCHEMA = 'AIVE_NEWS_PUBLICATION_APPROVAL_V1';
export const NEWS_STORY_ID = /^aive-mcp-news-[0-9a-f]{64}$/;
export const NEWS_APPROVAL_STATES = ['REVIEW_REQUESTED', 'OWNER_REVIEWED', 'OWNER_APPROVED', 'PUBLICATION_AUTHORIZED', 'REJECTED'] as const;
export type NewsApprovalState = typeof NEWS_APPROVAL_STATES[number];
/** An authorization is short-lived: it covers one attended publication, not a standing permission. */
export const MAX_AUTHORIZATION_HOURS = 72;
export const MAX_MEDIA_BYTES = 100 * 1024 * 1024;

/** The one account this pipeline may name (broadcast_accounts.id of @mr.agent.infrastructure). */
export const NEWS_TARGET = Object.freeze({ platform: 'tiktok', account_id: '09726d10-857d-468f-8e01-b70361c17fa1', handle: 'mr.agent.infrastructure' });

export interface NewsApproval {
  schema: typeof NEWS_APPROVAL_SCHEMA;
  story_id: string;
  topic_name: string;
  render: { sha256: string; bytes: number; duration_s: number; width: number; height: number; fps: number; label: string };
  caption: string;
  target: { platform: string; account_id: string; handle: string };
  media_url: string | null;
  state: NewsApprovalState;
  reviewed_at: string | null;
  approved_at: string | null;
  authorized_at: string | null;
  authorization_expires_at: string | null;
  rejected_reason: string | null;
}

export class NewsGateError extends Error {
  constructor(public code: string, message: string) { super(`${code}: ${message}`); }
}
const fail = (code: string, message: string): never => { throw new NewsGateError(code, message); };

export const isNewsTopic = (topicName: string) => topicName.startsWith(NEWS_TOPIC_PREFIX);
export const newsTopicName = (storyId: string) => `${NEWS_TOPIC_PREFIX}${storyId}`;

const iso = (v: unknown) => typeof v === 'string' && Number.isFinite(Date.parse(v));
const isoOrNull = (v: unknown) => v === null || iso(v);

/** Strict structural validation; no partial or unknown shapes are accepted. */
export function parseNewsApproval(raw: unknown): NewsApproval {
  const a = raw as NewsApproval;
  const keys = ['schema', 'story_id', 'topic_name', 'render', 'caption', 'target', 'media_url', 'state', 'reviewed_at', 'approved_at', 'authorized_at', 'authorization_expires_at', 'rejected_reason'];
  if (!a || typeof a !== 'object' || Array.isArray(a)) fail('NEWS_APPROVAL_INVALID', 'not an object');
  const extra = Object.keys(a).filter(k => !keys.includes(k));
  if (extra.length || keys.some(k => !(k in a))) fail('NEWS_APPROVAL_INVALID', `fields must be exactly ${keys.join(',')}`);
  if (a.schema !== NEWS_APPROVAL_SCHEMA) fail('NEWS_APPROVAL_INVALID', 'schema');
  if (!NEWS_STORY_ID.test(a.story_id)) fail('NEWS_APPROVAL_INVALID', 'story_id');
  if (a.topic_name !== newsTopicName(a.story_id)) fail('NEWS_APPROVAL_INVALID', 'topic_name must be NEWS:<story_id>');
  const r = a.render;
  if (!r || !/^[0-9a-f]{64}$/.test(r.sha256) || !(r.bytes > 0 && r.bytes <= MAX_MEDIA_BYTES) || r.width !== 1080 || r.height !== 1920 || r.fps !== 30 || !(r.duration_s > 0 && r.duration_s <= 60) || typeof r.label !== 'string')
    fail('NEWS_APPROVAL_INVALID', 'render must be a 1080x1920 30 fps MP4 under 60 s with sha256 and size');
  if (typeof a.caption !== 'string' || !a.caption.trim() || a.caption.length > 2200) fail('NEWS_APPROVAL_INVALID', 'caption');
  if (!a.target || a.target.platform !== 'tiktok' || typeof a.target.account_id !== 'string' || typeof a.target.handle !== 'string') fail('NEWS_APPROVAL_INVALID', 'target');
  if (a.media_url !== null && !(typeof a.media_url === 'string' && /^https:\/\/[^\s]+$/.test(a.media_url))) fail('NEWS_APPROVAL_INVALID', 'media_url must be https or null');
  if (!NEWS_APPROVAL_STATES.includes(a.state)) fail('NEWS_APPROVAL_INVALID', 'state');
  for (const k of ['reviewed_at', 'approved_at', 'authorized_at', 'authorization_expires_at'] as const) if (!isoOrNull(a[k])) fail('NEWS_APPROVAL_INVALID', k);
  if (a.rejected_reason !== null && typeof a.rejected_reason !== 'string') fail('NEWS_APPROVAL_INVALID', 'rejected_reason');
  // Each state requires the timestamps of every step before it, so a file cannot skip review.
  const need: Record<NewsApprovalState, (keyof NewsApproval)[]> = {
    REVIEW_REQUESTED: [], OWNER_REVIEWED: ['reviewed_at'], OWNER_APPROVED: ['reviewed_at', 'approved_at'],
    PUBLICATION_AUTHORIZED: ['reviewed_at', 'approved_at', 'authorized_at', 'authorization_expires_at', 'media_url'], REJECTED: ['rejected_reason'],
  };
  for (const k of need[a.state]) if (a[k] === null) fail('NEWS_APPROVAL_INVALID', `${a.state} requires ${k}`);
  if (a.state === 'PUBLICATION_AUTHORIZED') {
    const span = (Date.parse(a.authorization_expires_at!) - Date.parse(a.authorized_at!)) / 3_600_000;
    if (!(span > 0 && span <= MAX_AUTHORIZATION_HOURS)) fail('NEWS_APPROVAL_INVALID', `authorization window must be 0-${MAX_AUTHORIZATION_HOURS} h`);
  }
  return a;
}

export function newsApprovalDir(): string {
  return process.env.NEWS_APPROVAL_DIR?.trim() || path.join(__dirname, '..', '..', 'data', 'news-approvals');
}
/** The committed approval for a NEWS topic, or a refusal. A missing file means "not approved", never "approved". */
export function loadNewsApproval(topicName: string, dir = newsApprovalDir()): NewsApproval {
  const storyId = topicName.slice(NEWS_TOPIC_PREFIX.length);
  if (!isNewsTopic(topicName) || !NEWS_STORY_ID.test(storyId)) fail('NEWS_TOPIC_INVALID', 'topic must be NEWS:<story_id>');
  const file = path.join(dir, `${storyId}.json`);
  if (!existsSync(file)) fail('NEWS_NOT_AUTHORIZED', 'no approval file for this story');
  let raw: unknown;
  try { raw = JSON.parse(readFileSync(file, 'utf8')); } catch { fail('NEWS_APPROVAL_INVALID', 'approval file is not valid JSON'); }
  return parseNewsApproval(raw);
}

export interface NewsRow { id: string; topic_name: string; status: string; caption: string | null; pregenerated_media_url: string | null }

/**
 * Every condition for publishing one news row, checked before any capacity claim or provider call.
 * Pure: the media bytes are checked separately by verifyNewsMedia.
 */
export function assertNewsPublishable(input: { approval: NewsApproval; row: NewsRow; accountId: string; forced: boolean; now: Date }): void {
  const { approval: a, row, accountId, forced, now } = input;
  if (!forced) fail('NEWS_NOT_FORCED', 'a news topic is published only by an explicit force_topic run');
  if (a.state !== 'PUBLICATION_AUTHORIZED') fail('NEWS_NOT_AUTHORIZED', `approval state is ${a.state}`);
  if (Date.parse(a.authorization_expires_at!) <= now.getTime()) fail('NEWS_APPROVAL_EXPIRED', `authorization expired at ${a.authorization_expires_at}`);
  if (row.topic_name !== a.topic_name) fail('NEWS_TOPIC_MISMATCH', 'queue row is for a different story');
  if (row.status !== 'pending') fail('NEWS_ALREADY_USED', `queue row status is ${row.status}; a news episode posts once`);
  if (!row.pregenerated_media_url) fail('NEWS_NO_MEDIA', 'news never generates from a prompt; the approved render URL is required');
  if (row.pregenerated_media_url !== a.media_url) fail('NEWS_MEDIA_URL_MISMATCH', 'queue row media URL is not the approved URL');
  if ((row.caption ?? '') !== a.caption) fail('NEWS_CAPTION_MISMATCH', 'queue row caption is not the approved caption');
  if (a.target.account_id !== accountId || a.target.account_id !== NEWS_TARGET.account_id || a.target.handle !== NEWS_TARGET.handle) fail('NEWS_ACCOUNT_MISMATCH', 'approval is not for this account');
}

/** Downloads the approved URL (HTTPS, bounded) and checks size and SHA-256 against the approval. */
export async function verifyNewsMedia(a: NewsApproval, fetchImpl: typeof fetch = fetch): Promise<void> {
  if (!a.media_url) fail('NEWS_NO_MEDIA', 'approval has no media URL');
  const res = await fetchImpl(a.media_url!, { redirect: 'error' });
  if (!res.ok || !res.body) fail('NEWS_MEDIA_UNREADABLE', `media fetch HTTP ${res.status}`);
  const hash = createHash('sha256'); let bytes = 0;
  const reader = res.body!.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    bytes += value.byteLength;
    if (bytes > MAX_MEDIA_BYTES) { await reader.cancel(); fail('NEWS_MEDIA_MISMATCH', 'media exceeds the size bound'); }
    hash.update(value);
  }
  if (bytes !== a.render.bytes) fail('NEWS_MEDIA_MISMATCH', `media is ${bytes} bytes, approved ${a.render.bytes}`);
  if (hash.digest('hex') !== a.render.sha256) fail('NEWS_MEDIA_MISMATCH', 'media SHA-256 is not the approved render');
}

/** One status line for a news episode, from the separate records that each own a step. */
export type NewsStatus = 'STORY_GENERATED' | 'RENDER_COMPLETED' | 'OWNER_REVIEWED' | 'OWNER_APPROVED' | 'PUBLICATION_AUTHORIZED'
  | 'PUBLICATION_SUBMITTED' | 'PUBLICATION_CONFIRMED' | 'PUBLICATION_FAILED' | 'REJECTED';
export function newsStatus(input: { render: { sha256: string; failures: unknown[] } | null; approval: NewsApproval | null; attemptState: string | null }): NewsStatus {
  const { render, approval, attemptState } = input;
  if (attemptState === 'confirmed_published') return 'PUBLICATION_CONFIRMED';
  if (attemptState === 'provider_failed' || attemptState === 'provider_rejected') return 'PUBLICATION_FAILED';
  if (attemptState && ['reserved', 'submitting', 'provider_accepted', 'confirmation_pending', 'outcome_unknown'].includes(attemptState)) return 'PUBLICATION_SUBMITTED';
  if (approval?.state === 'REJECTED') return 'REJECTED';
  const renderDone = !!render && render.failures.length === 0;
  // An approval counts only for the exact render it names; approving another render never carries over.
  if (renderDone && approval && approval.render.sha256 === render!.sha256) {
    if (approval.state === 'PUBLICATION_AUTHORIZED') return 'PUBLICATION_AUTHORIZED';
    if (approval.state === 'OWNER_APPROVED') return 'OWNER_APPROVED';
    if (approval.state === 'OWNER_REVIEWED') return 'OWNER_REVIEWED';
  }
  return renderDone ? 'RENDER_COMPLETED' : 'STORY_GENERATED';
}

/**
 * A failed news publication never retries on its own authorization: the owner must authorize again after the failure.
 * (claim_broadcast_publication re-reserves a provider_failed intent, so this is checked before the claim.)
 */
export function assertRetryAuthorized(a: NewsApproval, prior: { state: string; failed_at: string | null } | null): void {
  if (!prior || prior.state !== 'provider_failed') return;
  const failedAt = Date.parse(prior.failed_at ?? '');
  if (!a.authorized_at || !Number.isFinite(failedAt) || Date.parse(a.authorized_at) <= failedAt)
    fail('NEWS_REAUTHORIZE_AFTER_FAILURE', 'the previous attempt failed; a new authorization after that failure is required');
}

/**
 * Requesting review of a render. A DIFFERENT render supersedes the active approval whatever its state (even
 * PUBLICATION_AUTHORIZED): the old file is archived and the new render starts again at REVIEW_REQUESTED, so an
 * authorization never carries over to another render. The same render is only refreshed while still REVIEW_REQUESTED.
 */
export function planApprovalRequest(existing: NewsApproval | null, next: NewsApproval): { write: NewsApproval; archive: NewsApproval | null } {
  if (next.state !== 'REVIEW_REQUESTED') fail('NEWS_APPROVAL_INVALID', 'a request starts at REVIEW_REQUESTED');
  if (!existing) return { write: next, archive: null };
  if (existing.story_id !== next.story_id) fail('NEWS_APPROVAL_INVALID', 'different story');
  if (existing.render.sha256 === next.render.sha256) {
    if (existing.state !== 'REVIEW_REQUESTED') fail('NEWS_APPROVAL_EXISTS', `this exact render is already ${existing.state}`);
    return { write: next, archive: null };
  }
  return { write: next, archive: existing };
}
export const supersededFile = (dir: string, a: NewsApproval) => path.join(dir, 'superseded', `${a.story_id}.${a.render.sha256.slice(0, 12)}.json`);
