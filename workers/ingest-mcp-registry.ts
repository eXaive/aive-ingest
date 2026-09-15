/**
 * MCP Registry Ingest Worker
 *
 * Pages GET https://registry.modelcontextprotocol.io/v0.1/servers in a
 * CHECKPOINTED daily FULL walk (2026-09-15, revised 2026-09-16) backed by one
 * row of ingest_checkpoints (source = 'mcp-registry'):
 *
 *   backfill — the steady state, every day. Resumes from the saved cursor
 *     (null = start of corpus) and runs until a WALK_BUDGET_MS wall-clock
 *     budget or the end of the corpus, committing every SEGMENT_PAGES pages:
 *     rows first, then the cursor, then an ingestion_log row. A budget stop
 *     is a committed checkpoint and exits 0; the next run — later the same
 *     day, or tomorrow's cron — resumes from the cursor, not the first page.
 *     Reaching the end of the corpus RE-ARMS for another full walk: cursor
 *     clears to null, phase stays 'backfill', and tomorrow's run starts a
 *     fresh full pass rather than switching to reading changes only.
 *   delta — GET ?updated_since=<watermark> (the registry turns include_deleted
 *     on with updated_since; it is set explicitly too), paged and upserted
 *     idempotently, watermark advancing to run start minus DELTA_LOOKBACK_MS
 *     on completion. NOT the steady state (reverted 2026-09-16 — see below).
 *     Kept fully working as something an operator can hand-flip a checkpoint
 *     row into (to exercise the path, or if a real need for it returns), and
 *     as the target of the oversized-changeset fallback below, but nothing in
 *     normal daily operation enters it.
 *
 * WHY CHECKPOINTED (run 34832547013, 2026-09-14). The daily full walk had
 * grown to 700+ pages (~70k entries) while the registry slowed to ~2-4.5 s/
 * page. It crossed the workflow's timeout-minutes: 30 and was killed at
 * elapsed_s≈1801 on page 700. Every fetched page was held in memory until the
 * end, so the kill committed nothing — no rows, no scan_runs close, no
 * ingestion_log row — and the watchdog read 31.6h since the last run.
 *
 * WHY FULL WALKS STAY THE STEADY STATE, NOT DELTA (reverted 2026-09-16). The
 * 2026-09-15 pass auto-flipped phase to 'delta' once a backfill reached the
 * end of the corpus, reasoning that registry metadata is immutable except
 * status, so only changes need reading after one full pass. True of the
 * registry's data — but it silently changed what mcp_servers.last_seen means:
 * from "every server, confirmed listed today" (a full walk touches every row)
 * to "last appeared in a read" (a delta only touches what changed), a real
 * change load-bearing for several surfaces (mcp-trust's default sort, the
 * server profile's last-seen row, the corpus hero timestamp, probe-mcp's
 * probe-freshest-first ordering) that was never a deliberate decision. The
 * first full pass under the checkpoint design also completed in 14m42s
 * against the 30-minute job (1,059 pages) — real headroom, not a near-miss —
 * so there was no capacity reason to give up full walks yet. The corpus grew
 * ~70k to 105,853 entries in a few weeks, so that headroom WILL erode; the
 * duration warning below is the monitoring response to that, not a reason to
 * switch to delta today.
 *
 * WHAT last_seen MEANS. Unchanged: every daily full walk reads every server,
 * so last_seen is "confirmed alive as of this walk" for the whole corpus,
 * every day. Deletions are not lost either way: they arrive as a status
 * transition (include_deleted), are captured by the status_hash comparison,
 * and are counted in the run's status_transitions.
 *
 * Actual API shape (confirmed from live registry 2026-06-15):
 *   servers[i] = {
 *     server: { name, title, description, version, packages?, remotes?, ... },
 *     _meta:  { "io.modelcontextprotocol.registry/official": { status, updatedAt, isLatest, ... } }
 *   }
 *
 * Batched write strategy, per committed batch:
 *   1. One entry per name: the isLatest === true entry, with a semver fallback
 *      for names that have none in the batch.
 *   2. For each chunk of CHUNK_SIZE: pre-fetch existing rows in one query, bulk
 *      upsert (onConflict: 'name', first_seen untouched via DB default), bulk
 *      insert snapshots only for new/changed hashes.
 *
 * Run standalone: npx tsx --env-file=.env.local workers/ingest-mcp-registry.ts
 */

import { createHash }   from 'crypto';
// Direct Postgres via the scoped aive_ingest role — no supabase-js, no
// service key. logIngestionPg is the ingest-local ingestion_log writer; the
// shared workers/logIngestion.ts (service-key client) is untouched for its
// private-repo callers. See lib/ingest/db.ts.
import { q, upsertRows, insertRows, endIngestPool, logIngestionPg, type IngestionResult } from '../lib/ingest/db';
import { hashCanonical } from '../lib/mcp/canonicalize';
import { parseLimit } from "../lib/ingest/parseLimit";
import {
  buildRegistryOutcome,
  classifyCacheRefreshError,
  type CacheRefreshHealth,
  type CacheRefreshErrorClass,
  type CacheRefreshStatus,
  type RegistryOutcome,
} from '../lib/ingest/mcpRegistryOutcome';

const REGISTRY_BASE = 'https://registry.modelcontextprotocol.io/v0.1';
const PAGE_LIMIT    = 100;
const CHUNK_SIZE    = 150;   // caps the pre-fetch IN-list (~6.6 KB at 150 × 40-char names)
const SOURCE_SLUG   = 'mcp-registry';

// GHA pipes stdout, so Node's async pipe writes can sit in the buffer until
// exit — run #6 produced zero live output for 30m then got killed by
// timeout-minutes with nothing to diagnose. Force blocking (synchronous)
// writes so every progress line streams immediately.
(process.stdout as unknown as { _handle?: { setBlocking?: (b: boolean) => void } })._handle?.setBlocking?.(true);
(process.stderr as unknown as { _handle?: { setBlocking?: (b: boolean) => void } })._handle?.setBlocking?.(true);

// Cumulative time attribution + page counts, shared across the fetch helpers
// as out-params so a mid-pagination stop still leaves counts readable.
export interface ScanProgress {
  pages:          number; // completed pages
  pagesAttempted: number; // requests issued (completed + the page in flight/failed)
  retries:        number; // total retries (429 + 5xx + transport)
  retries429:     number; // rate-limit retries
  retries5xx:     number; // upstream-error retries
  retriesTransport: number; // timeout / socket / DNS retries (no HTTP status)
  fetchMs:        number; // cumulative ms inside fetch() calls
  sleepMs:        number; // cumulative ms in inter-page pacing sleep
  backoffMs:      number; // cumulative ms in retry backoff sleep
  lastCursor:     string | null; // cursor of the page in flight — the resume point if it fails
}

// ── Types ────────────────────────────────────────────────────────────────────

interface MCPServerBody {
  $schema?:     string;
  name:         string;
  title?:       string;
  description?: string;
  version?:     string;
  packages?:    unknown;
  remotes?:     unknown;
  [key: string]: unknown;
}

interface MCPRegistryMeta {
  status?:          string;
  statusMessage?:   string;
  updatedAt?:       string;
  publishedAt?:     string;
  statusChangedAt?: string;
  isLatest?:        boolean;
}

export interface MCPRegistryItem {
  server: MCPServerBody;
  _meta?: {
    'io.modelcontextprotocol.registry/official'?: MCPRegistryMeta;
    [key: string]: unknown;
  };
}

interface RegistryPage {
  servers:   MCPRegistryItem[];
  metadata?: { nextCursor?: string | null; count?: number };
}

// Row written to mcp_servers — excludes id and first_seen (set by DB defaults)
export interface ServerRow {
  name:                string;
  description:         string | null;
  version:             string | null;
  status:              string | null;
  packages:            object | null;
  remotes:             object | null;
  raw:                 object;
  source:              'registry';
  definition_hash:     string;
  status_hash:         string;
  status_message_hash: string;
  last_seen:           string;
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function stableHash(item: MCPRegistryItem): string {
  const s = item.server;
  return createHash('sha256')
    .update(JSON.stringify({ version: s.version ?? null, packages: s.packages ?? null, remotes: s.remotes ?? null }))
    .digest('hex');
}

function fmtErr(e: { message?: string; details?: string; hint?: string; code?: string } | null | undefined): string {
  if (!e) return '(null error)';
  return JSON.stringify({ message: e.message, code: e.code, details: e.details, hint: e.hint });
}

function chunks<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function parseSemver(v: string | null | undefined): [number, number, number] {
  const m = (v ?? '').replace(/^v/, '').match(/^(\d+)\.(\d+)\.(\d+)/);
  return m ? [+m[1], +m[2], +m[3]] : [0, 0, 0];
}

function semverGt(a: string | null | undefined, b: string | null | undefined): boolean {
  const [aMaj, aMin, aPat] = parseSemver(a);
  const [bMaj, bMin, bPat] = parseSemver(b);
  return aMaj !== bMaj ? aMaj > bMaj : aMin !== bMin ? aMin > bMin : aPat > bPat;
}

const officialMeta = (item: MCPRegistryItem): MCPRegistryMeta | undefined =>
  item._meta?.['io.modelcontextprotocol.registry/official'];

// ── rate-limit handling (added 2026-08-02 after scheduled run #4) ───────────
// That run walked 587 pages in 16s: the registry degraded to HTTP 200s with
// EMPTY server arrays but valid cursors while rate-limited, then finally
// hard-429'd — so the worker hammered a wall for the whole scan. Two changes:
//   1. pagination HALTS on the first 429 — the failed page is retried with
//      exponential backoff (5s/15s/45s, max 3 attempts, Retry-After honoured
//      over the schedule when present), and if retries are exhausted the walk
//      stops and checkpoints what it has;
//   2. a 250ms inter-page delay lowers the request rate (~12.5 req/s → 4/s
//      with observed ~80ms/page) to avoid tripping the limit at all.

// ── 5xx retry (added 2026-09-02 after the 09-02 scheduled run) ─────────────
// That run walked 45 of ~890 pages and then took a single HTTP 500 from the
// registry ("Failed to get registry list"). The ladder above only recognised
// 429, so the 500 went straight to the caller and 4,500 already-fetched
// servers were discarded. It was a transient upstream blip; one retry would
// have absorbed it.
//
// 5xx and 429 share the retry ladder because they call for the same response
// -- wait, ask again for the SAME page. They are counted separately so the
// scan_runs status can say which one happened.
//
// 4xx OTHER THAN 429 IS NOT RETRIED, deliberately. A 400 or 404 is a bad
// request -- a malformed cursor, a dropped parameter -- and asking again with
// the identical URL cannot fix it.

const INTER_PAGE_DELAY_MS = 250;
const RETRY_BACKOFF_MS = [5_000, 15_000, 45_000]; // max 3 retry attempts
/* Per-request ceiling. Run #42 breached it when the registry degraded to
   ~8.9s/page; it is deliberately NOT raised here, because a longer timeout
   only delays the same failure. Breaching it is retryable. */
const FETCH_TIMEOUT_MS = 30_000;

/** Worth asking again for the same page: rate limiting, or the far side faltering. */
export function isRetryableStatus(status: number): boolean {
  return status === 429 || status >= 500;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Parse Retry-After (seconds or HTTP-date) to a wait in ms; null if absent/unreadable. */
function retryAfterMs(res: Response): number | null {
  const raw = res.headers.get('retry-after');
  if (!raw) return null;
  const secs = Number(raw.trim());
  if (Number.isFinite(secs) && secs >= 0) return Math.ceil(secs * 1000);
  const at = Date.parse(raw);
  if (!Number.isNaN(at)) return Math.max(0, at - Date.now());
  return null;
}

// ── transport retry (added 2026-09-04 after run #42) ───────────────────────
// A timeout, a socket reset and a DNS blip call for exactly the response a
// 5xx does: wait, ask for the SAME page again. They share the ladder; the only
// difference is that there is no Response to hand back, so exhaustion throws
// TransportFailure, which the walk catches inside its loop and turns into a
// checkpointed stop that keeps every page already fetched.

/** Retries exhausted with no HTTP response at all. Distinct from a bad status. */
export class TransportFailure extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TransportFailure';
  }
}

/** A short, log-safe description. Never includes the URL — it carries no
 *  secret today, but this log is public and error objects are not curated. */
function describeTransportError(err: unknown): string {
  if (err instanceof Error) {
    const cause = (err as { cause?: { code?: string } }).cause;
    const code = typeof cause?.code === 'string' ? ` (${cause.code})` : '';
    return `${err.name}: ${err.message}${code}`;
  }
  return String(err);
}

/**
 * Fetch one page. On 429, 5xx, OR a transport throw: back off and retry the
 * SAME page (never the next one) up to RETRY_BACKOFF_MS.length times,
 * preferring Retry-After when the registry sends it. Every wait is logged with
 * its sleep_source (retry-after | schedule).
 *
 * A wait is NEVER shortened. When honouring it would run past `deadlineMs` (the
 * run's walk budget), the page is not retried early: the still-failing
 * response is returned — or TransportFailure thrown — so the caller commits
 * what it has and checkpoints. Asking sooner than the registry said to would
 * be pushing against a server that is already pushing back.
 *
 * Returns the final Response — a still-failing response after exhaustion is
 * returned to the caller, whose !ok check ends pagination. Exhausting the
 * ladder on transport errors THROWS TransportFailure instead, because there is
 * no Response to return; the caller treats both the same way.
 */
export async function fetchPageWithRetry(
  url: string,
  progress: ScanProgress,
  /* Overridable for tests only: a real abort takes 30s to reproduce. A
     parameter rather than an env var — an env hook would be a live production
     switch that exists only for tests. */
  timeoutMs: number = FETCH_TIMEOUT_MS,
  /* Wall-clock instant the walk must stop by. Infinity = no budget. */
  deadlineMs: number = Number.POSITIVE_INFINITY,
): Promise<Response> {
  for (let attempt = 0; ; attempt++) {
    const tFetch = Date.now();
    let res: Response;
    try {
      res = await fetch(url, {
        headers: { 'User-Agent': 'AIVE/1.0 (aive.global)', Accept: 'application/json' },
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch (err) {
      progress.fetchMs += Date.now() - tFetch;
      const detail = describeTransportError(err);
      if (attempt >= RETRY_BACKOFF_MS.length) {
        throw new TransportFailure(detail);
      }
      // Retry-After is unavailable here — there are no headers to read.
      const waitMs = RETRY_BACKOFF_MS[attempt];
      if (Date.now() + waitMs > deadlineMs) {
        console.log(
          `[ingest-mcp-registry] transport page=${progress.pagesAttempted} wait_ms=${waitMs} sleep_source=schedule ` +
          `would pass the run budget — not retrying early; checkpointing instead (${detail})`,
        );
        throw new TransportFailure(`${detail}; the next retry would pass the run budget`);
      }
      progress.retries++;
      progress.retriesTransport++;
      console.log(
        `[ingest-mcp-registry] transport page=${progress.pagesAttempted} attempt=${attempt + 1}/${RETRY_BACKOFF_MS.length} ` +
        `backoff_ms=${waitMs} sleep_source=schedule (${detail}) — halting pagination; retrying this page`,
      );
      const tBackoff = Date.now();
      await sleep(waitMs);
      progress.backoffMs += Date.now() - tBackoff;
      continue;
    }
    progress.fetchMs += Date.now() - tFetch;
    if (!isRetryableStatus(res.status)) return res;
    if (attempt >= RETRY_BACKOFF_MS.length) return res; // exhausted → caller checkpoints
    const hinted = retryAfterMs(res);
    const waitMs = hinted ?? RETRY_BACKOFF_MS[attempt];
    const sleepSource = hinted !== null ? 'retry-after' : 'schedule';
    if (Date.now() + waitMs > deadlineMs) {
      console.log(
        `[ingest-mcp-registry] ${res.status} page=${progress.pagesAttempted} wait_ms=${waitMs} sleep_source=${sleepSource} ` +
        'would pass the run budget — not retrying early; checkpointing instead',
      );
      return res;
    }
    progress.retries++;
    if (res.status === 429) progress.retries429++; else progress.retries5xx++;
    console.log(
      `[ingest-mcp-registry] ${res.status} page=${progress.pagesAttempted} attempt=${attempt + 1}/${RETRY_BACKOFF_MS.length} backoff_ms=${waitMs} ` +
      `sleep_source=${sleepSource} (${hinted !== null ? 'Retry-After honoured' : 'backoff schedule'}) — halting pagination; retrying this page`,
    );
    await res.text().catch(() => ''); // drain before waiting
    const tBackoff = Date.now();
    await sleep(waitMs);
    progress.backoffMs += Date.now() - tBackoff;
  }
}

// ── Checkpointed walk (2026-09-15) ──────────────────────────────────────────

/** One run's walk budget. Well inside timeout-minutes: 30, so the final
 *  commit, the cache refresh and the freshness tripwire always get to run. */
export const WALK_BUDGET_MS = 20 * 60_000;
/** Pages per committed batch. A hard kill between commits loses at most this much work. */
export const SEGMENT_PAGES = 50;
/** Delta watermark lookback; see the header. */
export const DELTA_LOOKBACK_MS = 5 * 60_000;
/** 60% of the job's timeout-minutes: 30 (ingest-mcp-registry.yml) — deliberately
 *  NOT WALK_BUDGET_MS, which is an internal walk ceiling, not the job's own.
 *  A run whose total duration reaches this is the early-warning signal that
 *  corpus growth is eating the margin between a normal run (~15min today) and
 *  the 30-minute job kill. Update this alongside timeout-minutes if that ever
 *  changes. */
export const DURATION_WARNING_MS = 18 * 60_000;
/** True once total run duration reaches the warning threshold. A pure predicate
 *  so the firing/non-firing boundary is testable without a real 18-minute run. */
export function durationWarningFires(totalElapsedMs: number, thresholdMs: number = DURATION_WARNING_MS): boolean {
  return totalElapsedMs >= thresholdMs;
}

export type StopReason = 'complete' | 'budget' | 'registry' | 'store';
export type FlushKind = 'segment' | 'stop' | 'complete';
export interface PendingPage { requestCursor: string | null; items: MCPRegistryItem[] }

/**
 * What to commit now, what to hold, and where a restart resumes.
 *
 * The registry pages in name order, so one server's versions can straddle a
 * page boundary. Committing the first half alone would let the isLatest filter
 * pick an older version — and, for a server not yet in the table, write it and
 * snapshot a "version bump" that never happened once the rest arrived. So the
 * last name in the batch is held back: carried into the next batch within this
 * run (segment), or left for the next run (stop). Either way the resume cursor
 * rewinds to the first page holding that name, so a restart re-reads it;
 * re-upserting that page's other rows is idempotent. A complete walk commits
 * everything.
 */
export function planFlush(
  pending: PendingPage[],
  nextCursor: string | null,
  kind: FlushKind,
): { commit: MCPRegistryItem[]; carry: PendingPage[]; resumeCursor: string | null } {
  const all = pending.flatMap((p) => p.items);
  if (kind === 'complete') return { commit: all, carry: [], resumeCursor: null };
  let last: string | undefined;
  for (let i = all.length - 1; i >= 0 && last === undefined; i--) last = all[i].server?.name || undefined;
  const first = last === undefined ? -1 : pending.findIndex((p) => p.items.some((i) => i.server?.name === last));
  const held = last === undefined ? [] : all.filter((i) => i.server?.name === last);
  // Nothing to hold back, or one name fills the whole batch: commit it all.
  if (first < 0 || held.length === all.length) return { commit: all, carry: [], resumeCursor: nextCursor };
  return {
    commit: all.filter((i) => i.server?.name !== last),
    carry: kind === 'segment' ? [{ requestCursor: pending[first].requestCursor, items: held }] : [],
    resumeCursor: pending[first].requestCursor,
  };
}

export interface WalkArgs {
  progress: ScanProgress;
  updatedSince: string | null;
  startCursor: string | null;
  baseUrl?: string;
  timeoutMs?: number;
  deadlineMs?: number;
  segmentPages?: number;
  interPageDelayMs?: number;
  /** Commit a batch. Resolve false to stop the walk (the store refused it). */
  onFlush: (items: MCPRegistryItem[], resumeCursor: string | null, kind: FlushKind) => Promise<boolean>;
}

export interface WalkResult {
  stop: StopReason;
  failure: string | null;
  /** Where a restart resumes; null when the walk completed. */
  resumeCursor: string | null;
}

/**
 * Page the registry, handing batches to onFlush as they complete.
 *
 * Nothing is thrown for a registry problem: a failing page (retries exhausted,
 * a transport failure, an unreadable body) or the budget ends the walk with a
 * final 'stop' flush of everything already fetched, and the result says why.
 * Before 2026-09-15 the whole walk lived in one in-memory array until the end,
 * which is what a job timeout destroyed.
 */
export async function walkRegistry(a: WalkArgs): Promise<WalkResult> {
  const baseUrl = a.baseUrl ?? REGISTRY_BASE;
  const timeoutMs = a.timeoutMs ?? FETCH_TIMEOUT_MS;
  const deadlineMs = a.deadlineMs ?? Number.POSITIVE_INFINITY;
  const segmentPages = a.segmentPages ?? SEGMENT_PAGES;
  const interPageDelayMs = a.interPageDelayMs ?? INTER_PAGE_DELAY_MS;
  const t0 = Date.now();
  let cursor: string | null = a.startCursor;
  let pending: PendingPage[] = [];
  let seen = 0;

  const flush = async (kind: FlushKind, next: string | null): Promise<{ ok: boolean; resume: string | null }> => {
    const plan = planFlush(pending, next, kind);
    pending = plan.carry;
    const ok = await a.onFlush(plan.commit, plan.resumeCursor, kind);
    return { ok, resume: plan.resumeCursor };
  };
  const stop = async (reason: 'budget' | 'registry', failure: string | null): Promise<WalkResult> => {
    const f = await flush('stop', cursor);
    return f.ok
      ? { stop: reason, failure, resumeCursor: f.resume }
      : { stop: 'store', failure: failure ?? 'a batch failed to commit', resumeCursor: f.resume };
  };

  for (;;) {
    if (Date.now() >= deadlineMs) return stop('budget', null);

    const url = new URL(`${baseUrl}/servers`);
    url.searchParams.set('limit', String(PAGE_LIMIT));
    url.searchParams.set('include_deleted', 'true');
    if (a.updatedSince) url.searchParams.set('updated_since', a.updatedSince);
    if (cursor)         url.searchParams.set('cursor', cursor);

    a.progress.pagesAttempted++;
    a.progress.lastCursor = cursor;

    let res: Response;
    try {
      res = await fetchPageWithRetry(url.toString(), a.progress, timeoutMs, deadlineMs);
    } catch (err) {
      return stop('registry', err instanceof TransportFailure
        ? `Transport failure after retries: ${err.message}`
        : describeTransportError(err));
    }
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      return stop('registry', `Registry ${res.status}: ${body.slice(0, 300)}`);
    }
    /* Not retried — a 200 that will not parse is unlikely to parse next time. */
    let page: RegistryPage;
    try {
      page = await res.json() as RegistryPage;
    } catch (err) {
      return stop('registry', `Registry 200 with unreadable body: ${describeTransportError(err)}`);
    }

    a.progress.pages++;
    const servers = page.servers ?? [];
    seen += servers.length;
    pending.push({ requestCursor: cursor, items: servers });
    const next = page.metadata?.nextCursor ?? null;

    if (a.progress.pages % 10 === 0) {
      console.log(
        `[ingest-mcp-registry] progress page=${a.progress.pages} servers=${seen} ` +
        `elapsed_s=${((Date.now() - t0) / 1000).toFixed(1)} ` +
        `fetch_ms=${a.progress.fetchMs} sleep_ms=${a.progress.sleepMs} backoff_ms=${a.progress.backoffMs} ` +
        (Number.isFinite(deadlineMs) ? `budget_left_s=${Math.max(0, (deadlineMs - Date.now()) / 1000).toFixed(0)} ` : '') +
        `cursor=${(next ?? '(end)').slice(0, 16)}`,
      );
    }

    if (!next) {
      const f = await flush('complete', null);
      return f.ok
        ? { stop: 'complete', failure: null, resumeCursor: null }
        : { stop: 'store', failure: 'the final batch failed to commit', resumeCursor: f.resume };
    }
    cursor = next;

    if (pending.length >= segmentPages) {
      const f = await flush('segment', cursor);
      if (!f.ok) return { stop: 'store', failure: 'a batch failed to commit', resumeCursor: f.resume };
    }

    if (interPageDelayMs > 0) {
      const tSleep = Date.now();
      await sleep(interPageDelayMs);
      a.progress.sleepMs += Date.now() - tSleep;
    }
  }
}

/**
 * Result of an in-memory sweep. `complete` false means pagination stopped
 * early and `items` holds everything fetched up to that point — NOT an empty
 * result.
 */
export interface FetchOutcome {
  items: MCPRegistryItem[];
  complete: boolean;
  failure: string | null;
  /** Where to resume the sweep. */
  resumeCursor: string | null;
}

/**
 * The pre-checkpoint contract, kept for scripts/verify-registry-retry.ts and
 * manual inspection: one unbudgeted sweep collected in memory. The daily
 * ingest does not use it. On a stop, the last server's versions are left for
 * the resume cursor (see planFlush). baseUrl and timeoutMs are test seams,
 * parameters rather than env vars for the reason given at fetchPageWithRetry.
 */
export async function fetchAllItems(
  progress: ScanProgress,
  updatedSince: string | null,
  startCursor: string | null,
  baseUrl: string = REGISTRY_BASE,
  timeoutMs: number = FETCH_TIMEOUT_MS,
): Promise<FetchOutcome> {
  const items: MCPRegistryItem[] = [];
  const r = await walkRegistry({
    progress, updatedSince, startCursor, baseUrl, timeoutMs,
    segmentPages: Number.POSITIVE_INFINITY,
    onFlush: async (batch) => { items.push(...batch); return true; },
  });
  return { items, complete: r.stop === 'complete', failure: r.failure, resumeCursor: r.resumeCursor };
}

// ── Storage contract ─────────────────────────────────────────────────────────
// Everything the run reads or writes goes through RegistryStore, so the
// checkpoint logic is exercised end to end by scripts/verify-registry-checkpoint.ts
// against an in-memory store. Production uses pgRegistryStore (aive_ingest role).

export type Phase = 'backfill' | 'delta';

export interface Checkpoint {
  source: string;
  /** backfill: cursor of the next page to read (null = the first page). delta: always null. */
  cursor: string | null;
  phase: Phase;
  /** delta: read changes since this instant. backfill: refreshed on every completed pass, minus the lookback — where a manually-entered delta would anchor. */
  updated_since_watermark: string | null;
  last_full_pass_at: string | null;
}

export interface ExistingServer {
  name: string; id: string; version: string | null; status: string | null;
  definition_hash: string | null; status_hash: string | null; status_message_hash: string | null;
}

export interface SnapshotRow {
  server_id: string; definition_hash: string; status_hash: string; status_message_hash: string;
  version: string | null; status: string | null; raw: object; captured_at: string;
}

export interface ScanRunClose { pages_fetched: number; servers_returned: number | null; status: string }

export interface RegistryStore {
  loadCheckpoint(source: string): Promise<Checkpoint | null>;
  saveCheckpoint(cp: Checkpoint): Promise<void>;
  existingServers(names: string[]): Promise<ExistingServer[]>;
  upsertServers(rows: ServerRow[]): Promise<{ name: string; id: string }[]>;
  insertSnapshots(rows: SnapshotRow[]): Promise<number>;
  /** Never throws (the ingestion_log contract). */
  logIngestion(result: IngestionResult): Promise<void>;
  /** Bookkeeping: failures are logged, never thrown. */
  openScanRun(startedAt: Date): Promise<string | null>;
  closeScanRun(id: string | null, fields: ScanRunClose): Promise<void>;
}

/* node-pg returns timestamptz as a JS Date, and String(Date) is NOT RFC3339 —
   the registry 400s it ("Invalid updated_since format"; found 2026-08-02). */
const isoOrNull = (v: unknown): string | null => (v == null ? null : new Date(v as string).toISOString());

const MCP_COLS = ['name', 'description', 'version', 'status', 'packages', 'remotes', 'raw', 'source', 'definition_hash', 'status_hash', 'status_message_hash', 'last_seen'];

export const pgRegistryStore: RegistryStore = {
  async loadCheckpoint(source) {
    const rows = await q<{ cursor: string | null; phase: string; updated_since_watermark: unknown; last_full_pass_at: unknown }>(
      'SELECT cursor, phase, updated_since_watermark, last_full_pass_at FROM ingest_checkpoints WHERE source = $1',
      [source],
    );
    const r = rows[0];
    if (!r) return null;
    if (r.phase !== 'backfill' && r.phase !== 'delta') throw new Error(`ingest_checkpoints.phase is ${JSON.stringify(r.phase)}`);
    return {
      source, cursor: r.cursor, phase: r.phase,
      updated_since_watermark: isoOrNull(r.updated_since_watermark),
      last_full_pass_at: isoOrNull(r.last_full_pass_at),
    };
  },
  async saveCheckpoint(cp) {
    await q(
      `INSERT INTO ingest_checkpoints (source, cursor, phase, updated_since_watermark, last_full_pass_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, now())
       ON CONFLICT (source) DO UPDATE SET
         cursor = EXCLUDED.cursor, phase = EXCLUDED.phase,
         updated_since_watermark = EXCLUDED.updated_since_watermark,
         last_full_pass_at = EXCLUDED.last_full_pass_at, updated_at = now()`,
      [cp.source, cp.cursor, cp.phase, cp.updated_since_watermark, cp.last_full_pass_at],
    );
  },
  existingServers(names) {
    return q<ExistingServer>(
      'SELECT name, id, version, status, definition_hash, status_hash, status_message_hash FROM mcp_servers WHERE name = ANY($1)',
      [names],
    );
  },
  upsertServers(rows) {
    return upsertRows<{ name: string; id: string }>({
      table: 'mcp_servers',
      cols: MCP_COLS,
      rows: rows.map((r) => [
        r.name, r.description, r.version, r.status,
        r.packages == null ? null : JSON.stringify(r.packages),
        r.remotes == null ? null : JSON.stringify(r.remotes),
        JSON.stringify(r.raw),
        r.source, r.definition_hash, r.status_hash, r.status_message_hash, r.last_seen,
      ]),
      conflictCols: ['name'],
      updateCols: ['description', 'version', 'status', 'packages', 'remotes', 'raw', 'source', 'definition_hash', 'status_hash', 'status_message_hash', 'last_seen'],
      returning: ['name', 'id'],
    });
  },
  insertSnapshots(rows) {
    return insertRows(
      'mcp_server_snapshots',
      ['server_id', 'definition_hash', 'status_hash', 'status_message_hash', 'version', 'status', 'raw', 'captured_at'],
      rows.map((r) => [
        r.server_id, r.definition_hash, r.status_hash, r.status_message_hash,
        r.version, r.status, JSON.stringify(r.raw), r.captured_at,
      ]),
    );
  },
  logIngestion: logIngestionPg,
  async openScanRun(startedAt) {
    try {
      const rows = await q<{ id: string }>(
        "INSERT INTO scan_runs (started_at, status) VALUES ($1, 'running') RETURNING id",
        [startedAt.toISOString()],
      );
      return rows[0]?.id ?? null;
    } catch (err) {
      console.error('[ingest-mcp-registry] scan_runs insert failed:', err instanceof Error ? err.message : err);
      return null;
    }
  },
  async closeScanRun(id, fields) {
    if (!id) return;
    try {
      await q(
        'UPDATE scan_runs SET finished_at = now(), pages_fetched = $2, servers_returned = $3, status = $4 WHERE id = $1',
        [id, fields.pages_fetched, fields.servers_returned, fields.status],
      );
    } catch (err) {
      console.error('[ingest-mcp-registry] scan_runs update failed:', err instanceof Error ? err.message : err);
    }
  },
};

// ── Commit one batch ─────────────────────────────────────────────────────────

export interface CommitResult {
  fetched: number; filtered: number; upserted: number; snapshots: number;
  errors: number; firstError: string | null; versionGuardSkips: number;
  /** "active->deleted" style counts of status changes against the stored row. */
  statusTransitions: Record<string, number>;
}

/**
 * Upsert one batch and snapshot what changed. `completeCorpus` is true only
 * when the batch holds every version of every name (a walk that began at the
 * first page and finished in one batch); any other batch is a slice.
 */
export async function commitItems(
  store: RegistryStore,
  items: MCPRegistryItem[],
  opts: { completeCorpus: boolean; now: string },
): Promise<CommitResult> {
  const out: CommitResult = {
    fetched: items.length, filtered: 0, upserted: 0, snapshots: 0,
    errors: 0, firstError: null, versionGuardSkips: 0, statusTransitions: {},
  };
  const recordError = (label: string, e: unknown) => {
    const formatted = e && typeof e === 'object' && 'message' in e
      ? fmtErr(e as { message?: string; code?: string; details?: string; hint?: string })
      : String(e);
    if (!out.firstError) {
      out.firstError = `${label}: ${formatted}`;
      console.error(`[ingest-mcp-registry] FIRST ERROR — ${label}:`, formatted);
    }
    out.errors++;
  };

  // One entry per server name: strictly isLatest === true, else highest semver.
  const byName = new Map<string, MCPRegistryItem[]>();
  for (const item of items) {
    const name = item.server?.name;
    if (!name) continue;
    if (!byName.has(name)) byName.set(name, []);
    byName.get(name)!.push(item);
  }

  type Candidate = ServerRow & { _item: MCPRegistryItem; _flagged: boolean };
  const rows: Candidate[] = [];
  for (const [name, group] of byName) {
    const flagged = group.filter((i) => officialMeta(i)?.isLatest === true);
    const pool = flagged.length > 0 ? flagged : group;
    const best = pool.reduce((acc, cur) => (semverGt(cur.server.version, acc.server.version) ? cur : acc));
    const s = best.server;
    const meta = officialMeta(best);
    rows.push({
      name,
      description:         s.description ?? null,
      version:             s.version ?? null,
      status:              meta?.status ?? null,
      packages:            (s.packages ?? null) as object | null,
      remotes:             (s.remotes ?? null) as object | null,
      raw:                 best as object,
      source:              'registry',
      definition_hash:     stableHash(best),
      // Status hashes use canonicalize (deep key-sort) so they agree with the
      // backfill's hashes of the same content read back from jsonb.
      status_hash:         hashCanonical(meta?.status ?? null),
      status_message_hash: hashCanonical(meta?.statusMessage ?? null),
      last_seen:           opts.now,
      _item:               best,
      _flagged:            flagged.length > 0,
    });
  }
  out.filtered = rows.length;

  for (const chunk of chunks(rows, CHUNK_SIZE)) {
    let existingRows: ExistingServer[];
    try {
      existingRows = await store.existingServers(chunk.map((r) => r.name));
    } catch (err) {
      recordError(`pre-fetch chunk(${chunk[0].name}…)`, err);
      continue;
    }
    const existingMap = new Map(existingRows.map((r) => [r.name, r]));

    /* VERSION GUARD for slices. A name with no isLatest entry in this batch
       falls back to its highest semver HERE — which, in a slice, may not be
       its highest overall. So a fallback pick never overwrites a higher version
       already stored. The registry's own isLatest flag is authoritative even
       when it names a lower version (a genuine rollback), and a complete corpus
       skips the guard entirely. */
    const writable = opts.completeCorpus
      ? chunk
      : chunk.filter((row) => {
          if (row._flagged) return true;
          const existing = existingMap.get(row.name);
          if (!existing?.version || !row.version) return true;
          if (!semverGt(existing.version, row.version)) return true;
          out.versionGuardSkips++;
          return false;
        });
    if (writable.length === 0) continue;

    let upsertedRows: { name: string; id: string }[];
    try {
      upsertedRows = await store.upsertServers(writable.map(({ _item: _i, _flagged: _f, ...row }) => row));
    } catch (err) {
      recordError(`upsert chunk(${chunk[0].name}…)`, err);
      continue;
    }
    const idMap = new Map(upsertedRows.map((r) => [r.name, r.id]));
    out.upserted += upsertedRows.length;

    /* Snapshot on ANY hash change — definition, status or statusMessage — so a
       status transition (active→deprecated→deleted) is captured even when the
       definition is unchanged. include_deleted surfaces deletions; the delta
       read returns them as changes. */
    const snapshotRows: SnapshotRow[] = [];
    for (const row of writable) {
      const existing = existingMap.get(row.name);
      if (existing && (existing.status ?? null) !== row.status) {
        const key = `${existing.status ?? 'none'}->${row.status ?? 'none'}`;
        out.statusTransitions[key] = (out.statusTransitions[key] ?? 0) + 1;
      }
      const changed =
        !existing ||
        existing.definition_hash !== row.definition_hash ||
        existing.status_hash !== row.status_hash ||
        existing.status_message_hash !== row.status_message_hash;
      if (!changed) continue;
      const serverId = idMap.get(row.name) ?? existing?.id;
      if (!serverId) continue;
      snapshotRows.push({
        server_id:           serverId,
        definition_hash:     row.definition_hash,
        status_hash:         row.status_hash,
        status_message_hash: row.status_message_hash,
        version:             row.version,
        status:              row.status,
        raw:                 row._item as object,
        captured_at:         opts.now,
      });
    }
    if (snapshotRows.length > 0) {
      try {
        out.snapshots += await store.insertSnapshots(snapshotRows);
      } catch (err) {
        recordError(`snapshot chunk(${chunk[0].name}…)`, err);
      }
    }
  }
  return out;
}

// ── Dashboard cache refresh — POST-INGEST TRIGGER (restored 2026-08-10) ──────
// Removed 2026-08-01 because this role has no write access to
// mcp_dashboard_cache. The gap was then filled by a pg_cron job at a FIXED
// 05:30 UTC, which fires BEFORE this ingest whenever Actions delays it, so the
// surface served the PREVIOUS day's ingest. The trigger belongs HERE,
// following the data, not the clock.
//
// trigger_mcp_dashboard_refresh() is a security-definer wrapper (migration
// 20260810000001) that gives this role exactly one capability: request a
// recompute. It still cannot write mcp_dashboard_cache, so the 2026-08-01
// boundary holds — what changed is WHO MAY ASK, not who may write.
//
// Cache failures never increment ingest_errors or rerun ingestion; any partial
// cache outcome remains explicit. Missing health fails closed.

type CacheAttempt = {
  status: CacheRefreshStatus;
  error: string | null;
  errorClass: CacheRefreshErrorClass;
  durationMs: number;
  startedAt: string | null;
  finishedAt: string | null;
};

export interface CacheRefreshBundle {
  dashboard: CacheAttempt;
  reachability: CacheAttempt;
  health: CacheRefreshHealth | null;
}

const skippedAttempt = (): CacheAttempt => ({
  status: 'SKIPPED', error: null, errorClass: null,
  durationMs: 0, startedAt: null, finishedAt: null,
});

export async function refreshMcpCaches(): Promise<CacheRefreshBundle> {
  async function readCacheHealth(): Promise<CacheRefreshHealth | null> {
    try {
      const rows = await q<{ health: CacheRefreshHealth | null }>(
        'SELECT public.mcp_dashboard_refresh_health() AS health',
      );
      return rows[0]?.health ?? null;
    } catch (error) {
      console.error('[ingest-mcp-registry] cache health read unavailable:', error instanceof Error ? error.message : error);
      return null;
    }
  }

  async function runCacheRefresh(
    component: 'dashboard' | 'reachability',
    sql: string,
  ): Promise<CacheAttempt> {
    const attempt = skippedAttempt();
    const startedMs = Date.now();
    attempt.startedAt = new Date(startedMs).toISOString();
    try {
      const refreshed = await q<{ computed_at: string | null }>(sql);
      attempt.status = 'SUCCEEDED';
      console.log(`[ingest-mcp-registry] ${component} cache refreshed at ${refreshed[0]?.computed_at ?? 'unknown'}`);
    } catch (error) {
      attempt.status = 'FAILED';
      attempt.error = error instanceof Error ? error.message : String(error);
      attempt.errorClass = classifyCacheRefreshError(error);
      console.error(
        `[ingest-mcp-registry] ${component} cache refresh FAILED class=${attempt.errorClass}: ${attempt.error} — ` +
        'registry ingestion remains committed; the other cache is evaluated independently',
      );
    } finally {
      attempt.durationMs = Date.now() - startedMs;
      attempt.finishedAt = new Date().toISOString();
    }

    if (attempt.status === 'FAILED') {
      try {
        await q(
          'SELECT public.record_mcp_cache_refresh_failure($1,$2,$3,$4,$5,$6)',
          [component, attempt.startedAt, attempt.finishedAt, attempt.durationMs,
            attempt.error, attempt.errorClass],
        );
      } catch (recordError) {
        const detail = recordError instanceof Error ? recordError.message : String(recordError);
        attempt.error = `${attempt.error ?? 'CACHE_REFRESH_FAILED_WITHOUT_RECORDED_DETAIL'}; failure status unavailable: ${detail}`;
        console.error(`[ingest-mcp-registry] ${component} failure status record unavailable: ${detail}`);
      }
    }

    return attempt;
  }

  const dashboard = await runCacheRefresh(
    'dashboard', 'SELECT public.trigger_mcp_dashboard_refresh() AS computed_at',
  );
  const reachability = await runCacheRefresh(
    'reachability', 'SELECT public.trigger_mcp_reachability_refresh() AS computed_at',
  );
  return { dashboard, reachability, health: await readCacheHealth() };
}

// ── Main export ──────────────────────────────────────────────────────────────

/** Seams for scripts/verify-registry-checkpoint.ts; production passes none. */
export interface RunOptions {
  store?: RegistryStore;
  baseUrl?: string;
  timeoutMs?: number;
  budgetMs?: number;
  segmentPages?: number;
  lookbackMs?: number;
  interPageDelayMs?: number;
  refreshCaches?: () => Promise<CacheRefreshBundle>;
  /** Test-only override for DURATION_WARNING_MS; production never sets this. */
  durationWarningThresholdMs?: number;
}

export interface RunResult extends RegistryOutcome {
  run_started_at: string;
  phase_before: Phase;
  phase_after: Phase;
  stop: StopReason;
  stop_detail: string | null;
  pages: number;
  fetched: number;
  filtered: number;
  upserted: number;
  snapshots: number;
  checkpoints: number;
  updated_since: string | null;
  cursor_after: string | null;
  watermark_after: string | null;
  last_full_pass_at: string | null;
  status_transitions: Record<string, number>;
  retries: number; retries_429: number; retries_5xx: number; retries_transport: number;
  ingest_elapsed_ms: number;
  total_elapsed_ms: number;
  duration_warning: boolean;
  duration_warning_threshold_ms: number;
}

export async function ingestMCPRegistry(opts: RunOptions = {}): Promise<RunResult> {
  const store = opts.store ?? pgRegistryStore;
  const budgetMs = opts.budgetMs ?? WALK_BUDGET_MS;
  const lookbackMs = opts.lookbackMs ?? DELTA_LOOKBACK_MS;
  const startedAt = new Date();
  const runStartMs = startedAt.getTime();
  const anchor = () => new Date(runStartMs - lookbackMs).toISOString();
  const scanRunId = await store.openScanRun(startedAt);
  const progress: ScanProgress = {
    pages: 0, pagesAttempted: 0, retries: 0, retries429: 0, retries5xx: 0, retriesTransport: 0,
    fetchMs: 0, sleepMs: 0, backoffMs: 0, lastCursor: null,
  };

  try {
    /* A missing table (migration not applied) or an unreadable row throws here,
       before anything is fetched — loud, rather than silently restarting the
       full walk every day. */
    const loaded = await store.loadCheckpoint(SOURCE_SLUG);
    let cp: Checkpoint = loaded ?? {
      source: SOURCE_SLUG, cursor: null, phase: 'backfill',
      updated_since_watermark: anchor(), last_full_pass_at: null,
    };
    if (!loaded) {
      await store.saveCheckpoint(cp);
      console.log('[ingest-mcp-registry] no checkpoint — starting the one-time backfill from the first page');
    }
    if (cp.phase === 'delta' && !cp.updated_since_watermark) {
      throw new Error('ingest_checkpoints: delta phase without a watermark');
    }

    const phaseBefore: Phase = cp.phase;
    const updatedSince = phaseBefore === 'delta' ? cp.updated_since_watermark : null;
    const startCursor = phaseBefore === 'backfill' ? cp.cursor : null;
    console.log(
      `[ingest-mcp-registry] start phase=${phaseBefore} ` +
      (phaseBefore === 'delta' ? `updated_since=${updatedSince}` : `cursor=${startCursor ?? '(first page)'}`) +
      ` budget_s=${Math.round(budgetMs / 1000)}`,
    );

    const totals = { fetched: 0, filtered: 0, upserted: 0, snapshots: 0, errors: 0, guardSkips: 0 };
    const logged = { fetched: 0, upserted: 0 };
    const transitions: Record<string, number> = {};
    let firstError: string | null = null;
    let checkpoints = 0;
    let batchStartedAt = startedAt;

    const onFlush = async (items: MCPRegistryItem[], resumeCursor: string | null, kind: FlushKind): Promise<boolean> => {
      const committed = await commitItems(store, items, {
        completeCorpus: kind === 'complete' && phaseBefore === 'backfill' && startCursor === null && checkpoints === 0,
        now: new Date().toISOString(),
      });
      totals.fetched += committed.fetched; totals.filtered += committed.filtered;
      totals.upserted += committed.upserted; totals.snapshots += committed.snapshots;
      totals.errors += committed.errors; totals.guardSkips += committed.versionGuardSkips;
      for (const [k, n] of Object.entries(committed.statusTransitions)) transitions[k] = (transitions[k] ?? 0) + n;
      if (!firstError && committed.firstError) firstError = committed.firstError;

      /* Rows failed to write: do NOT move the cursor past them. The walk stops
         here and the next run re-reads this batch. */
      if (committed.errors > 0) return false;

      if (phaseBefore === 'backfill') {
        cp = kind === 'complete'
          // Re-arm for tomorrow's full walk (2026-09-16 revert): daily FULL
          // walks are the steady state, not a one-time backfill that hands off
          // to delta. The watermark is still refreshed to this run's anchor —
          // stale by design otherwise — so a manually-flipped delta run (the
          // header's "operator override" path) starts from THIS pass.
          ? { ...cp, cursor: null, phase: 'backfill', updated_since_watermark: anchor(), last_full_pass_at: new Date().toISOString() }
          : { ...cp, cursor: resumeCursor };
        await store.saveCheckpoint(cp);
      }
      checkpoints++;
      console.log(
        `[ingest-mcp-registry] checkpoint #${checkpoints} ${kind} phase=${phaseBefore} items=${committed.fetched} ` +
        `filtered=${committed.filtered} upserted=${committed.upserted} snapshots=${committed.snapshots}` +
        // cp.phase is always 'backfill' here (only a manual/operator action ever
        // sets 'delta', and never inside this branch), so cursor=null always
        // means "next read starts at the first page" — never "end of corpus".
        (phaseBefore === 'backfill' ? ` cursor=${cp.cursor ?? '(first page)'}` : ''),
      );
      /* One ingestion_log row per committed batch, so the watchdog sees the
         run's progress as it lands. The batch that ends the run is logged once
         the run finishes, carrying the run's outcome. */
      if (kind === 'segment') {
        await store.logIngestion({
          sourceSlug: SOURCE_SLUG, startedAt: batchStartedAt,
          itemsFetched: committed.fetched, itemsNew: committed.upserted,
          metadata: {
            kind: 'checkpoint', phase: phaseBefore, checkpoint: checkpoints, scan_run_id: scanRunId,
            cursor_after: phaseBefore === 'backfill' ? cp.cursor : null,
            pages: progress.pages, snapshots: committed.snapshots,
            status_transitions: committed.statusTransitions,
          },
        });
        logged.fetched += committed.fetched;
        logged.upserted += committed.upserted;
      }
      batchStartedAt = new Date();
      return true;
    };

    const walk = await walkRegistry({
      progress, updatedSince, startCursor,
      baseUrl: opts.baseUrl, timeoutMs: opts.timeoutMs,
      deadlineMs: runStartMs + budgetMs,
      segmentPages: opts.segmentPages, interPageDelayMs: opts.interPageDelayMs,
      onFlush,
    });

    if (phaseBefore === 'delta') {
      if (walk.stop === 'complete') {
        cp = { ...cp, updated_since_watermark: anchor() };
        await store.saveCheckpoint(cp);
      } else if (walk.stop === 'budget') {
        /* A change set too large for one run. A delta cursor is only valid for
           this exact updated_since, and keeping the old watermark would re-read
           the same oversized set forever — so start a fresh backfill anchored
           at this run and let it re-establish the corpus. */
        cp = { ...cp, phase: 'backfill', cursor: null, updated_since_watermark: anchor() };
        await store.saveCheckpoint(cp);
        console.warn('[ingest-mcp-registry] delta exceeded the run budget — falling back to a fresh backfill');
      }
      // A registry or store stop keeps the watermark: the next run re-reads the
      // same change set, which the idempotent upsert makes harmless.
    }

    const storeFailed = walk.stop === 'store' || totals.errors > 0;
    const readNothing = walk.stop === 'registry' && progress.pages === 0;
    const ingestErrors = storeFailed ? Math.max(totals.errors, 1) : readNothing ? 1 : 0;
    const ingestFirstError = storeFailed
      ? firstError ?? walk.failure
      : readNothing ? `registry unavailable — no page read this run: ${walk.failure}` : null;
    if (walk.stop !== 'complete') {
      console.warn(`[ingest-mcp-registry] walk stopped: ${walk.stop}${walk.failure ? ` — ${walk.failure}` : ''}; resume at ${cp.cursor ?? '(see phase)'}`);
    }
    const ingestElapsedMs = Date.now() - runStartMs;

    let cache: CacheRefreshBundle = { dashboard: skippedAttempt(), reachability: skippedAttempt(), health: null };
    if (walk.stop === 'complete' && ingestErrors === 0) {
      cache = await (opts.refreshCaches ?? refreshMcpCaches)();
    } else if (ingestErrors === 0) {
      /* The caches present themselves as a picture of the whole registry.
         Recomputing them from a checkpointed slice would publish that slice AS
         the census. Better a cache that is visibly a day old. */
      console.log('[ingest-mcp-registry] cache refreshes skipped — this run committed a checkpoint, not a complete pass');
    } else {
      console.log(`[ingest-mcp-registry] cache refreshes skipped — run had errors=${ingestErrors}, refusing to cache a bad ingest`);
    }

    const outcome = buildRegistryOutcome({
      ingestErrors,
      ingestFirstError,
      dashboard: cache.dashboard,
      reachability: cache.reachability,
      cacheHealthAfter: cache.health,
      // A checkpoint is a slice, not a census: PARTIAL, but with no error — the
      // next run resumes it. Only a store failure or a run that read nothing is
      // an error (and so an ingestion_log failure the watchdog counts).
      ingestPartial: walk.stop !== 'complete' && ingestErrors === 0,
    });

    const retryNote = progress.retries > 0
      ? ` retries=${progress.retries} (429=${progress.retries429} 5xx=${progress.retries5xx} transport=${progress.retriesTransport})`
      : '';
    await store.closeScanRun(scanRunId, {
      pages_fetched: progress.pages,
      servers_returned: totals.fetched,
      status:
        `${ingestErrors > 0 ? 'error' : walk.stop === 'complete' ? 'success' : 'checkpoint'} ` +
        `phase=${phaseBefore}->${cp.phase} stop=${walk.stop}` + retryNote +
        (walk.failure ? ` — ${walk.failure}` : '') +
        (cp.cursor ? ` resume_cursor=${cp.cursor}` : '') +
        (totals.guardSkips > 0 ? ` version_guard_skipped=${totals.guardSkips}` : ''),
    });

    const totalElapsedMs = Date.now() - runStartMs;
    const durationWarningThresholdMs = opts.durationWarningThresholdMs ?? DURATION_WARNING_MS;
    const durationWarning = durationWarningFires(totalElapsedMs, durationWarningThresholdMs);
    if (durationWarning) {
      console.warn(
        `[ingest-mcp-registry] DURATION WARNING: run took ${(totalElapsedMs / 60_000).toFixed(1)}min, past the ` +
        `${(durationWarningThresholdMs / 60_000).toFixed(0)}min (60%) mark of the 30-minute job budget — ` +
        'corpus growth may be eating the margin before the walk budget or the job timeout',
      );
    }
    /* The run's own row is the newest (started at the final batch), so the
       watchdog's newest-first failure streak sees the run's outcome, not an
       earlier checkpoint row. Item counts cover only what checkpoint rows have
       not already reported; the run totals are in metadata. */
    await store.logIngestion({
      sourceSlug: SOURCE_SLUG,
      startedAt: batchStartedAt,
      itemsFetched: totals.fetched - logged.fetched,
      itemsNew: totals.upserted - logged.upserted,
      metadata: {
        kind: 'run', run_started_at: startedAt.toISOString(), scan_run_id: scanRunId,
        phase_before: phaseBefore, phase_after: cp.phase, stop: walk.stop, stop_detail: walk.failure,
        updated_since: updatedSince, cursor_after: cp.cursor,
        watermark_after: cp.updated_since_watermark, last_full_pass_at: cp.last_full_pass_at,
        checkpoints, pages: progress.pages, run_fetched: totals.fetched, run_upserted: totals.upserted,
        filtered: totals.filtered, snapshots: totals.snapshots, status_transitions: transitions,
        ingest_elapsed_ms: ingestElapsedMs, total_elapsed_ms: totalElapsedMs,
        duration_warning: durationWarning, duration_warning_threshold_ms: durationWarningThresholdMs,
        retries: progress.retries, retries_429: progress.retries429, retries_5xx: progress.retries5xx,
        retries_transport: progress.retriesTransport,
        partial_version_guard_skipped: totals.guardSkips,
        ...outcome,
      },
      ...(outcome.ingest_first_error ? { errorMessage: outcome.ingest_first_error } : {}),
    });

    console.log(
      `[ingest-mcp-registry] Done — phase=${phaseBefore}->${cp.phase} stop=${walk.stop} pages=${progress.pages} ` +
      `fetched=${totals.fetched} filtered=${totals.filtered} upserted=${totals.upserted} snapshots=${totals.snapshots} ` +
      `checkpoints=${checkpoints} ingest_status=${outcome.ingest_status} overall_status=${outcome.overall_status} ` +
      `ingest_elapsed_ms=${ingestElapsedMs} total_elapsed_ms=${totalElapsedMs} duration_warning=${durationWarning} ` +
      `fetch_ms=${progress.fetchMs} sleep_ms=${progress.sleepMs} backoff_ms=${progress.backoffMs}` + retryNote,
    );

    return {
      ...outcome,
      run_started_at: startedAt.toISOString(),
      phase_before: phaseBefore, phase_after: cp.phase, stop: walk.stop, stop_detail: walk.failure,
      pages: progress.pages, fetched: totals.fetched, filtered: totals.filtered,
      upserted: totals.upserted, snapshots: totals.snapshots, checkpoints,
      updated_since: updatedSince, cursor_after: cp.cursor,
      watermark_after: cp.updated_since_watermark, last_full_pass_at: cp.last_full_pass_at,
      status_transitions: transitions,
      retries: progress.retries, retries_429: progress.retries429,
      retries_5xx: progress.retries5xx, retries_transport: progress.retriesTransport,
      ingest_elapsed_ms: ingestElapsedMs, total_elapsed_ms: totalElapsedMs,
      duration_warning: durationWarning, duration_warning_threshold_ms: durationWarningThresholdMs,
    };
  } catch (err) {
    // Anything unanticipated (a checkpoint read or write failing): close the
    // bookkeeping and log the failure before failing loud.
    const msg = err instanceof Error ? err.message : String(err);
    await store.closeScanRun(scanRunId, { pages_fetched: progress.pages, servers_returned: null, status: `error — ${msg}` });
    await store.logIngestion({ sourceSlug: SOURCE_SLUG, startedAt, errorMessage: msg, metadata: { kind: 'run', stop: 'error' } });
    throw err;
  }
}

/**
 * Exit codes for a checkpointed run (2026-09-15).
 *   0  the run committed: a complete pass or delta, OR a checkpoint the next
 *      run resumes from (a budget stop, or a registry stop after at least one
 *      page). A budget stop is the design working; exiting non-zero on it would
 *      turn every backfill day red.
 *   1  nothing usable landed: a store error, or a registry that answered no
 *      page at all this run. ingestion_log carries the error, so three in a
 *      row also page through the watchdog.
 * (exitCodeForRegistryOutcome's 2 = PARTIAL described the in-memory full walk,
 *  where a stopped scan was an incomplete census. A checkpoint is not.)
 */
export function exitCodeForRun(r: Pick<RunResult, 'ingest_status'>): 0 | 1 {
  return r.ingest_status === 'FAILED' ? 1 : 0;
}

// ── Freshness tripwire ───────────────────────────────────────────────────────
// Same pattern as the Finance-stream tripwire in run-scheduler-tick.ts: the
// newest mcp_server_snapshots.captured_at must be younger than the window
// below, else the standalone runner exits non-zero and the Actions run turns
// RED — that red run IS the alert. Snapshots are write-on-change, but the
// registry changes every day, so a healthy run writes some snapshot daily.
// MCP_FRESHNESS_HOURS overrides for testing.
//
// 48h → 26h (2026-09-02). At 48h the gate tolerated a FULLY MISSED DAY: it
// takes two consecutive failures to trip, so the first one is invisible.
// 26h = 24h cadence + 2h of scheduler slack. GitHub's scheduled dispatch
// drifts badly on this repo — a cron of 05:00 has started as late as 17:08 —
// so 26h absorbs ordinary drift and still trips on a missed day.
/* Data quality, not security: a bad value here skews a freshness window, it
   does not open a gate. So this one LOGS LOUDLY and continues on the documented
   default rather than refusing. */
const MCP_FRESHNESS_PARSED = parseLimit(process.env.MCP_FRESHNESS_HOURS, 26);
if (!MCP_FRESHNESS_PARSED.ok) {
  console.error(
    [
      "================================================================",
      `  MCP_FRESHNESS_HOURS is unreadable: ${MCP_FRESHNESS_PARSED.problem}`,
      "  Falling back to 26h. Freshness reporting will not reflect the",
      "  configured value until this is corrected.",
      "================================================================",
    ].join("\n"),
  );
}
const MCP_FRESHNESS_HOURS = MCP_FRESHNESS_PARSED.value ?? 26;

export async function assertSnapshotFreshness(): Promise<void> {
  let data: { captured_at: string }[];
  try {
    data = await q<{ captured_at: string }>(
      'SELECT captured_at FROM mcp_server_snapshots ORDER BY captured_at DESC LIMIT 1',
    );
  } catch (error) {
    throw new Error(`freshness check failed: ${error instanceof Error ? error.message : error}`);
  }
  const newestMs = data[0] ? Date.parse(String(data[0].captured_at)) : 0;
  const ageHours = (Date.now() - newestMs) / 3_600_000;
  if (ageHours > MCP_FRESHNESS_HOURS) {
    throw new Error(
      `MCP REGISTRY STREAM DARK: newest snapshot captured_at is ${ageHours.toFixed(1)}h old (limit ${MCP_FRESHNESS_HOURS}h)`
    );
  }
  console.log(`[ingest-mcp-registry] ✓ snapshot stream fresh (newest ${ageHours.toFixed(1)}h old)`);
}

// ── Standalone runner (also the GHA daily-workflow entrypoint) ───────────────

/* GitHub Actions annotations. A run that exits 0 can still have something
   worth seeing at the top of the run page. `::notice::`/`::warning::` are
   plain markers with no secret in them. */
function annotate(r: RunResult): void {
  if (r.stop === 'budget') {
    console.log(`::notice title=MCP registry checkpoint::The ${r.phase_before} walk reached its budget and committed; the next run resumes from ${r.cursor_after ?? 'a fresh backfill'}.`);
  } else if (r.stop === 'registry' && r.ingest_status !== 'FAILED') {
    console.log(`::warning title=MCP registry stopped early::Committed ${r.pages} page(s) and checkpointed. ${r.stop_detail ?? ''}`);
  }
  if (r.phase_before === 'backfill' && r.stop === 'complete') {
    console.log(`::notice title=MCP registry full pass complete::Walked the whole corpus this run (${r.pages} pages); re-armed for tomorrow's full walk.`);
  }
  if (r.duration_warning) {
    console.log(
      `::warning title=MCP registry run duration::Took ${(r.total_elapsed_ms / 60_000).toFixed(1)}min, past ` +
      `${(r.duration_warning_threshold_ms / 60_000).toFixed(0)}min (60% of the 30-minute job budget). ` +
      'Corpus growth may be eating the margin before the 20-minute walk budget or the 30-minute job timeout.',
    );
  }
  for (const [name, status, fresh] of [
    ['dashboard', r.dashboard_refresh_status, r.dashboard_cache_fresh],
    ['reachability', r.reachability_refresh_status, r.reachability_cache_fresh],
  ] as const) {
    if (status === 'FAILED') {
      console.log(`::warning title=MCP ${name} cache refresh failed::Registry data committed; the ${name} cache is serving older figures. Repeated failures go red via ingest-watchdog.`);
    } else if (status === 'SUCCEEDED' && fresh === false) {
      console.log(`::warning title=MCP ${name} cache still stale::The refresh call succeeded but the cache row did not record a fresh publish.`);
    }
  }
}

if (require.main === module) {
  ingestMCPRegistry()
    .then(async r => {
      console.log('[ingest-mcp-registry] Result:', r);
      annotate(r);
      /* Freshness runs BEFORE the exit code is chosen and throws on failure,
         so a dark stream is exit 1 regardless of how the ingest reported
         itself. */
      await assertSnapshotFreshness();
      await endIngestPool();
      process.exit(exitCodeForRun(r));
    })
    .catch(async e => { console.error(e); await endIngestPool(); process.exit(1); });
}
