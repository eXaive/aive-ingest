/**
 * MCP Registry Ingest Worker
 *
 * Pages GET https://registry.modelcontextprotocol.io/v0.1/servers — FULL scan
 * every run (2026-07-23: the old ?updated_since incremental mode meant servers
 * with no registry update were never returned, so their last_seen never
 * advanced; a full scan makes last_seen a true "still listed" liveness marker
 * for every server, every run). Snapshots remain write-on-change via the
 * definition_hash comparison. Each run writes one scan_runs row
 * (started_at / finished_at / pages_fetched / servers_returned / status).
 *
 * Actual API shape (confirmed from live registry 2026-06-15):
 *   servers[i] = {
 *     server: { name, title, description, version, packages?, remotes?, ... },
 *     _meta:  { "io.modelcontextprotocol.registry/official": { status, updatedAt, isLatest, ... } }
 *   }
 *
 * Batched write strategy (avoids per-row round-trips):
 *   1. Filter to isLatest === true, with semver fallback for servers that have no true entry.
 *   2. For each chunk of CHUNK_SIZE items:
 *      a. Pre-fetch existing (name, id, definition_hash) in one IN query.
 *      b. Bulk upsert the chunk (onConflict: 'name') — first_seen untouched via DB default.
 *      c. Bulk insert snapshots only for new/changed hashes.
 *   Result: ~3 × ceil(N / CHUNK_SIZE) DB calls instead of 3 × N.
 *
 * Run standalone: npx tsx --env-file=.env.local workers/ingest-mcp-registry.ts
 */

import { createHash }   from 'crypto';
// Direct Postgres via the scoped aive_ingest role — no supabase-js, no
// service key. logIngestionPg is the ingest-local ingestion_log writer; the
// shared workers/logIngestion.ts (service-key client) is untouched for its
// private-repo callers. See lib/ingest/db.ts.
import { q, upsertRows, insertRows, endIngestPool, logIngestionPg } from '../lib/ingest/db';
import { hashCanonical } from '../lib/mcp/canonicalize';
import { parseLimit } from "../lib/ingest/parseLimit";
import {
  buildRegistryOutcome,
  classifyCacheRefreshError,
  exitCodeForRegistryOutcome,
  type CacheRefreshHealth,
  type CacheRefreshErrorClass,
  type CacheRefreshStatus,
  type RegistryOutcome,
} from '../lib/ingest/mcpRegistryOutcome';

const REGISTRY_BASE = 'https://registry.modelcontextprotocol.io/v0.1';
const PAGE_LIMIT    = 100;
const CHUNK_SIZE    = 150;   // caps the step-4a IN-list URL (~6.6 KB at 150 × 40-char names, well under PostgREST's ~16 KB limit)
const SOURCE_SLUG   = 'mcp-registry';

// GHA pipes stdout, so Node's async pipe writes can sit in the buffer until
// exit — run #6 produced zero live output for 30m then got killed by
// timeout-minutes with nothing to diagnose. Force blocking (synchronous)
// writes so every progress line streams immediately.
(process.stdout as unknown as { _handle?: { setBlocking?: (b: boolean) => void } })._handle?.setBlocking?.(true);
(process.stderr as unknown as { _handle?: { setBlocking?: (b: boolean) => void } })._handle?.setBlocking?.(true);

// Cumulative time attribution + page counts, shared across the fetch helpers
// as out-params so a mid-pagination throw still leaves counts readable.
export interface ScanProgress {
  pages:          number; // completed pages
  pagesAttempted: number; // requests issued (completed + the page in flight/failed)
  retries:        number; // total retries (429 + 5xx)
  retries429:     number; // rate-limit retries
  retries5xx:     number; // upstream-error retries
  retriesTransport: number; // timeout / socket / DNS retries (no HTTP status)
  fetchMs:        number; // cumulative ms inside fetch() calls
  sleepMs:        number; // cumulative ms in inter-page pacing sleep
  backoffMs:      number; // cumulative ms in retry backoff sleep
  lastCursor:     string | null; // cursor for the page that failed — the resume point
}

// (supabase-js client removed — all DB access goes through lib/ingest/db.ts)

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

interface MCPRegistryItem {
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
interface ServerRow {
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

// ── rate-limit handling (added 2026-08-02 after scheduled run #4) ───────────
// That run walked 587 pages in 16s: the registry degraded to HTTP 200s with
// EMPTY server arrays but valid cursors while rate-limited, then finally
// hard-429'd — so the worker hammered a wall for the whole scan. Two changes:
//   1. pagination HALTS on the first 429 — the failed page is retried with
//      exponential backoff (5s/15s/45s, max 3 attempts, Retry-After honoured
//      over the schedule when present), and if retries are exhausted the run
//      throws (red run stays the alert) with page counts recorded so a
//      partial scan is distinguishable from a total failure;
//   2. a 250ms inter-page delay lowers the request rate (~12.5 req/s → 4/s
//      with observed ~80ms/page) to avoid tripping the limit at all. At the
//      observed 647-page full scan this adds ~162s: roughly 1 min → ~4 min.

// ── 5xx retry (added 2026-09-02 after the 09-02 scheduled run) ─────────────
// That run walked 45 of ~890 pages and then took a single HTTP 500 from the
// registry ("Failed to get registry list"). The ladder above only recognised
// 429, so `res.status !== 429` returned the 500 straight to the caller, the
// !ok check threw, and 4,500 already-fetched servers were discarded. The
// registry answered 200 on five consecutive probes hours later: it was a
// transient upstream blip, and one retry would have absorbed it.
//
// 5xx and 429 share the retry ladder because they call for the same response
// -- wait, ask again for the SAME page. They are counted separately so the
// scan_runs status can say which one happened; "recovered after 2 retries" is
// a different operational story depending on whether the registry was rate
// limiting us or falling over.
//
// 4xx OTHER THAN 429 IS NOT RETRIED, deliberately. A 400 or 404 is a bad
// request -- a malformed cursor, a dropped parameter -- and asking again with
// the identical URL cannot fix it. Retrying those would turn a fast, loud,
// correct failure into three slow ones.

const INTER_PAGE_DELAY_MS = 250;
const RETRY_BACKOFF_MS = [5_000, 15_000, 45_000]; // max 3 retry attempts
/* Per-request ceiling. Run #42 breached it when the registry degraded to
   ~8.9s/page; it is deliberately NOT raised here, because a longer timeout
   only delays the same failure. What changed is that breaching it is now
   retryable and no longer discards the scan. */
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
// The 09-02 pass taught the ladder to retry 5xx. Run #42 then failed on
// something the ladder never saw: the registry slowed from ~1.5s to ~8.9s per
// page, one request passed the 30s AbortSignal, and fetch() THREW. A throw
// carries no HTTP status, so isRetryableStatus was never consulted — zero
// retries were attempted (backoff_ms=0) — and the throw unwound out of
// fetchAllItems, destroying its accumulator. 342 pages and ~34,200 servers
// were reported as `fetched: 0`.
//
// A timeout, a socket reset and a DNS blip call for exactly the response a
// 5xx does: wait, ask for the SAME page again. So they now share the ladder.
// The difference is only in what comes back — there is no Response to hand to
// the caller — so exhaustion throws TransportFailure, which fetchAllItems
// catches inside its loop and turns into a PARTIAL.

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
 * preferring Retry-After when the registry sends it.
 *
 * Returns the final Response — a still-failing response after exhaustion is
 * returned to the caller, whose !ok check ends pagination. Exhausting the
 * ladder on transport errors THROWS TransportFailure instead, because there is
 * no Response to return; the caller treats both the same way.
 */
export async function fetchPageWithRetry(
  url: string,
  progress: ScanProgress,
  /* Overridable for tests only. A real abort takes 30s to reproduce, which
     makes the transport path effectively untestable at the default; the
     verify script drives it at a few hundred ms. A parameter rather than an
     env var, for the same reason baseUrl is: an env hook would be a live
     production switch that exists only for tests. */
  timeoutMs: number = FETCH_TIMEOUT_MS,
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
      // Exhausted: no Response exists, so this cannot be returned. Throw, and
      // let fetchAllItems keep the pages it already has.
      if (attempt >= RETRY_BACKOFF_MS.length) {
        throw new TransportFailure(detail);
      }
      // Retry-After is unavailable here — there are no headers to read.
      const waitMs = RETRY_BACKOFF_MS[attempt];
      progress.retries++;
      progress.retriesTransport++;
      console.log(
        `[ingest-mcp-registry] transport page=${progress.pagesAttempted} attempt=${attempt + 1}/${RETRY_BACKOFF_MS.length} ` +
        `backoff_ms=${waitMs} (${detail}) — halting pagination; retrying this page`,
      );
      const tBackoff = Date.now();
      await sleep(waitMs);
      progress.backoffMs += Date.now() - tBackoff;
      continue;
    }
    progress.fetchMs += Date.now() - tFetch;
    if (!isRetryableStatus(res.status)) return res;
    if (attempt >= RETRY_BACKOFF_MS.length) return res; // exhausted → caller halts
    const hinted = retryAfterMs(res);
    const waitMs = hinted ?? RETRY_BACKOFF_MS[attempt];
    progress.retries++;
    if (res.status === 429) progress.retries429++; else progress.retries5xx++;
    console.log(
      `[ingest-mcp-registry] ${res.status} page=${progress.pagesAttempted} attempt=${attempt + 1}/${RETRY_BACKOFF_MS.length} backoff_ms=${waitMs} ` +
      `(${hinted !== null ? 'Retry-After honoured' : 'backoff schedule'}) — halting pagination; retrying this page`,
    );
    await res.text().catch(() => ''); // drain before waiting
    const tBackoff = Date.now();
    await sleep(waitMs);
    progress.backoffMs += Date.now() - tBackoff;
  }
}

// progress fields are out-params so a mid-pagination throw still leaves the
// counts available for the scan_runs row: pages = completed, pagesAttempted =
// requests issued (completed + the page that failed), retries = 429 retries.
//   include_deleted=true — surface deleted-status servers (the default scan
//     hides them), so deletion transitions become observable.
//   updatedSince — when set (delta mode), only servers changed since that
//     timestamp are returned; null (full mode) sweeps everything.
/**
 * Result of a sweep. `complete` false means pagination stopped early and
 * `items` holds everything fetched up to that point — NOT an empty result.
 * The caller decides what to do with a partial corpus; it is never discarded
 * here.
 */
export interface FetchOutcome {
  items: MCPRegistryItem[];
  complete: boolean;
  failure: string | null;
  /** Cursor of the page that failed; feed back via MCP_RESUME_CURSOR to continue. */
  resumeCursor: string | null;
}

export async function fetchAllItems(
  progress: ScanProgress,
  updatedSince: string | null,
  startCursor: string | null,
  /* Overridable so scripts/verify-registry-retry.ts can point the real
     pagination loop at a loopback server and prove the retry and partial-scan
     behaviour end to end. A parameter rather than an env var deliberately:
     an env hook would be a live production switch that exists only for tests,
     and pointing the daily ingest at an arbitrary host is not a capability
     worth shipping to save a line here. */
  baseUrl: string = REGISTRY_BASE,
  /* Test seam, same rationale as baseUrl — see fetchPageWithRetry. */
  timeoutMs: number = FETCH_TIMEOUT_MS,
): Promise<FetchOutcome> {
  const all: MCPRegistryItem[] = [];
  let cursor: string | null = startCursor;
  const t0 = Date.now();

  do {
    const url = new URL(`${baseUrl}/servers`);
    url.searchParams.set('limit', String(PAGE_LIMIT));
    url.searchParams.set('include_deleted', 'true');
    if (updatedSince) url.searchParams.set('updated_since', updatedSince);
    if (cursor)       url.searchParams.set('cursor', cursor);

    progress.pagesAttempted++;
    progress.lastCursor = cursor;

    /* THE CATCH IS INSIDE THE LOOP, and that placement is the whole fix.
       `all` is declared outside it, so returning from here keeps every page
       fetched so far. Letting the throw escape the function instead is what
       turned run #42's 342 pages into `fetched: 0` — the accumulator was a
       local that died with the stack frame. Retries are already exhausted by
       the time TransportFailure is raised. */
    let res: Response;
    try {
      res = await fetchPageWithRetry(url.toString(), progress, timeoutMs);
    } catch (err) {
      return {
        items: all,
        complete: false,
        failure: err instanceof TransportFailure
          ? `Transport failure after ${RETRY_BACKOFF_MS.length} retries: ${err.message}`
          : describeTransportError(err),
        resumeCursor: cursor,
      };
    }

    /* Retries are exhausted. Pagination halts here — but everything already
       fetched is HANDED BACK, not thrown away. Before 2026-09-02 this threw,
       and the catch turned 45 pages of real data into `fetched: 0`. */
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      return {
        items: all,
        complete: false,
        failure: `Registry ${res.status}: ${body}`,
        resumeCursor: cursor,
      };
    }

    /* A malformed body is the same class of loss: json() throws, and without
       this the accumulator would die exactly as it did on run #42. Not
       retried — a 200 that will not parse is unlikely to parse next time. */
    let page: RegistryPage;
    try {
      page = await res.json() as RegistryPage;
    } catch (err) {
      return {
        items: all,
        complete: false,
        failure: `Registry 200 with unreadable body: ${describeTransportError(err)}`,
        resumeCursor: cursor,
      };
    }

    progress.pages++;
    all.push(...(page.servers ?? []));
    cursor = page.metadata?.nextCursor ?? null;

    if (progress.pages % 10 === 0) {
      console.log(
        `[ingest-mcp-registry] progress page=${progress.pages} servers=${all.length} ` +
        `elapsed_s=${((Date.now() - t0) / 1000).toFixed(1)} ` +
        `fetch_ms=${progress.fetchMs} sleep_ms=${progress.sleepMs} backoff_ms=${progress.backoffMs} ` +
        `cursor=${(cursor ?? '(end)').slice(0, 16)}`,
      );
    }

    if (cursor) {
      const tSleep = Date.now();
      await sleep(INTER_PAGE_DELAY_MS);
      progress.sleepMs += Date.now() - tSleep;
    }
  } while (cursor);

  return { items: all, complete: true, failure: null, resumeCursor: null };
}

/** Delta anchor: the newest snapshot captured_at from the last successful run.
 *  null when the table is empty (first run → full sweep regardless of mode). */
async function lastSuccessfulCapturedAt(): Promise<string | null> {
  const rows = await q<{ captured_at: string }>(
    'SELECT captured_at FROM mcp_server_snapshots ORDER BY captured_at DESC LIMIT 1',
  );
  // node-pg returns timestamptz as a JS Date; String(Date) is NOT RFC3339 and
  // the registry 400s it ("Invalid updated_since format"). Normalize through
  // toISOString() — latent since the supabase-js → pg migration (2026-08-01),
  // invisible in CI because the workflow runs full mode (updated_since unused);
  // found 2026-08-02 by the 429-handling pass's delta-mode test.
  const v = rows[0]?.captured_at;
  return v == null ? null : new Date(v).toISOString();
}

// ── scan_runs bookkeeping ────────────────────────────────────────────────────
// One row per ingest run. Failures here are logged but never abort the scan —
// bookkeeping must not take down ingestion.

async function openScanRun(startedAt: Date): Promise<string | null> {
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
}

async function closeScanRun(
  id: string | null,
  fields: { pages_fetched: number; servers_returned: number | null; status: string },
): Promise<void> {
  if (!id) return;
  try {
    await q(
      'UPDATE scan_runs SET finished_at = now(), pages_fetched = $2, servers_returned = $3, status = $4 WHERE id = $1',
      [id, fields.pages_fetched, fields.servers_returned, fields.status],
    );
  } catch (err) {
    console.error('[ingest-mcp-registry] scan_runs update failed:', err instanceof Error ? err.message : err);
  }
}

// ── Main export ──────────────────────────────────────────────────────────────

export async function ingestMCPRegistry(
  opts: { mode?: 'full' | 'delta' } = {},
): Promise<{
  fetched: number; filtered: number; pages: number; upserted: number; snapshots: number;
  mode: 'full' | 'delta'; updated_since: string | null;
  ingest_elapsed_ms: number; total_elapsed_ms: number;
  /** Where a partial sweep stopped; null when the sweep completed. */
  resume_cursor: string | null;
} & RegistryOutcome> {
  const mode = opts.mode ?? 'full';
  const startedAt = new Date();
  const scanRunId = await openScanRun(startedAt);
  const progress: ScanProgress = {
    pages: 0, pagesAttempted: 0, retries: 0, retries429: 0, retries5xx: 0, retriesTransport: 0,
    fetchMs: 0, sleepMs: 0, backoffMs: 0, lastCursor: null,
  };
  let upserted = 0, snapshots = 0, errors = 0;
  let partialVersionSkips = 0; // rows held back by the partial-scan version guard
  let first_error: string | null = null;

  /* Manual continuation of a partial sweep: set MCP_RESUME_CURSOR to the
     resume_cursor a partial run reported. Deliberately NOT automatic — a
     resumed run covers only the tail of the corpus, so refreshing last_seen
     for that slice alone is a decision an operator makes knowingly, not a
     default that quietly replaces the daily census. */
  const resumeCursor = process.env.MCP_RESUME_CURSOR?.trim() || null;
  if (resumeCursor) {
    console.log(`[ingest-mcp-registry] resuming from cursor=${resumeCursor} — this sweep covers only the remainder of the corpus`);
  }

  // Delta mode polls only servers changed since the last successful snapshot;
  // full mode (default; the workflow entrypoint) sweeps everything so last_seen
  // stays a true liveness marker for every listed server. First run with an
  // empty table always sweeps full.
  const updatedSince = mode === 'delta' ? await lastSuccessfulCapturedAt() : null;

  console.log(
    `[ingest-mcp-registry] start mode=${mode}` +
    (mode === 'delta' ? ` updated_since=${updatedSince ?? '(none — empty table, full sweep)'}` : ''),
  );

  function recordError(label: string, e: unknown) {
    const formatted = e && typeof e === 'object' && 'message' in e
      ? fmtErr(e as { message?: string; code?: string; details?: string; hint?: string })
      : String(e);
    if (!first_error) {
      first_error = `${label}: ${formatted}`;
      console.error(`[ingest-mcp-registry] FIRST ERROR — ${label}:`, formatted);
    }
    errors++;
  }

  // ── 1. Fetch ────────────────────────────────────────────────────────────────
  let fetchOutcome: FetchOutcome;
  try {
    fetchOutcome = await fetchAllItems(progress, updatedSince, resumeCursor);
  } catch (err: unknown) {
    /* LAST RESORT ONLY, and it should now be unreachable. Every failure this
       used to swallow — the 30s abort, DNS, socket resets, a malformed body —
       is caught inside fetchAllItems' loop and returned as a PARTIAL with the
       pages intact. This comment previously said such a throw "stays a total
       failure" and named the AbortSignal as a known gap; run #42 then hit that
       exact gap and discarded 342 pages. Anything still landing here is a
       fault nothing anticipated, so it fails whole and loud rather than
       reporting a partial it cannot vouch for. */
    fetchOutcome = {
      items: [], complete: false,
      failure: err instanceof Error ? err.message : String(err),
      resumeCursor: progress.lastCursor,
    };
  }

  const allItems = fetchOutcome.items;
  /* PARTIAL: pagination stopped early but there is real data to commit.
     TOTAL FAILURE: it stopped early with nothing. Only the second discards. */
  const partial = !fetchOutcome.complete && allItems.length > 0;

  if (!fetchOutcome.complete) {
    console.error('[ingest-mcp-registry] Fetch failed:', fetchOutcome.failure);
  }

  if (!fetchOutcome.complete && allItems.length === 0) {
    const msg = fetchOutcome.failure ?? 'fetch failed without recorded detail';
    const kind = msg.startsWith('Registry 429') ? 'error rate_limited (retries exhausted)' : 'error';
    const scanStatus =
      `${kind} pages_completed=${progress.pages} pages_attempted=${progress.pagesAttempted}` +
      (progress.retries > 0 ? ` retries=${progress.retries} (429=${progress.retries429} 5xx=${progress.retries5xx} transport=${progress.retriesTransport})` : '') +
      ' — total failure';
    await closeScanRun(scanRunId, { pages_fetched: progress.pages, servers_returned: null, status: scanStatus });
    await logIngestionPg({ sourceSlug: SOURCE_SLUG, startedAt, errorMessage: msg });
    console.log(
      `[ingest-mcp-registry] final status=error pages=${progress.pages} servers=0 ` +
      `elapsed_ms=${Date.now() - startedAt.getTime()} ` +
      `fetch_ms=${progress.fetchMs} sleep_ms=${progress.sleepMs} backoff_ms=${progress.backoffMs}`,
    );
    const ingestElapsedMs = Date.now() - startedAt.getTime();
    return {
      fetched: 0, filtered: 0, pages: progress.pages, upserted: 0, snapshots: 0,
      mode, updated_since: updatedSince,
      ingest_elapsed_ms: ingestElapsedMs,
      total_elapsed_ms: ingestElapsedMs,
      resume_cursor: fetchOutcome.resumeCursor,
      ...buildRegistryOutcome({
        ingestErrors: 1,
        ingestFirstError: msg,
        dashboard: {
          status: 'SKIPPED', error: null, errorClass: null,
          durationMs: 0, startedAt: null, finishedAt: null,
        },
        reachability: {
          status: 'SKIPPED', error: null, errorClass: null,
          durationMs: 0, startedAt: null, finishedAt: null,
        },
        cacheHealthAfter: null,
      }),
    };
  }

  if (partial) {
    console.warn(
      `[ingest-mcp-registry] PARTIAL SCAN — keeping ${allItems.length} servers from ${progress.pages} completed page(s) ` +
      `instead of discarding them; resume_cursor=${fetchOutcome.resumeCursor ?? '(start)'}`,
    );
    first_error = `partial scan: ${fetchOutcome.failure}`;
  }
  console.log(`[ingest-mcp-registry] Fetched ${allItems.length} items across ${progress.pages} pages (mode=${mode}${updatedSince ? ` since=${updatedSince}` : ''}, include_deleted${partial ? ', PARTIAL' : ''})`);

  // ── 2. Filter: one entry per server name, strictly isLatest === true ────────
  // Group all items by server name first so we can apply per-name selection.
  const byName = new Map<string, MCPRegistryItem[]>();
  for (const item of allItems) {
    const name = item.server.name;
    if (!name) continue;
    if (!byName.has(name)) byName.set(name, []);
    byName.get(name)!.push(item);
  }

  const latest: MCPRegistryItem[] = [];
  for (const [, items] of byName) {
    // Primary: entries explicitly flagged isLatest === true
    const flagged = items.filter(
      i => i._meta?.['io.modelcontextprotocol.registry/official']?.isLatest === true,
    );
    const pool = flagged.length > 0 ? flagged : items; // fallback: all versions for this name
    // Pick the single highest semver from the pool (proper numeric compare, not string sort)
    const best = pool.reduce((acc, cur) =>
      semverGt(cur.server.version, acc.server.version) ? cur : acc,
    );
    latest.push(best);
  }
  console.log(`[ingest-mcp-registry] After isLatest filter: ${latest.length} / ${allItems.length} (${byName.size} unique names)`);

  // ── 3. Build in-memory row objects ──────────────────────────────────────────
  // first_seen intentionally excluded — DB DEFAULT now() on insert, untouched on conflict.
  const now = new Date().toISOString();

  type RowWithHash = ServerRow & { _hash: string; _item: MCPRegistryItem };

  const rows: RowWithHash[] = [];
  for (const item of latest) {
    const s    = item.server;
    const meta = item._meta?.['io.modelcontextprotocol.registry/official'];
    const name = s.name;
    if (!name) { recordError('missing name', 'item.server.name is empty'); continue; }

    // Status hashes use canonicalize (deep key-sort) so they agree with the
    // backfill's hashes of the same content read back from jsonb.
    const statusHash    = hashCanonical(meta?.status        ?? null);
    const statusMsgHash = hashCanonical(meta?.statusMessage ?? null);

    rows.push({
      name,
      description:         s.description   ?? null,
      version:             s.version       ?? null,
      status:              meta?.status    ?? null,
      packages:            (s.packages     ?? null) as object | null,
      remotes:             (s.remotes      ?? null) as object | null,
      raw:                 item as object,
      source:              'registry',
      definition_hash:     stableHash(item),
      status_hash:         statusHash,
      status_message_hash: statusMsgHash,
      last_seen:           now,
      _hash:               stableHash(item),
      _item:               item,
    });
  }

  // ── 4. Process in chunks ────────────────────────────────────────────────────
  for (const chunk of chunks(rows, CHUNK_SIZE)) {
    const names = chunk.map(r => r.name);

    // 4a. Pre-fetch existing (name, id, all three hashes) for this chunk — 1 query
    let existingRows: { name: string; id: string; version: string | null; definition_hash: string | null; status_hash: string | null; status_message_hash: string | null }[];
    try {
      existingRows = await q(
        'SELECT name, id, version, definition_hash, status_hash, status_message_hash FROM mcp_servers WHERE name = ANY($1)',
        [names],
      );
    } catch (fetchErr) {
      recordError(`pre-fetch chunk(${chunk[0].name}…)`, fetchErr);
      continue;
    }

    const existingMap = new Map(
      existingRows.map((r) => [r.name, { id: r.id, version: r.version, hash: r.definition_hash, statusHash: r.status_hash, statusMsgHash: r.status_message_hash }])
    );

    /* 4b-pre. PARTIAL-SCAN VERSION GUARD.
       The isLatest filter picks the highest version PER NAME out of the corpus
       it was given. On a complete sweep that corpus holds every version. On a
       partial one it does not, so a name whose newest version sat on an
       unfetched page would be "upgraded" to an older version — writing a
       backwards definition_hash and a spurious snapshot recording a downgrade
       that never happened.

       So on a partial sweep, skip any row the database already holds at a
       higher semver. Ordering-independent by design: it does not assume the
       registry paginates by name, only that a version we already recorded is
       not superseded by a lower one. A complete sweep skips this entirely —
       there, a lower version IS the truth (a genuine registry rollback). */
    const writable = partial
      ? chunk.filter((row) => {
          const existing = existingMap.get(row.name);
          if (!existing?.version || !row.version) return true;
          if (!semverGt(existing.version, row.version)) return true;
          partialVersionSkips++;
          return false;
        })
      : chunk;

    if (writable.length === 0) continue;

    // 4b. Bulk upsert — 1 query; returns name+id for new and updated rows
    // Strip internal _hash/_item fields before sending to Supabase
    const upsertPayload: ServerRow[] = writable.map(({ _hash: _h, _item: _i, ...row }) => row);

    let upsertedRows: { name: string; id: string }[];
    try {
      const MCP_COLS = ['name', 'description', 'version', 'status', 'packages', 'remotes', 'raw', 'source', 'definition_hash', 'status_hash', 'status_message_hash', 'last_seen'];
      upsertedRows = await upsertRows<{ name: string; id: string }>({
        table: 'mcp_servers',
        cols: MCP_COLS,
        rows: upsertPayload.map((r) => [
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
    } catch (upsertErr) {
      recordError(`upsert chunk(${chunk[0].name}…)`, upsertErr);
      continue;
    }

    const idMap = new Map(upsertedRows.map((r) => [r.name, r.id]));

    upserted += upsertedRows.length;

    // 4c. Collect snapshots for new/changed rows — no extra DB reads.
    // Write-on-change now fires when ANY of the three hashes differs, so a
    // status/statusMessage transition (active→deprecated→deleted) is captured
    // even when version/packages/remotes (definition_hash) are unchanged — the
    // gap that silently dropped deprecation/deletion transitions before.
    const snapshotRows: object[] = [];
    for (const row of writable) {
      const existing = existingMap.get(row.name);
      const changed =
        !existing ||
        existing.hash !== row._hash ||
        existing.statusHash !== row.status_hash ||
        existing.statusMsgHash !== row.status_message_hash;
      if (!changed) continue;

      const serverId = idMap.get(row.name) ?? existing?.id;
      if (!serverId) continue;

      const meta = row._item._meta?.['io.modelcontextprotocol.registry/official'];
      snapshotRows.push({
        server_id:           serverId,
        definition_hash:     row._hash,
        status_hash:         row.status_hash,
        status_message_hash: row.status_message_hash,
        version:             row._item.server.version ?? null,
        status:              meta?.status             ?? null,
        raw:                 row._item as object,
        captured_at:         now,
      });
    }

    // 4d. Bulk insert snapshots — 1 query (skipped if none)
    if (snapshotRows.length > 0) {
      try {
        await insertRows(
          'mcp_server_snapshots',
          ['server_id', 'definition_hash', 'status_hash', 'status_message_hash', 'version', 'status', 'raw', 'captured_at'],
          (snapshotRows as any[]).map((r) => [
            r.server_id, r.definition_hash, r.status_hash, r.status_message_hash,
            r.version, r.status, JSON.stringify(r.raw), r.captured_at,
          ]),
        );
        snapshots += snapshotRows.length;
      } catch (snapErr) {
        recordError(`snapshot chunk(${chunk[0].name}…)`, snapErr);
      }
    }

    console.log(`[ingest-mcp-registry] chunk done — upserted=${upsertedRows.length} snapshots=${snapshotRows.length}`);
  }

  const ingestElapsedMs = Date.now() - startedAt.getTime();

  const retryNote = progress.retries > 0
    ? ` (recovered after ${progress.retries} ${progress.retries === 1 ? 'retry' : 'retries'}: 429=${progress.retries429} 5xx=${progress.retries5xx} transport=${progress.retriesTransport})`
    : '';
  await closeScanRun(scanRunId, {
    pages_fetched:    progress.pages,
    servers_returned: allItems.length,
    /* A partial scan is named as such so downstream can tell a census from a
       slice: last_seen is only a liveness marker for servers this run reached. */
    status:           (errors > 0 ? 'error' : partial ? 'partial' : 'success') +
                      retryNote +
                      (partial
                        ? ` — pagination stopped at page ${progress.pagesAttempted}: ${fetchOutcome.failure}` +
                          ` resume_cursor=${fetchOutcome.resumeCursor ?? '(start)'}` +
                          (partialVersionSkips > 0 ? ` version_guard_skipped=${partialVersionSkips}` : '')
                        : ''),
  });

  // ── 5. Dashboard cache refresh — POST-INGEST TRIGGER (restored 2026-08-10) ──
  // Removed 2026-08-01 because this role has no write access to
  // mcp_dashboard_cache. The gap was then filled by a pg_cron job at a FIXED
  // 05:30 UTC, which fires BEFORE this ingest whenever Actions delays it (it
  // finished 05:48 on 08-09 and 06:15 on 08-10 — both after 05:30). The surface
  // consequently served the PREVIOUS day's ingest. A later fixed time races the
  // same way, so the trigger belongs HERE, following the data, not the clock.
  //
  // trigger_mcp_dashboard_refresh() is a security-definer wrapper (migration
  // 20260810000001) that gives this role exactly one capability: request a
  // recompute. It still cannot write mcp_dashboard_cache, so the 2026-08-01
  // boundary holds — what changed is WHO MAY ASK, not who may write.
  //
  // The ingestion row is written after both independent attempts. Cache
  // failures never increment ingest_errors or rerun ingestion; any partial
  // cache outcome remains explicit and red. Missing health fails closed.
  let cacheHealthAfter: CacheRefreshHealth | null = null;

  type CacheAttempt = {
    status: CacheRefreshStatus;
    error: string | null;
    errorClass: CacheRefreshErrorClass;
    durationMs: number;
    startedAt: string | null;
    finishedAt: string | null;
  };

  const skippedAttempt = (): CacheAttempt => ({
    status: 'SKIPPED', error: null, errorClass: null,
    durationMs: 0, startedAt: null, finishedAt: null,
  });
  let dashboardAttempt = skippedAttempt();
  let reachabilityAttempt = skippedAttempt();

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

  if (errors === 0 && !partial) {
    dashboardAttempt = await runCacheRefresh(
      'dashboard', 'SELECT public.trigger_mcp_dashboard_refresh() AS computed_at',
    );
    reachabilityAttempt = await runCacheRefresh(
      'reachability', 'SELECT public.trigger_mcp_reachability_refresh() AS computed_at',
    );
    cacheHealthAfter = await readCacheHealth();
  } else if (partial) {
    /* The caches present themselves as a picture of the whole registry.
       Recomputing them from a slice would publish that slice AS the census,
       with no marker saying so. Better a cache that is visibly one day old. */
    console.log('[ingest-mcp-registry] cache refreshes skipped — partial scan; will not publish a slice as a full census');
  } else {
    console.log(`[ingest-mcp-registry] cache refreshes skipped — run had errors=${errors}, refusing to cache a bad ingest`);
  }

  const outcome = buildRegistryOutcome({
    ingestErrors: errors,
    ingestFirstError: first_error,
    dashboard: dashboardAttempt,
    reachability: reachabilityAttempt,
    cacheHealthAfter,
    ingestPartial: partial,
  });
  const totalElapsedMs = Date.now() - startedAt.getTime();

  await logIngestionPg({
    sourceSlug: SOURCE_SLUG,
    startedAt,
    itemsFetched: allItems.length,
    itemsNew: upserted,
    metadata: {
      mode, updated_since: updatedSince, filtered: latest.length, pages: progress.pages, snapshots,
      ingest_elapsed_ms: ingestElapsedMs, total_elapsed_ms: totalElapsedMs,
      retries: progress.retries, retries_429: progress.retries429, retries_5xx: progress.retries5xx,
      retries_transport: progress.retriesTransport,
      resume_cursor: fetchOutcome.resumeCursor,
      partial_version_guard_skipped: partialVersionSkips,
      ...outcome,
    },
    ...(outcome.ingest_first_error ? { errorMessage: outcome.ingest_first_error } : {}),
  });

  console.log(
    `[ingest-mcp-registry] Done — mode=${mode} fetched=${allItems.length} pages=${progress.pages} ` +
    `filtered=${latest.length} upserted=${upserted} snapshots=${snapshots} ` +
    `ingest_status=${outcome.ingest_status} dashboard_refresh_status=${outcome.dashboard_refresh_status} ` +
    `dashboard_refresh_ms=${outcome.dashboard_refresh_ms} reachability_refresh_status=${outcome.reachability_refresh_status} ` +
    `reachability_refresh_ms=${outcome.reachability_refresh_ms} overall_status=${outcome.overall_status} ` +
    `ingest_elapsed_ms=${ingestElapsedMs} total_elapsed_ms=${totalElapsedMs} ` +
    `fetch_ms=${progress.fetchMs} sleep_ms=${progress.sleepMs} backoff_ms=${progress.backoffMs} ` +
    `retries=${progress.retries} (429=${progress.retries429} 5xx=${progress.retries5xx} transport=${progress.retriesTransport})`,
  );
  return {
    fetched: allItems.length, filtered: latest.length, pages: progress.pages,
    upserted, snapshots, mode, updated_since: updatedSince,
    ingest_elapsed_ms: ingestElapsedMs, total_elapsed_ms: totalElapsedMs,
    resume_cursor: fetchOutcome.resumeCursor,
    ...outcome,
  };
}

// ── Freshness tripwire ───────────────────────────────────────────────────────
// Same pattern as the Finance-stream tripwire in run-scheduler-tick.ts: the
// newest mcp_server_snapshots.captured_at must be younger than the window
// below, else the standalone runner exits non-zero and the Actions run turns
// RED — that red run IS the alert. Snapshots are write-on-change, but the
// registry is large enough that a daily full scan writes some snapshot every
// day. MCP_FRESHNESS_HOURS overrides for testing.
//
// 48h → 26h (2026-09-02). At 48h the gate tolerated a FULLY MISSED DAY: it
// takes two consecutive failures to trip, so the first one is invisible.
// Demonstrated on the 09-02 run, which ingested nothing and still printed
// "✓ snapshot stream fresh (newest 22.4h old)". The window has to be shorter
// than two runs for one missed run to register.
//
// 26h = 24h cadence + 2h of scheduler slack. That slack is not cosmetic:
// GitHub's scheduled dispatch drifts badly on this repo — a cron of 05:00 has
// started as late as 17:08, so consecutive runs can sit ~25h apart with
// nothing wrong. 26h absorbs ordinary drift and still trips on a missed day;
// a tighter window would just relabel GitHub's queueing as a data outage.
/* Data quality, not security: a bad value here skews a freshness window, it
   does not open a gate. So this one LOGS LOUDLY and continues on the documented
   default rather than refusing — refusing would stop an ingest for a cosmetic
   misconfiguration. Number(undefined) is NaN, which every comparison treats as
   false, so the check is still necessary. */
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

/* GitHub Actions annotations. A run that exits 0 because the DATA landed can
   still have something worth seeing at the top of the run page; without this,
   moving cache failures off the exit code would make them invisible rather
   than proportionate. `::warning::` is a plain marker with no secret in it. */
function annotate(outcome: RegistryOutcome, resumeCursor: string | null): void {
  if (outcome.ingest_status === 'PARTIAL') {
    console.log(`::warning title=MCP registry partial scan::Committed what was fetched, then stopped. Resume cursor: ${resumeCursor ?? '(start)'}`);
  }
  for (const [name, status, fresh] of [
    ['dashboard', outcome.dashboard_refresh_status, outcome.dashboard_cache_fresh],
    ['reachability', outcome.reachability_refresh_status, outcome.reachability_cache_fresh],
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
      annotate(r, r.resume_cursor);
      /* Freshness runs BEFORE the exit code is chosen and throws on failure,
         so a dark stream is exit 1 regardless of how the ingest reported
         itself — a run that ingests nothing and calls it fine is exactly what
         this tripwire exists to catch. */
      await assertSnapshotFreshness();
      await endIngestPool();
      process.exit(exitCodeForRegistryOutcome(r));
    })
    .catch(async e => { console.error(e); await endIngestPool(); process.exit(1); });
}
