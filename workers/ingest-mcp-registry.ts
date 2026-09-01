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
interface ScanProgress {
  pages:          number; // completed pages
  pagesAttempted: number; // requests issued (completed + the page in flight/failed)
  retries:        number; // 429 retries
  fetchMs:        number; // cumulative ms inside fetch() calls
  sleepMs:        number; // cumulative ms in inter-page pacing sleep
  backoffMs:      number; // cumulative ms in 429 backoff sleep
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

const INTER_PAGE_DELAY_MS = 250;
const RETRY_BACKOFF_MS = [5_000, 15_000, 45_000]; // max 3 retry attempts

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

/**
 * Fetch one page. On 429: back off and retry the SAME page (never the next
 * one) up to RETRY_BACKOFF_MS.length times, preferring Retry-After when the
 * registry sends it. Returns the final Response — a still-429 response after
 * exhaustion is returned to the caller, whose !ok throw ends the run.
 */
async function fetchPageWithRetry(url: string, progress: ScanProgress): Promise<Response> {
  for (let attempt = 0; ; attempt++) {
    const tFetch = Date.now();
    const res = await fetch(url, {
      headers: { 'User-Agent': 'AIVE/1.0 (aive.global)', Accept: 'application/json' },
      signal: AbortSignal.timeout(30_000),
    });
    progress.fetchMs += Date.now() - tFetch;
    if (res.status !== 429) return res;
    if (attempt >= RETRY_BACKOFF_MS.length) return res; // exhausted → caller throws
    const hinted = retryAfterMs(res);
    const waitMs = hinted ?? RETRY_BACKOFF_MS[attempt];
    progress.retries++;
    console.log(
      `[ingest-mcp-registry] 429 page=${progress.pagesAttempted} attempt=${attempt + 1}/${RETRY_BACKOFF_MS.length} backoff_ms=${waitMs} ` +
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
async function fetchAllItems(
  progress: ScanProgress,
  updatedSince: string | null,
): Promise<MCPRegistryItem[]> {
  const all: MCPRegistryItem[] = [];
  let cursor: string | null = null;
  const t0 = Date.now();

  do {
    const url = new URL(`${REGISTRY_BASE}/servers`);
    url.searchParams.set('limit', String(PAGE_LIMIT));
    url.searchParams.set('include_deleted', 'true');
    if (updatedSince) url.searchParams.set('updated_since', updatedSince);
    if (cursor)       url.searchParams.set('cursor', cursor);

    progress.pagesAttempted++;
    const res = await fetchPageWithRetry(url.toString(), progress);

    // A 429 here means retries are exhausted: pagination has already halted
    // (no subsequent page was requested) and this throw ends the run red.
    if (!res.ok) throw new Error(`Registry ${res.status}: ${await res.text().catch(() => '')}`);

    const page: RegistryPage = await res.json();
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

  return all;
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
} & RegistryOutcome> {
  const mode = opts.mode ?? 'full';
  const startedAt = new Date();
  const scanRunId = await openScanRun(startedAt);
  const progress: ScanProgress = { pages: 0, pagesAttempted: 0, retries: 0, fetchMs: 0, sleepMs: 0, backoffMs: 0 };
  let upserted = 0, snapshots = 0, errors = 0;
  let first_error: string | null = null;

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
  let allItems: MCPRegistryItem[];
  try {
    allItems = await fetchAllItems(progress, updatedSince);
    console.log(`[ingest-mcp-registry] Fetched ${allItems.length} items across ${progress.pages} pages (mode=${mode}${updatedSince ? ` since=${updatedSince}` : ''}, include_deleted)`);
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error('[ingest-mcp-registry] Fetch failed:', msg);
    // Distinguish a partial scan from a total failure: pages_completed is how
    // far pagination got before the failure; pages_attempted includes the page
    // that failed. A rate-limit exhaustion is named as such.
    const kind = msg.startsWith('Registry 429') ? 'error rate_limited (retries exhausted)' : 'error';
    const scanStatus =
      `${kind} pages_completed=${progress.pages} pages_attempted=${progress.pagesAttempted}` +
      (progress.retries > 0 ? ` retries=${progress.retries}` : '') +
      (progress.pages > 0 ? ' — partial scan' : ' — total failure');
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
    let existingRows: { name: string; id: string; definition_hash: string | null; status_hash: string | null; status_message_hash: string | null }[];
    try {
      existingRows = await q(
        'SELECT name, id, definition_hash, status_hash, status_message_hash FROM mcp_servers WHERE name = ANY($1)',
        [names],
      );
    } catch (fetchErr) {
      recordError(`pre-fetch chunk(${chunk[0].name}…)`, fetchErr);
      continue;
    }

    const existingMap = new Map(
      existingRows.map((r) => [r.name, { id: r.id, hash: r.definition_hash, statusHash: r.status_hash, statusMsgHash: r.status_message_hash }])
    );

    // 4b. Bulk upsert — 1 query; returns name+id for new and updated rows
    // Strip internal _hash/_item fields before sending to Supabase
    const upsertPayload: ServerRow[] = chunk.map(({ _hash: _h, _item: _i, ...row }) => row);

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
    for (const row of chunk) {
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

  await closeScanRun(scanRunId, {
    pages_fetched:    progress.pages,
    servers_returned: allItems.length,
    status:           (errors > 0 ? 'error' : 'success') +
                      (progress.retries > 0 ? ` (recovered after ${progress.retries} rate-limit ${progress.retries === 1 ? 'retry' : 'retries'})` : ''),
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

  if (errors === 0) {
    dashboardAttempt = await runCacheRefresh(
      'dashboard', 'SELECT public.trigger_mcp_dashboard_refresh() AS computed_at',
    );
    reachabilityAttempt = await runCacheRefresh(
      'reachability', 'SELECT public.trigger_mcp_reachability_refresh() AS computed_at',
    );
    cacheHealthAfter = await readCacheHealth();
  } else {
    console.log(`[ingest-mcp-registry] cache refreshes skipped — run had errors=${errors}, refusing to cache a bad ingest`);
  }

  const outcome = buildRegistryOutcome({
    ingestErrors: errors,
    ingestFirstError: first_error,
    dashboard: dashboardAttempt,
    reachability: reachabilityAttempt,
    cacheHealthAfter,
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
    `fetch_ms=${progress.fetchMs} sleep_ms=${progress.sleepMs} backoff_ms=${progress.backoffMs}`,
  );
  return {
    fetched: allItems.length, filtered: latest.length, pages: progress.pages,
    upserted, snapshots, mode, updated_since: updatedSince,
    ingest_elapsed_ms: ingestElapsedMs, total_elapsed_ms: totalElapsedMs,
    ...outcome,
  };
}

// ── Freshness tripwire ───────────────────────────────────────────────────────
// Same pattern as the Finance-stream tripwire in run-scheduler-tick.ts: the
// newest mcp_server_snapshots.captured_at must be younger than 48h, else the
// standalone runner exits non-zero and the Actions run turns RED — that red
// run IS the alert. Snapshots are write-on-change, but the registry is large
// enough that a daily full scan writes some snapshot every day; 48h = one
// fully missed day + buffer. MCP_FRESHNESS_HOURS overrides for testing.
/* Data quality, not security: a bad value here skews a freshness window, it
   does not open a gate. So this one LOGS LOUDLY and continues on the documented
   default rather than refusing — refusing would stop an ingest for a cosmetic
   misconfiguration. Number(undefined) is NaN, which every comparison treats as
   false, so the check is still necessary. */
const MCP_FRESHNESS_PARSED = parseLimit(process.env.MCP_FRESHNESS_HOURS, 48);
if (!MCP_FRESHNESS_PARSED.ok) {
  console.error(
    [
      "================================================================",
      `  MCP_FRESHNESS_HOURS is unreadable: ${MCP_FRESHNESS_PARSED.problem}`,
      "  Falling back to 48h. Freshness reporting will not reflect the",
      "  configured value until this is corrected.",
      "================================================================",
    ].join("\n"),
  );
}
const MCP_FRESHNESS_HOURS = MCP_FRESHNESS_PARSED.value ?? 48;

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

if (require.main === module) {
  ingestMCPRegistry()
    .then(async r => {
      console.log('[ingest-mcp-registry] Result:', r);
      await assertSnapshotFreshness();
      await endIngestPool();
      process.exit(exitCodeForRegistryOutcome(r));
    })
    .catch(async e => { console.error(e); await endIngestPool(); process.exit(1); });
}
