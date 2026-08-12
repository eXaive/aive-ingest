/**
 * lib/mcp/listCollector.ts -- the shared body of a listing collector.
 *
 * WHY THIS EXISTS, AND WHAT IT DOES NOT FIX.
 * collect-mcp-tools.ts restates the dispatch loop from discover-mcp-servers.ts
 * (runSweep) because runSweep's callbacks are typed to DiscoverRow. That is a
 * filed forge todo (4a33a213). resources/list and prompts/list would have made
 * it a third and FOURTH copy. They do not: both drive this one module, so the
 * count goes from two to three, not to four, and a fix here reaches both new
 * collectors at once.
 *
 * It does NOT retire the existing duplication. Rewriting collect-mcp-tools.ts to
 * sit on this module means editing a collector that is live and working, on the
 * same pass that introduces two new ones -- a bad trade. The todo is updated to
 * record that the count is now three and that this module is the shape the other
 * two should converge on, rather than a third divergent copy appearing silently.
 *
 * WHAT IS PARAMETERISED: the method name, the User-Agent, the JSON-RPC result
 * key, the row mapper, the destination tables, and the env var prefix. What is
 * NOT parameterised, because getting it wrong is how sweeps die:
 *   - release the host BEFORE anything else, so a throw cannot leak a slot
 *   - 429 penalises the HOST and requeues once, and is never retried past that
 *   - every await inside collectOnce is guarded, so no endpoint can reject
 *     Promise.all and abandon the in-flight workers (the 2026-08-11 post-mortem)
 *   - flush() runs in a finally, on every path including an abort
 *
 * NO initialize. NO session header sent or read. NO Authorization, no cookies.
 * NO resources/read, NO prompts/get -- these workers read declarations and stop
 * at the declaration. Mcp-Name is required only for tools/call, resources/read
 * and prompts/get, none of which is issued here, so it is not sent.
 */

import { randomUUID } from 'node:crypto';
import { q, ingestPool, insertRows, logIngestionPg } from '../ingest/db';
import { parseLimit } from '../ingest/parseLimit';
import { loadExclusions, isExcluded, exclusionNote, type Method } from './exclusions';
import { classifyException, classifyStatus, parseRetryAfter } from './errorClass';
// HostScheduler and Item are imported and used UNMODIFIED. Nothing in
// discover-mcp-servers.ts is changed by this module.
import { HostScheduler, type Item } from '../../workers/discover-mcp-servers';

const MCP_PROTOCOL_VERSION = '2026-07-28';
const DEFAULT_CONCURRENCY = 8;
const TIMEOUT_MS = 15_000;
const INSERT_CHUNK = 500;
/** Cursor pages per endpoint. A stop is RECORDED as truncated, never swallowed. */
const MAX_PAGES = 20;
const MAX_BODY_BYTES = 1_048_576;

/**
 * Mirrors the CHECK on the capture tables exactly.
 *
 * 'unsupported' IS NOT AN ERROR AND MUST NOT BE FOLDED INTO ONE. A server that
 * answers tools/list and returns -32601 for prompts/list is stating a fact about
 * itself: it publishes tools and not prompts. Merging that into 'error' would
 * mix it with timeouts and TLS failures and make "how many current-spec servers
 * expose prompts at all" unanswerable. It is a measurement, not a fault.
 */
export type CaptureStatus =
  | 'ok' | 'error' | 'unsupported' | 'session_required' | 'not_attempted' | 'excluded';

/** One declaration row, already mapped to its destination columns. */
export type ItemRow = Record<string, unknown>;

export interface CaptureRow {
  server_id: string;
  endpoint_url: string;
  captured_at: string;
  status: CaptureStatus;
  http_status: number | null;
  response_time_ms: number | null;
  error_class: string | null;
  item_count: number | null;
  page_count: number;
  truncated: boolean;
  raw_json: unknown;
  raw_bytes: number | null;
  token_estimate: number | null;
  cache_ttl_ms: number | null;
  protocol_version: string | null;
  list_changed: boolean | null;
  note: string | null;
  items: ItemRow[];
}

/** Everything that differs between one listing collector and another. */
export interface ListSpec {
  /** JSON-RPC method, e.g. 'resources/list'. */
  method: string;
  /** Which collector is asking, for exclusion matching. */
  exclusionMethod: Method;
  /** Byte-identical to the constant rendered on the census page. */
  userAgent: string;
  /** ingestion_log source slug. */
  sourceSlug: string;
  /** Env var prefix, e.g. 'RESOURCES' -> RESOURCES_LIMIT / _CONCURRENCY. */
  envPrefix: string;
  /** Key inside result that holds the array, e.g. 'resources'. */
  resultKey: string;
  /** Capture table name. */
  captureTable: string;
  /** Column on the capture table holding the count, e.g. 'resource_count'. */
  countColumn: string;
  /** Item table name. */
  itemTable: string;
  /** Item table columns, in the order itemToRow returns them. */
  itemColumns: string[];
  /** Map one wire object to its item-table values. Pure and total. */
  itemToRow: (o: any) => unknown[];
  /** Columns that must be JSON.stringify'd before binding. */
  jsonColumns: Set<string>;
  /** Log prefix, e.g. '[collect-mcp-resources]'. */
  tag: string;
}

/** Bounded body read. Guarded at every call site. */
async function readBounded(res: Response): Promise<string> {
  if (!res.body) return '';
  const reader = res.body.getReader();
  const decoder = new TextDecoder('utf-8');
  let out = '';
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) {
        total += value.byteLength;
        out += decoder.decode(value, { stream: true });
        if (total >= MAX_BODY_BYTES) break;
      }
    }
    out += decoder.decode();
  } finally {
    try { await reader.cancel(); } catch { /* already closed */ }
  }
  return out;
}

/** JSON or SSE, same as the discover and tools workers: Accept advertises both. */
export function parseEnvelope(body: string, contentType: string | null): any | null {
  const isSse = (contentType ?? '').toLowerCase().includes('text/event-stream');
  if (!isSse) {
    try { return JSON.parse(body); } catch { return null; }
  }
  for (const line of body.split(/\r?\n/)) {
    if (!line.startsWith('data:')) continue;
    const payload = line.slice(5).trim();
    if (!payload || payload === '[DONE]') continue;
    try {
      const parsed = JSON.parse(payload);
      if (parsed && typeof parsed === 'object') return parsed;
    } catch { /* a partial frame is not a failure */ }
  }
  return null;
}

const errorMessage = (env: any): string =>
  typeof env?.error?.message === 'string' ? env.error.message : '';

export function demandsSession(msg: string): boolean {
  return /session/i.test(msg) && /(required|missing|invalid|expired|initiali)/i.test(msg);
}

/** One constant feeds header and body, so HeaderMismatch is unrepresentable. */
function listBody(spec: ListSpec, cursor: string | null): string {
  const params: Record<string, unknown> = {
    _meta: {
      'io.modelcontextprotocol/protocolVersion': MCP_PROTOCOL_VERSION,
      'io.modelcontextprotocol/clientInfo': { name: spec.sourceSlug, version: '1.0.0' },
      'io.modelcontextprotocol/clientCapabilities': {},
    },
  };
  if (cursor !== null) params.cursor = cursor;
  return JSON.stringify({ jsonrpc: '2.0', id: 1, method: spec.method, params });
}

function listHeaders(spec: ListSpec): Record<string, string> {
  return {
    'Content-Type': 'application/json',
    Accept: 'application/json, text/event-stream',
    'MCP-Protocol-Version': MCP_PROTOCOL_VERSION,
    'Mcp-Method': spec.method,
    'User-Agent': spec.userAgent,
  };
}

/**
 * One endpoint, start to finish, including its cursor chain. NEVER THROWS.
 * Exported so the fixture harness can drive it directly with a stubbed fetch.
 */
export async function collectOnce(
  spec: ListSpec,
  item: Item,
  fetchImpl: typeof fetch = fetch,
): Promise<{ capture: CaptureRow; retryAfterMs: number | null }> {
  const capturedAt = new Date().toISOString();
  const notes: string[] = [];
  const base: CaptureRow = {
    server_id: item.serverId, endpoint_url: item.url, captured_at: capturedAt,
    status: 'error', http_status: null, response_time_ms: null, error_class: null,
    item_count: null, page_count: 0, truncated: false,
    raw_json: null, raw_bytes: null, token_estimate: null,
    cache_ttl_ms: null, protocol_version: null, list_changed: null,
    note: null, items: [],
  };
  const finish = (r: Partial<CaptureRow>): CaptureRow => ({
    ...base, ...r, note: notes.length ? notes.join('; ').slice(0, 400) : null,
  });

  let parsed: URL;
  try { parsed = new URL(item.url); } catch {
    notes.push(`invalid URL: ${item.url.slice(0, 160)}`);
    return { capture: finish({ status: 'not_attempted', error_class: 'other' }), retryAfterMs: null };
  }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
    notes.push(`non-http(s) scheme ${parsed.protocol}`);
    return { capture: finish({ status: 'not_attempted', error_class: 'other' }), retryAfterMs: null };
  }

  const collected: any[] = [];
  const pages: unknown[] = [];
  let cursor: string | null = null;
  let pageCount = 0;
  let truncated = false;
  let lastStatus: number | null = null;
  let lastCt: string | null = null;
  let ttl: number | null = null;
  let listChanged: boolean | null = null;
  let rawBytes = 0;
  const t0 = Date.now();

  for (;;) {
    if (pageCount >= MAX_PAGES) {
      // NOT SILENT. The cursor chain was still going; the capture records that it
      // is partial, and the evidence view refuses to read a partial list as an
      // absence.
      truncated = true;
      notes.push(`MAX_PAGES=${MAX_PAGES} reached with a cursor still open -- capture is PARTIAL`);
      break;
    }

    let res: Response;
    try {
      res = await fetchImpl(parsed.href, {
        method: 'POST',
        redirect: 'follow',
        signal: AbortSignal.timeout(TIMEOUT_MS),
        headers: listHeaders(spec),
        body: listBody(spec, cursor),
      });
    } catch (err) {
      const { cls, raw } = classifyException(err);
      if (cls === 'other') notes.push(raw.slice(0, 200));
      return {
        capture: finish({
          status: pageCount > 0 ? 'ok' : 'error',
          error_class: cls, response_time_ms: Date.now() - t0,
          page_count: pageCount, truncated: pageCount > 0,
          item_count: pageCount > 0 ? collected.length : null,
          ...(pageCount > 0 ? { raw_json: pages, raw_bytes: rawBytes, token_estimate: Math.ceil(rawBytes / 4) } : {}),
        }),
        retryAfterMs: null,
      };
    }

    pageCount++;
    lastStatus = res.status;
    lastCt = res.headers.get('content-type')?.slice(0, 256) ?? null;

    // 429 is a HOST signal, not a per-request one. Body is not needed.
    if (res.status === 429) {
      const retryAfterMs = parseRetryAfter(res.headers.get('retry-after'), Date.now());
      try { await res.body?.cancel(); } catch { /* none */ }
      notes.push('429 -- host backed off');
      return {
        capture: finish({
          status: 'error', error_class: 'rate_limited', http_status: 429,
          response_time_ms: Date.now() - t0, page_count: pageCount,
        }),
        retryAfterMs,
      };
    }

    // THE GUARD. AbortSignal.timeout stays armed after fetch() settles on
    // headers, so an endpoint that answers promptly then stalls mid-body has its
    // stream aborted here. readBounded has try/finally and no catch, so without
    // this the rejection would escape the worker entirely.
    let body: string;
    try {
      body = await readBounded(res);
    } catch (err) {
      const { cls, raw } = classifyException(err);
      notes.push(`body read aborted after ${Date.now() - t0}ms: ${raw.slice(0, 160)}`);
      return {
        capture: finish({
          status: 'error', error_class: cls, http_status: res.status,
          response_time_ms: Date.now() - t0, page_count: pageCount,
        }),
        retryAfterMs: null,
      };
    }

    rawBytes += Buffer.byteLength(body, 'utf8');
    const env = parseEnvelope(body, lastCt);

    if (res.status >= 400) {
      const code = typeof env?.error === 'object' ? env?.error?.code : undefined;
      const msg = errorMessage(env);
      const unsupported = res.status === 405 || res.status === 501 || code === -32601;
      const session = msg !== '' && demandsSession(msg);
      notes.push(`HTTP ${res.status}${code !== undefined ? ` jsonrpc ${code}` : ''}`);
      return {
        capture: finish({
          // session_required wins: it is the more specific fact, and it is the
          // one the disclosure page promises we do not work around.
          status: session ? 'session_required' : unsupported ? 'unsupported' : 'error',
          error_class: classifyStatus(res.status), http_status: res.status,
          response_time_ms: Date.now() - t0, page_count: pageCount,
          raw_json: env ?? null,
        }),
        retryAfterMs: null,
      };
    }

    const result = env?.result;
    if (!result || !Array.isArray(result[spec.resultKey])) {
      const code = typeof env?.error === 'object' ? env?.error?.code : undefined;
      const msg = errorMessage(env);
      notes.push(
        code !== undefined
          ? `jsonrpc ${code}: ${msg.slice(0, 120)}`
          : `no result.${spec.resultKey} array in a 2xx response`,
      );
      return {
        capture: finish({
          // A 200 carrying -32601 is the common shape for "I do not implement
          // this method", and it is the case this whole distinction exists for.
          status: code === -32601 ? 'unsupported' : msg !== '' && demandsSession(msg) ? 'session_required' : 'error',
          error_class: classifyStatus(res.status), http_status: res.status,
          response_time_ms: Date.now() - t0, page_count: pageCount,
          raw_json: env ?? null,
        }),
        retryAfterMs: null,
      };
    }

    pages.push(result);
    for (const o of result[spec.resultKey]) collected.push(o);
    if (typeof result.ttl === 'number') ttl = result.ttl;
    const caps = env?.result?.capabilities?.[spec.resultKey]?.listChanged;
    if (typeof caps === 'boolean') listChanged = caps;

    const next = typeof result.nextCursor === 'string' && result.nextCursor !== '' ? result.nextCursor : null;
    if (next === null) break;
    if (next === cursor) {
      // A server repeating its cursor would loop forever. Recorded as partial.
      truncated = true;
      notes.push('server returned an unchanged nextCursor -- stopped, capture is PARTIAL');
      break;
    }
    cursor = next;
  }

  return {
    capture: finish({
      status: 'ok',
      error_class: 'ok',
      http_status: lastStatus,
      response_time_ms: Date.now() - t0,
      item_count: collected.length,
      page_count: pageCount,
      truncated,
      raw_json: pages,
      raw_bytes: rawBytes,
      token_estimate: Math.ceil(rawBytes / 4),
      cache_ttl_ms: ttl,
      protocol_version: MCP_PROTOCOL_VERSION,
      list_changed: listChanged,
      items: collected.map((o) => {
        const vals = spec.itemToRow(o);
        const row: ItemRow = {};
        spec.itemColumns.forEach((c, i) => { row[c] = vals[i]; });
        return row;
      }),
    }),
    retryAfterMs: null,
  };
}

/* ---------------------------------------------------------------------------
 * target set -- identical rule to collect-mcp-tools.ts
 * ------------------------------------------------------------------------ */

export interface Target { serverId: string; url: string; host: string }

export async function loadTargets(): Promise<{ targets: Target[]; excluded: Record<string, number> }> {
  const rows = await q<{ endpoint_url: string; server_id: string; discover_status: string }>(`
    WITH run AS (
      SELECT s.started_at, s.finished_at
        FROM scan_runs s
       WHERE s.finished_at IS NOT NULL
         AND EXISTS (
           SELECT 1 FROM mcp_endpoint_probes p
            WHERE p.discover_status IS NOT NULL
              AND p.probed_at BETWEEN s.started_at AND s.finished_at
         )
       ORDER BY s.finished_at DESC
       LIMIT 1
    ),
    latest AS (
      SELECT DISTINCT ON (p.endpoint_url)
             p.endpoint_url, p.server_id, p.discover_status
        FROM mcp_endpoint_probes p, run
       WHERE p.discover_status IS NOT NULL
         AND p.probed_at BETWEEN run.started_at AND run.finished_at
       ORDER BY p.endpoint_url, p.probed_at DESC
    )
    SELECT endpoint_url, server_id::text AS server_id, discover_status FROM latest
  `);

  const excluded: Record<string, number> = {};
  const targets: Target[] = [];
  for (const r of rows) {
    if (r.discover_status !== 'ok') {
      excluded[r.discover_status] = (excluded[r.discover_status] ?? 0) + 1;
      continue;
    }
    let host: string;
    try { host = new URL(r.endpoint_url).hostname.toLowerCase(); } catch { continue; }
    targets.push({ serverId: r.server_id, url: r.endpoint_url, host });
  }
  return { targets, excluded };
}

/* ---------------------------------------------------------------------------
 * main
 * ------------------------------------------------------------------------ */

export interface RunResult {
  attempted: number; captures: number; items: number; errors: number;
  byStatus: Record<string, number>; runId: string;
}

export async function runListCollector(spec: ListSpec): Promise<RunResult> {
  const startedAt = new Date();
  let errors = 0;

  /**
   * ONE ID PER RUN, stamped on every capture row and echoed to
   * ingestion_log.metadata.run_id, so "which run produced these rows" is
   * answerable from a recorded value rather than a time-window guess. Same
   * reasoning as collect-mcp-tools.ts, including why it is not a scan_runs row:
   * scan_runs has no source column and six workers already write to it.
   */
  const RUN_ID = randomUUID();

  const conc = parseLimit(process.env[`${spec.envPrefix}_CONCURRENCY`], DEFAULT_CONCURRENCY);
  if (!conc.ok || conc.value === null) throw new Error(`${spec.envPrefix}_CONCURRENCY unreadable: ${conc.problem}`);
  const concurrency = conc.value;

  const dl = parseLimit(process.env[`${spec.envPrefix}_DEADLINE_MINUTES`], 60);
  if (!dl.ok || dl.value === null) throw new Error(`${spec.envPrefix}_DEADLINE_MINUTES unreadable: ${dl.problem}`);
  const deadlineAt = startedAt.getTime() + dl.value * 60_000;

  const { targets: all, excluded } = await loadTargets();
  console.log(
    `${spec.tag} target set ${all.length} endpoints; excluded by discover_status: ` +
    (Object.entries(excluded).map(([k, v]) => `${k}=${v}`).join(' ') || '(none)'),
  );
  if (all.some((t) => t.url.includes('{'))) {
    throw new Error('a target URL contains an unsubstituted template placeholder -- refusing to dial');
  }

  const rawLimit = process.env[`${spec.envPrefix}_LIMIT`];
  const limited = rawLimit !== undefined && rawLimit.trim() !== '';
  let targets = all;
  if (limited) {
    const lp = parseLimit(rawLimit, all.length);
    if (!lp.ok || lp.value === null) throw new Error(`${spec.envPrefix}_LIMIT unreadable: ${lp.problem}`);
    targets = all.slice(0, lp.value);
    console.log(`${spec.tag} ${spec.envPrefix}_LIMIT=${lp.value} -- SMOKE RUN, NOT a full sweep`);
  }

  /* Operator opt-outs. Loaded ONCE, before the scheduler is built, so "no
   * request was sent" is a fact about control flow rather than a conditional
   * inside collectOnce that a later edit could reorder past the fetch.
   * FAIL CLOSED: an unreadable table throws before the first dial. */
  const optOutSet = await loadExclusions(ingestPool());
  const corpusSize = targets.length;
  const optOutTargets: { target: Target; note: string }[] = [];
  const dialable: Target[] = [];
  for (const t of targets) {
    const match = isExcluded({ url: t.url, host: t.host, serverId: t.serverId }, optOutSet, spec.exclusionMethod);
    if (match.excluded) optOutTargets.push({ target: t, note: exclusionNote(match) });
    else dialable.push(t);
  }
  targets = dialable;
  console.log(
    `${spec.tag} exclusions: ${optOutSet.count} active rule(s) -- ` +
    `${optOutTargets.length} of ${corpusSize} endpoints excluded and NOT dialled ` +
    `(a capture is still written for each, status='excluded')`,
  );

  const sched = new HostScheduler({ deadlineAt });
  for (const t of targets) sched.add({ serverId: t.serverId, url: t.url, host: t.host, attempts: 0 });
  console.log(
    `${spec.tag} ${targets.length} endpoints across ${sched.stats().hosts} hosts; ` +
    `concurrency ${concurrency}, deadline ${dl.value}min, MAX_PAGES ${MAX_PAGES}`,
  );
  console.log(`${spec.tag} run_id ${RUN_ID}${limited ? ' (BOUNDED RUN)' : ''}`);

  const byStatus: Record<string, number> = {};
  let capturesWritten = 0;
  let itemsWritten = 0;
  let seen = 0;
  let pending: CaptureRow[] = [];

  const bind = (col: string, v: unknown): unknown =>
    spec.jsonColumns.has(col) ? (v === null || v === undefined ? null : JSON.stringify(v)) : v ?? null;

  const flush = async (): Promise<void> => {
    if (pending.length === 0) return;
    const chunk = pending;
    pending = [];
    for (const c of chunk) {
      try {
        const inserted = await q<{ id: string }>(
          `INSERT INTO ${spec.captureTable}
             (server_id, endpoint_url, captured_at, status, http_status,
              response_time_ms, error_class, ${spec.countColumn}, page_count, truncated,
              raw_json, raw_bytes, token_estimate, cache_ttl_ms,
              protocol_version, list_changed, note, scan_run_id)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
           RETURNING id`,
          [
            c.server_id, c.endpoint_url, c.captured_at, c.status, c.http_status,
            c.response_time_ms, c.error_class, c.item_count, c.page_count, c.truncated,
            c.raw_json === null ? null : JSON.stringify(c.raw_json),
            c.raw_bytes, c.token_estimate, c.cache_ttl_ms,
            c.protocol_version, c.list_changed, c.note,
            // Stamped from the closure, not per row, so every capture in a run
            // carries the same value by construction -- including the excluded
            // ones and the probe_exception ones, which flow through this same
            // path. A per-row field could diverge; a closure constant cannot.
            RUN_ID,
          ],
        );
        capturesWritten++;
        const captureId = inserted[0]?.id;
        if (captureId && c.items.length > 0) {
          const cols = ['capture_id', 'server_id', 'endpoint_url', ...spec.itemColumns];
          await insertRows(spec.itemTable, cols, c.items.map((it) => [
            captureId, c.server_id, c.endpoint_url,
            ...spec.itemColumns.map((col) => bind(col, it[col])),
          ]));
          itemsWritten += c.items.length;
        }
      } catch (err) {
        errors++;
        console.error(`${spec.tag} insert failed for`, c.endpoint_url, err instanceof Error ? err.message : err);
      }
    }
  };

  // Queued BEFORE the sweep starts, so the finally's flush banks them on every
  // path including an abort.
  for (const { target, note } of optOutTargets) {
    const capture: CaptureRow = {
      server_id: target.serverId, endpoint_url: target.url,
      captured_at: new Date().toISOString(),
      status: 'excluded', http_status: null, response_time_ms: null,
      error_class: null, item_count: null, page_count: 0, truncated: false,
      raw_json: null, raw_bytes: null, token_estimate: null,
      cache_ttl_ms: null, protocol_version: null, list_changed: null,
      note: note.slice(0, 400), items: [],
    };
    byStatus[capture.status] = (byStatus[capture.status] ?? 0) + 1;
    pending.push(capture);
  }

  /** Never throws. Mirrors discoverSafely() and collectSafely(). */
  const collectSafely = async (item: Item): Promise<{ capture: CaptureRow; retryAfterMs: number | null }> => {
    try {
      return await collectOnce(spec, item);
    } catch (err) {
      const msg = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
      console.error(
        `${spec.tag} endpoint THREW (recorded as a capture, sweep continues) ` +
        `host=${item.host} url=${item.url} attempts=${item.attempts} -- ${msg}`,
      );
      if (err instanceof Error && err.stack) console.error(err.stack);
      return {
        capture: {
          server_id: item.serverId, endpoint_url: item.url,
          captured_at: new Date().toISOString(),
          status: 'error', http_status: null, response_time_ms: null,
          error_class: 'other', item_count: null, page_count: 0, truncated: false,
          raw_json: null, raw_bytes: null, token_estimate: null,
          cache_ttl_ms: null, protocol_version: null, list_changed: null,
          note: `probe_exception: ${msg}`.slice(0, 400), items: [],
        },
        retryAfterMs: null,
      };
    }
  };

  let sweepError: unknown = null;
  const MAX_ATTEMPTS = 2;

  try {
    const worker = async (): Promise<void> => {
      for (;;) {
        const claimed = sched.claim(Date.now());
        if (claimed === null) return;
        if ('waitMs' in claimed) {
          await new Promise((r) => setTimeout(r, claimed.waitMs));
          continue;
        }
        const { item, state } = claimed;
        item.attempts++;

        const { capture, retryAfterMs } = await collectSafely(item);

        // Release the host BEFORE anything else so a throw cannot leak a slot.
        if (capture.error_class === 'rate_limited' && item.attempts < MAX_ATTEMPTS) {
          sched.penalise(state, retryAfterMs, Date.now());
          sched.release(state, false);
          sched.requeue(item);
          continue;
        }
        if (capture.error_class === 'rate_limited') {
          sched.penalise(state, null, Date.now());
          sched.release(state, false);
        } else {
          sched.release(state, capture.status === 'ok');
        }

        byStatus[capture.status] = (byStatus[capture.status] ?? 0) + 1;
        pending.push(capture);
        seen++;
        if (pending.length >= INSERT_CHUNK) await flush();
        if (seen % 50 === 0) {
          const s = sched.stats();
          console.log(`${spec.tag} ${seen}/${targets.length} - 429s=${s.total429} - hosts backed off=${s.hostsBackedOff}`);
        }
      }
    };
    await Promise.all(Array.from({ length: concurrency }, worker));
  } catch (err) {
    sweepError = err;
    console.error(`${spec.tag} SWEEP ABORTED -- flushing what completed:`, err instanceof Error ? err.stack ?? err.message : err);
  } finally {
    await flush();
  }

  const hs = sched.stats();

  await logIngestionPg({
    sourceSlug: spec.sourceSlug,
    startedAt,
    itemsFetched: seen,
    itemsNew: itemsWritten,
    itemsFailed: errors,
    metadata: {
      run_id: RUN_ID,
      endpoints: corpusSize, hosts: hs.hosts, by_status: byStatus,
      captures_written: capturesWritten, items_written: itemsWritten,
      excluded_by_discover_status: excluded,
      endpoints_excluded_by_operator: optOutTargets.length,
      active_exclusion_rules: optOutSet.count,
      smoke_run: limited, deadline_hit: hs.deadlineHit, undispatched: hs.undispatched,
      max_pages: MAX_PAGES,
      method: spec.method,
      aborted: sweepError ? String(sweepError instanceof Error ? sweepError.message : sweepError) : null,
    },
  });

  if (hs.deadlineHit) {
    console.warn(
      `${spec.tag} DEADLINE reached after ${dl.value}min -- ` +
      `${hs.undispatched} of ${targets.length} endpoints were never dispatched and have NO capture today. ` +
      `This is partial coverage, not a clean sweep.`,
    );
  }

  console.log(
    `${spec.tag} done -- attempted=${seen} captures=${capturesWritten} items=${itemsWritten} errors=${errors}`,
  );
  console.log(`${spec.tag} status: ${Object.entries(byStatus).map(([k, v]) => `${k}=${v}`).join(' ') || '(none)'}`);

  if (sweepError) throw sweepError;
  return { attempted: seen, captures: capturesWritten, items: itemsWritten, errors, byStatus, runId: RUN_ID };
}
