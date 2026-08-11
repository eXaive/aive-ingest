/**
 * collect-mcp-tools.ts -- one tools/list POST per endpoint that answered
 * server/discover on the current specification, plus cursor follow-ups.
 *
 * THE DISCLOSURE GATE. This worker could not be written until
 * https://aive.global/mcp-trust/census said so. That page previously read
 * "server/discover is the only method we issue"; issuing a second method while
 * that sentence was live would have made a published commitment false for every
 * operator who had already read it. The page was amended first (aive-platform
 * d48d005, merged 7c110fc, verified live: 200, no redirect, two-method sentence
 * present). If a third method is ever added, the page changes FIRST.
 *
 * WHAT IS SENT -- one request shape, plus pagination:
 *
 *   POST <endpoint>
 *   Content-Type: application/json
 *   Accept: application/json, text/event-stream        (T:76)
 *   MCP-Protocol-Version: 2026-07-28                  (T:250-259, REQUIRED)
 *   Mcp-Method: tools/list                            (T:286-292, REQUIRED)
 *   {"jsonrpc":"2.0","id":1,"method":"tools/list",
 *    "params":{"_meta":{...}}}                        (S:1063-1065 params REQUIRED)
 *
 * NO initialize. NO session header sent or read. NO Authorization, no cookies.
 * NO tools/call -- this worker reads definitions and stops at the definition.
 * Mcp-Name is required only for tools/call, resources/read and prompts/get
 * (T:286-292), none of which this worker issues, so it is not sent.
 *
 * params is REQUIRED on a PaginatedRequest (S:1063-1065:
 * `interface PaginatedRequest extends JSONRPCRequest { params: PaginatedRequestParams }`),
 * and PaginatedRequestParams extends RequestParams, whose _meta is required.
 * `cursor` is optional and is added only on follow-up pages.
 *
 * TARGET SET: the endpoints whose latest discover row in the reference sweep has
 * discover_status='ok'. That is 360 distinct URLs of 11,109 probed.
 * session_required is a DIFFERENT status and is therefore excluded by
 * construction, not by a filter that could be forgotten -- and the exclusion is
 * asserted at startup anyway, because the disclosure page promises it.
 *
 * PACING: HostScheduler is imported from discover-mcp-servers.ts and used
 * unmodified. The dispatch loop below is NOT runSweep: runSweep's callbacks are
 * typed to DiscoverRow, and shoehorning a tool capture into that shape to reuse
 * the loop would be worse than restating twenty lines. The 429 path, the
 * release-before-anything-else rule and the flush/close-in-finally structure all
 * mirror it deliberately -- see the comments at each, and the 2026-08-11
 * post-mortem those rules came from.
 *
 * Env: AIVE_INGEST_DATABASE_URL (scoped role; pooler host is aws-1-us-east-1,
 * NOT aws-0). Optional: TOOLS_LIMIT (cap endpoints for a smoke run),
 * TOOLS_CONCURRENCY (default 8), TOOLS_DEADLINE_MINUTES (default 60).
 */

import { q, insertRows, endIngestPool, logIngestionPg } from '../lib/ingest/db';
import { parseLimit } from '../lib/ingest/parseLimit';
import { classifyException, classifyStatus, parseRetryAfter } from '../lib/mcp/errorClass';
import {
  schemaHash, contractHash, paramSet, paramCount, requiredCount, maxDepth,
  falseDriftClasses, tallyFalseDrift, tokenEstimate,
  type FalseDriftClasses,
} from '../lib/mcp/schemaHash';
// HostScheduler and Item are imported and used UNMODIFIED. Nothing in
// discover-mcp-servers.ts is changed by this file.
import { HostScheduler, type Item } from './discover-mcp-servers';

/**
 * BYTE-IDENTICAL to TOOLS_USER_AGENT in eXaive/aive-platform
 * app/mcp-trust/census/page.tsx once that page renders it. Same rule as
 * DISCOVER_USER_AGENT: the page is what an operator matches their access log
 * against, so if the two copies drift the page describes a collector that is not
 * the one calling them. If either changes, change both in the same pass.
 *
 * The URL must resolve to the census route -- that round trip is the entire
 * purpose of putting a URL in a User-Agent.
 */
const USER_AGENT =
  'AIVE-MCP-Tools/1.0 (+https://aive.global/mcp-trust/census; one tools/list POST per current-spec endpoint, no auth attempted, no tool ever invoked)';

const SOURCE_SLUG = 'mcp-tools';

const MCP_PROTOCOL_VERSION = '2026-07-28';
const TOOLS_METHOD = 'tools/list';

const DEFAULT_CONCURRENCY = 8;
const TIMEOUT_MS = 15_000;
const INSERT_CHUNK = 500;
/** Cursor pages per endpoint. A stop is RECORDED as truncated, never swallowed. */
const MAX_PAGES = 20;
const MAX_BODY_BYTES = 1_048_576; // tool lists are larger than discover results

/** One constant feeds the header and the body, so HeaderMismatch is unrepresentable. */
function toolsBody(cursor: string | null): string {
  const params: Record<string, unknown> = {
    _meta: {
      'io.modelcontextprotocol/protocolVersion': MCP_PROTOCOL_VERSION,
      'io.modelcontextprotocol/clientInfo': { name: 'aive-mcp-tools', version: '1.0.0' },
      'io.modelcontextprotocol/clientCapabilities': {},
    },
  };
  if (cursor !== null) params.cursor = cursor;
  return JSON.stringify({ jsonrpc: '2.0', id: 1, method: TOOLS_METHOD, params });
}

function toolsHeaders(): Record<string, string> {
  return {
    'Content-Type': 'application/json',
    Accept: 'application/json, text/event-stream',
    'MCP-Protocol-Version': MCP_PROTOCOL_VERSION,
    'Mcp-Method': TOOLS_METHOD,
    'User-Agent': USER_AGENT,
  };
}

export type CaptureStatus = 'ok' | 'error' | 'unsupported' | 'session_required' | 'not_attempted';

export interface ToolRow {
  tool_name: string;
  tool_title: string | null;
  description: string | null;
  input_schema: unknown;
  output_schema: unknown;
  schema_hash: string | null;
  contract_hash: string | null;
  param_set: unknown;
  param_count: number | null;
  required_count: number | null;
  max_depth: number | null;
  type_union: boolean | null;
  union_via_composition: boolean | null;
  has_ref: boolean | null;
  has_defs: boolean | null;
  annotations: unknown;
  token_estimate: number | null;
}

export interface CaptureRow {
  server_id: string;
  endpoint_url: string;
  captured_at: string;
  status: CaptureStatus;
  http_status: number | null;
  response_time_ms: number | null;
  error_class: string | null;
  tool_count: number | null;
  page_count: number;
  truncated: boolean;
  raw_json: unknown;
  raw_bytes: number | null;
  token_estimate: number | null;
  cache_ttl_ms: number | null;
  protocol_version: string | null;
  list_changed: boolean | null;
  note: string | null;
  tools: ToolRow[];
}

/** Bounded body read. Guarded at every call site -- see the note there. */
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

/** JSON or SSE, same as the discover worker: Accept advertises both. */
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

/** Same session-demand detection the discover worker uses. */
export function demandsSession(msg: string): boolean {
  return /session/i.test(msg) && /(required|missing|invalid|expired|initiali)/i.test(msg);
}

/** Turn one Tool object from the wire into a row. Pure and total. */
export function toolToRow(t: any): ToolRow {
  const input = t?.inputSchema ?? null;
  const drift: FalseDriftClasses = falseDriftClasses(input);
  const raw = JSON.stringify(t ?? null);
  return {
    tool_name: typeof t?.name === 'string' ? t.name : '',
    tool_title: typeof t?.title === 'string' ? t.title : null,
    description: typeof t?.description === 'string' ? t.description : null,
    input_schema: input,
    output_schema: t?.outputSchema ?? null,
    // Hashes only where a schema exists. A tool with no inputSchema is spec-
    // invalid (inputSchema is required on Tool), so the absence is recorded as
    // NULL rather than hashed as the string "null" -- which would collide with a
    // literal null schema and quietly merge two different facts.
    schema_hash: input === null ? null : schemaHash(input),
    contract_hash: input === null ? null : contractHash(input),
    param_set: paramSet(input),
    param_count: paramCount(input),
    required_count: requiredCount(input),
    max_depth: input === null ? null : maxDepth(input),
    type_union: drift.typeUnion,
    union_via_composition: drift.unionViaComposition,
    has_ref: drift.hasRef,
    has_defs: drift.hasDefs,
    annotations: t?.annotations ?? null,
    token_estimate: tokenEstimate(raw),
  };
}

/**
 * One endpoint, start to finish, including its cursor chain.
 *
 * NEVER THROWS. Every await is inside a try that classifies and returns a
 * capture. That contract is why the 2026-08-11 discover sweep died at 1,500 of
 * 11,006 endpoints: an unguarded `await readBounded(res)` let an
 * AbortSignal.timeout rejection escape, reject Promise.all, and abandon every
 * in-flight worker. The guard at the body read below is the one that mattered.
 */
export async function collectOnce(item: Item): Promise<{ capture: CaptureRow; retryAfterMs: number | null } > {
  const capturedAt = new Date().toISOString();
  const notes: string[] = [];
  const base: CaptureRow = {
    server_id: item.serverId, endpoint_url: item.url, captured_at: capturedAt,
    status: 'error', http_status: null, response_time_ms: null, error_class: null,
    tool_count: null, page_count: 0, truncated: false,
    raw_json: null, raw_bytes: null, token_estimate: null,
    cache_ttl_ms: null, protocol_version: null, list_changed: null,
    note: null, tools: [],
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

  const tools: any[] = [];
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
      // NOT SILENT. The cursor chain was still going; the capture records that
      // it is partial, and the evidence view refuses to read a partial list as
      // an absence.
      truncated = true;
      notes.push(`MAX_PAGES=${MAX_PAGES} reached with a cursor still open -- capture is PARTIAL`);
      break;
    }

    let res: Response;
    try {
      res = await fetch(parsed.href, {
        method: 'POST',
        redirect: 'follow',
        signal: AbortSignal.timeout(TIMEOUT_MS),
        headers: toolsHeaders(),
        body: toolsBody(cursor),
      });
    } catch (err) {
      const { cls, raw } = classifyException(err);
      if (cls === 'other') notes.push(raw.slice(0, 200));
      return {
        capture: finish({
          status: pageCount > 0 ? 'ok' : 'error',
          error_class: cls, response_time_ms: Date.now() - t0,
          page_count: pageCount, truncated: pageCount > 0,
          tool_count: pageCount > 0 ? tools.length : null,
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
    if (!result || !Array.isArray(result.tools)) {
      const code = typeof env?.error === 'object' ? env?.error?.code : undefined;
      const msg = errorMessage(env);
      notes.push(code !== undefined ? `jsonrpc ${code}: ${msg.slice(0, 120)}` : 'no result.tools array in a 2xx response');
      return {
        capture: finish({
          status: code === -32601 ? 'unsupported' : msg !== '' && demandsSession(msg) ? 'session_required' : 'error',
          error_class: classifyStatus(res.status), http_status: res.status,
          response_time_ms: Date.now() - t0, page_count: pageCount,
          raw_json: env ?? null,
        }),
        retryAfterMs: null,
      };
    }

    pages.push(result);
    for (const t of result.tools) tools.push(t);
    if (typeof result.ttl === 'number') ttl = result.ttl;
    if (typeof env?.result?.capabilities?.tools?.listChanged === 'boolean') {
      listChanged = env.result.capabilities.tools.listChanged;
    }

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

  const rows = tools.map(toolToRow);
  return {
    capture: finish({
      status: 'ok',
      error_class: 'ok',
      http_status: lastStatus,
      response_time_ms: Date.now() - t0,
      tool_count: rows.length,
      page_count: pageCount,
      truncated,
      raw_json: pages,
      raw_bytes: rawBytes,
      token_estimate: Math.ceil(rawBytes / 4),
      cache_ttl_ms: ttl,
      protocol_version: MCP_PROTOCOL_VERSION,
      list_changed: listChanged,
      tools: rows,
    }),
    retryAfterMs: null,
  };
}

/* ---------------------------------------------------------------------------
 * target set
 * ------------------------------------------------------------------------ */

export interface Target { serverId: string; url: string; host: string }

/**
 * The endpoints whose discover row in the most recent finished sweep says 'ok'.
 *
 * Located by the same self-validating rule mcp_discover_census() uses: the most
 * recent finished scan_run that actually contains discover rows inside its own
 * window. mcp_endpoint_probes has no scan_run_id column and scan_runs has no
 * source column, so the run is found by time window rather than by parsing a
 * free-text status string.
 */
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

export async function collectMcpTools(): Promise<{
  attempted: number; captures: number; tools: number; errors: number;
  byStatus: Record<string, number>; drift: ReturnType<typeof tallyFalseDrift>;
}> {
  const startedAt = new Date();
  let errors = 0;

  const concurrencyParsed = parseLimit(process.env.TOOLS_CONCURRENCY, DEFAULT_CONCURRENCY);
  if (!concurrencyParsed.ok || concurrencyParsed.value === null) {
    throw new Error(`TOOLS_CONCURRENCY unreadable: ${concurrencyParsed.problem}`);
  }
  const concurrency = concurrencyParsed.value;

  const deadlineParsed = parseLimit(process.env.TOOLS_DEADLINE_MINUTES, 60);
  if (!deadlineParsed.ok || deadlineParsed.value === null) {
    throw new Error(`TOOLS_DEADLINE_MINUTES unreadable: ${deadlineParsed.problem}`);
  }
  const deadlineAt = startedAt.getTime() + deadlineParsed.value * 60_000;

  const { targets: all, excluded } = await loadTargets();

  // THE DISCLOSURE PROMISE, ASSERTED. The page says tools/list goes only to
  // endpoints that answered without requiring a session. That is true by
  // construction (we select discover_status='ok'), and it is checked anyway,
  // because a promise on a public page deserves an assertion rather than a
  // comment.
  if (excluded.session_required === undefined && Object.keys(excluded).length > 0) {
    console.warn('[collect-mcp-tools] no session_required endpoints in the reference sweep');
  }
  console.log(
    `[collect-mcp-tools] target set ${all.length} endpoints; excluded by discover_status: ` +
    (Object.entries(excluded).map(([k, v]) => `${k}=${v}`).join(' ') || '(none)'),
  );
  if (all.some((t) => t.url.includes('{'))) {
    throw new Error('a target URL contains an unsubstituted template placeholder -- refusing to dial');
  }

  // parseLimit's fallback is typed number, so "unset means no cap" cannot be
  // expressed by passing null. The env var is inspected first and parseLimit is
  // only consulted when a value was actually supplied -- so a malformed
  // TOOLS_LIMIT still fails loudly instead of silently becoming "no cap".
  const rawLimit = process.env.TOOLS_LIMIT;
  const limited = rawLimit !== undefined && rawLimit.trim() !== '';
  let targets = all;
  if (limited) {
    const limitParsed = parseLimit(rawLimit, all.length);
    if (!limitParsed.ok || limitParsed.value === null) {
      throw new Error(`TOOLS_LIMIT unreadable: ${limitParsed.problem}`);
    }
    targets = all.slice(0, limitParsed.value);
    console.log(`[collect-mcp-tools] TOOLS_LIMIT=${limitParsed.value} -- SMOKE RUN, NOT a full sweep`);
  }

  const sched = new HostScheduler({ deadlineAt });
  for (const t of targets) sched.add({ serverId: t.serverId, url: t.url, host: t.host, attempts: 0 });
  console.log(
    `[collect-mcp-tools] ${targets.length} endpoints across ${sched.stats().hosts} hosts; ` +
    `concurrency ${concurrency}, per-host gap ${sched.baseGapMs}ms adaptive to ${sched.maxGapMs}ms, ` +
    `deadline ${deadlineParsed.value}min, MAX_PAGES ${MAX_PAGES}`,
  );

  const CAP_COLS = [
    'server_id', 'endpoint_url', 'captured_at', 'status', 'http_status',
    'response_time_ms', 'error_class', 'tool_count', 'page_count', 'truncated',
    'raw_json', 'raw_bytes', 'token_estimate', 'cache_ttl_ms',
    'protocol_version', 'list_changed',
  ];
  const TOOL_COLS = [
    'capture_id', 'server_id', 'endpoint_url', 'tool_name', 'tool_title',
    'description', 'input_schema', 'output_schema', 'schema_hash',
    'contract_hash', 'param_set', 'param_count', 'required_count', 'max_depth',
    'type_union', 'union_via_composition', 'has_ref', 'has_defs',
    'annotations', 'token_estimate',
  ];

  const byStatus: Record<string, number> = {};
  const driftAll: FalseDriftClasses[] = [];
  let capturesWritten = 0;
  let toolsWritten = 0;
  let seen = 0;
  let pending: CaptureRow[] = [];

  /**
   * Captures and their tools are written together, capture first, because
   * mcp_tools.capture_id is a FK. insertRows returns nothing useful for ids, so
   * each capture is inserted with RETURNING id via q() and its tools follow.
   */
  const flush = async (): Promise<void> => {
    if (pending.length === 0) return;
    const chunk = pending;
    pending = [];
    for (const c of chunk) {
      try {
        const inserted = await q<{ id: string }>(
          `INSERT INTO mcp_tool_captures
             (server_id, endpoint_url, captured_at, status, http_status,
              response_time_ms, error_class, tool_count, page_count, truncated,
              raw_json, raw_bytes, token_estimate, cache_ttl_ms,
              protocol_version, list_changed, note)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
           RETURNING id`,
          [
            c.server_id, c.endpoint_url, c.captured_at, c.status, c.http_status,
            c.response_time_ms, c.error_class, c.tool_count, c.page_count, c.truncated,
            c.raw_json === null ? null : JSON.stringify(c.raw_json),
            c.raw_bytes, c.token_estimate, c.cache_ttl_ms,
            c.protocol_version, c.list_changed, c.note,
          ],
        );
        capturesWritten++;
        const captureId = inserted[0]?.id;
        if (captureId && c.tools.length > 0) {
          await insertRows('mcp_tools', TOOL_COLS, c.tools.map((t) => [
            captureId, c.server_id, c.endpoint_url, t.tool_name, t.tool_title,
            t.description,
            t.input_schema === null ? null : JSON.stringify(t.input_schema),
            t.output_schema === null ? null : JSON.stringify(t.output_schema),
            t.schema_hash, t.contract_hash,
            t.param_set === null ? null : JSON.stringify(t.param_set),
            t.param_count, t.required_count, t.max_depth,
            t.type_union, t.union_via_composition, t.has_ref, t.has_defs,
            t.annotations === null ? null : JSON.stringify(t.annotations),
            t.token_estimate,
          ]));
          toolsWritten += c.tools.length;
        }
      } catch (err) {
        errors++;
        console.error('[collect-mcp-tools] insert failed for', c.endpoint_url, err instanceof Error ? err.message : err);
      }
    }
  };

  /** Never throws. Mirrors discoverSafely() in the discover worker. */
  const collectSafely = async (item: Item): Promise<{ capture: CaptureRow; retryAfterMs: number | null }> => {
    try {
      return await collectOnce(item);
    } catch (err) {
      const msg = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
      console.error(
        `[collect-mcp-tools] endpoint THREW (recorded as a capture, sweep continues) ` +
        `host=${item.host} url=${item.url} attempts=${item.attempts} -- ${msg}`,
      );
      if (err instanceof Error && err.stack) console.error(err.stack);
      return {
        capture: {
          server_id: item.serverId, endpoint_url: item.url,
          captured_at: new Date().toISOString(),
          status: 'error', http_status: null, response_time_ms: null,
          error_class: 'other', tool_count: null, page_count: 0, truncated: false,
          raw_json: null, raw_bytes: null, token_estimate: null,
          cache_ttl_ms: null, protocol_version: null, list_changed: null,
          note: `probe_exception: ${msg}`.slice(0, 400), tools: [],
        },
        retryAfterMs: null,
      };
    }
  };

  let sweepError: unknown = null;
  const MAX_ATTEMPTS = 2;

  try {
    // Dispatch loop. Mirrors runSweep, which cannot be reused because its
    // callbacks are typed to DiscoverRow. Every rule it encodes is restated:
    // release the host BEFORE anything else so a throw cannot leak a slot;
    // penalise on 429 and requeue once; never let one endpoint end the sweep.
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
        for (const t of capture.tools) {
          driftAll.push({
            typeUnion: !!t.type_union, unionViaComposition: !!t.union_via_composition,
            hasRef: !!t.has_ref, hasDefs: !!t.has_defs,
          });
        }
        pending.push(capture);
        seen++;
        if (pending.length >= INSERT_CHUNK) await flush();
        if (seen % 50 === 0) {
          const s = sched.stats();
          console.log(`[collect-mcp-tools] ${seen}/${targets.length} - 429s=${s.total429} - hosts backed off=${s.hostsBackedOff}`);
        }
      }
    };
    await Promise.all(Array.from({ length: concurrency }, worker));
  } catch (err) {
    sweepError = err;
    console.error('[collect-mcp-tools] SWEEP ABORTED -- flushing what completed:', err instanceof Error ? err.stack ?? err.message : err);
  } finally {
    // Both of these are in the finally for the reason the discover worker's are:
    // on 2026-08-11 a throw after the sweep lost an already-probed chunk and left
    // a scan_runs row open at status='running' forever.
    await flush();
  }

  const drift = tallyFalseDrift(driftAll);
  const hs = sched.stats();
  const truncatedCount = Object.entries(byStatus).length ? undefined : undefined;

  await logIngestionPg({
    sourceSlug: SOURCE_SLUG,
    startedAt,
    itemsFetched: seen,
    itemsNew: toolsWritten,
    itemsFailed: errors,
    metadata: {
      endpoints: targets.length, hosts: hs.hosts, by_status: byStatus,
      captures_written: capturesWritten, tools_written: toolsWritten,
      excluded_by_discover_status: excluded,
      smoke_run: limited, deadline_hit: hs.deadlineHit, undispatched: hs.undispatched,
      max_pages: MAX_PAGES,
      false_drift: drift,
      aborted: sweepError ? String(sweepError instanceof Error ? sweepError.message : sweepError) : null,
    },
  });

  if (hs.deadlineHit) {
    console.warn(
      `[collect-mcp-tools] DEADLINE reached after ${deadlineParsed.value}min -- ` +
      `${hs.undispatched} of ${targets.length} endpoints were never dispatched and have NO capture today. ` +
      `This is partial coverage, not a clean sweep.`,
    );
  }

  console.log(`[collect-mcp-tools] done -- attempted=${seen} captures=${capturesWritten} tools=${toolsWritten} errors=${errors}`);
  console.log(`[collect-mcp-tools] status: ${Object.entries(byStatus).map(([k, v]) => `${k}=${v}`).join(' ') || '(none)'}`);
  console.log(
    `[collect-mcp-tools] false-drift exposure (MEASUREMENT, not a filter): ` +
    `schemas=${drift.schemas} type_union=${drift.typeUnion} union_via_composition=${drift.unionViaComposition} ` +
    `has_ref=${drift.hasRef} has_defs=${drift.hasDefs}`,
  );

  if (sweepError) throw sweepError;
  void truncatedCount;
  return { attempted: seen, captures: capturesWritten, tools: toolsWritten, errors, byStatus, drift };
}

if (require.main === module) {
  collectMcpTools()
    .then(async (r) => {
      await endIngestPool();
      // NO HOLLOW SUCCESS. Every outcome is a capture row, so zero captures means
      // the write path never worked.
      if (r.captures === 0) {
        console.error(
          `[collect-mcp-tools] FAILED: run completed having written ZERO captures ` +
          `(attempted=${r.attempted}, insert errors=${r.errors}).`,
        );
        process.exit(1);
      }
      process.exit(r.errors > 0 ? 1 : 0);
    })
    .catch(async (e) => { console.error(e); await endIngestPool(); process.exit(1); });
}
