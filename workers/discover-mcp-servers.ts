/**
 * discover-mcp-servers.ts — one server/discover POST per MCP remote endpoint.
 *
 * WHY A SEPARATE WORKER, not an extension of probe-mcp-endpoints.ts: that
 * worker's fetchOnce is typed `method: 'HEAD' | 'GET'` and its whole contract
 * is "reachability only, no protocol negotiation, no POST". POST is not
 * reachable there without changing that contract, and its daily sweep is
 * load-bearing. Nothing in probe-mcp-endpoints.ts is touched by this file.
 *
 * WHAT IS SENT — exactly one request per endpoint, in the form the spec
 * requires (line anchors at DISCOVER_BODY below; S = schema.ts,
 * T = transports/streamable-http.mdx, both at the 2026-07-28 revision):
 *
 *   POST <endpoint>
 *   Content-Type: application/json
 *   Accept: application/json, text/event-stream       (T:76)
 *   MCP-Protocol-Version: 2026-07-28                  (T:250-259, REQUIRED)
 *   Mcp-Method: server/discover                       (T:286-292, REQUIRED)
 *   {"jsonrpc":"2.0","id":1,"method":"server/discover",
 *    "params":{"_meta":{
 *      "io.modelcontextprotocol/protocolVersion":"2026-07-28",   (S:76, REQUIRED)
 *      "io.modelcontextprotocol/clientInfo":{...},               (S:90, optional)
 *      "io.modelcontextprotocol/clientCapabilities":{}           (S:98, REQUIRED)
 *    }}}
 *
 * server/discover is MANDATORY for servers in revision 2026-07-28 and returns
 * supported protocol versions, capabilities and identity in a single request.
 * There is therefore:
 *   - NO initialize handshake. It was removed in that revision. This worker
 *     never sends `initialize`, and never sends `notifications/initialized`.
 *   - NO Mcp-Session-Id. Sessions were removed (SEP-2567): T:685-686 says a
 *     current server must ignore the header and must not mint or echo session
 *     IDs. It is never sent, and never read from the response by this worker.
 *   - NO Authorization, no cookies, no credentials of any kind.
 *   - NO Mcp-Name: T:286-292 requires it only for tools/call, resources/read
 *     and prompts/get, which this worker never calls.
 *
 * CORRECTION, 2026-08-09 — this worker previously sent a bare
 * {jsonrpc,id,method} with no `params` and neither required header, on the
 * belief that SEP-2575 had REMOVED the MCP-Protocol-Version header. It did not:
 * it MIRRORED the version into the body, and T:250-259 requires the header on
 * every POST and requires it to EQUAL the body's `_meta` value, with a 400
 * HeaderMismatch when they disagree. A conformant server MUST have rejected the
 * old request, so the first sweep would have been ~9,400 rows of
 * discover_status='error'. One constant now feeds the header and the body so
 * they cannot drift apart.
 *
 * WHAT IS WRITTEN — new mcp_endpoint_probes rows (columns from migrations
 * 20260809000012 and …0018, both live):
 *   protocol_versions text[]  the revision strings, VERBATIM
 *   version_source     text   advertised | rejection | NULL — HOW that list was
 *                             obtained, never merged (…0018)
 *   discover_status    text   ok | unsupported | error | session_required |
 *                             not_attempted
 *   discover_raw       jsonb  the full result or error envelope, unmodified
 * plus the reachability columns this request genuinely observed (error_class is
 * NOT NULL and describes the transport, never the protocol).
 *
 * TWO KINDS OF VERSION LIST, and the row says which:
 *   'advertised' — a successful server/discover returned
 *                  DiscoverResult.supportedVersions (S:683). Typed field.
 *   'rejection'  — a 400 UnsupportedProtocolVersionError enumerated the
 *                  server's versions in its message and we parsed them out
 *                  (T:264-266). First-party but weaker — prose, not a field.
 * A version-rejection row keeps discover_status='error' (the call DID fail)
 * while carrying protocol_versions + version_source='rejection'. That
 * combination is intentional, not a bug: the request failed and the response
 * was informative. mcp_server_era bands both kinds identically and exposes
 * version_source so the populations can be reported apart.
 *
 * VERBATIM MEANS VERBATIM. Revision strings are written exactly as received:
 * no trimming, no case folding, no date reformatting, no dropping of values
 * that "look wrong". public.mcp_server_era (migration 20260809000015) guards
 * shape at read time with an ISO-date regex, and a malformed value there
 * classifies 'unknown' rather than inflating a band. Normalising here would
 * destroy the evidence that guard exists to catch. The only bounds applied are
 * on COUNT and BYTES (below) — never on the content of a value.
 *
 * READ-SIDE PREREQUISITE, stated because this worker will otherwise look
 * broken: mcp_server_era takes the LATEST probe row per endpoint. This sweep
 * runs at 03:00Z and the reachability sweep at 07:00Z, so by the time anyone
 * reads the view the latest row is a HEAD row with discover_status NULL, and
 * every server classifies 'unknown'. The view must be changed to select the
 * latest row WITH discover evidence (`WHERE discover_status IS NOT NULL`).
 * That is a migration in eXaive/aive-platform and is NOT part of this repo.
 * The 03:00Z slot is deliberate: it leaves the 07:00Z HEAD row as the newest
 * row overall, so the published reachability verdicts (serverVerdict's
 * latest-error_class read) are not silently re-based onto POST outcomes.
 *
 * NO HOLLOW SUCCESS: a run that completes having written ZERO rows exits
 * non-zero. A worker that reports success while writing nothing is the exact
 * failure mode that let other streams freeze unnoticed; there is no state of
 * the world in which a full sweep of a non-empty corpus legitimately writes no
 * row, because failures are recorded as rows too.
 *
 * Env: AIVE_INGEST_DATABASE_URL (scoped aive_ingest role — pooler host is
 * aws-1-us-east-1, NOT aws-0). Optional: DISCOVER_LIMIT (cap endpoint count
 * for smoke tests; unset = full corpus), DISCOVER_CONCURRENCY (default 8).
 */

import { q, insertRows, endIngestPool, logIngestionPg } from '../lib/ingest/db';
import { parseLimit } from '../lib/ingest/parseLimit';
import {
  classifyException, classifyStatus, parseRetryAfter, type ErrorClass,
} from '../lib/mcp/errorClass';

// Points at the DISCLOSURE PAGE, not at this repo (changed 2026-08-10). An
// operator who finds this string in an access log needs the page that says what
// we send, what we never send, and how to be excluded — not a source tree they
// have to read to work that out. The page carries the repo link for anyone who
// does want the source.
//
// BYTE-IDENTICAL to DISCOVER_USER_AGENT in eXaive/aive-platform
// app/mcp-trust/census/page.tsx, which renders this string as the thing
// operators match against, and the URL must resolve to that same route. If
// either copy changes, change both in the same pass — a disclosure URL that
// 404s is worse than no URL at all.
const USER_AGENT =
  'AIVE-MCP-Discover/1.0 (+https://aive.global/mcp-trust/census; one server/discover POST per endpoint, no auth attempted)';

const SOURCE_SLUG = 'mcp-discover';

const DEFAULT_CONCURRENCY = 8;
const TIMEOUT_MS = 15_000;
const INSERT_CHUNK = 500;

// ── per-host pacing defaults ────────────────────────────────────────────────
// These are the ADAPTIVE floor and ceiling, not a fixed gap. See HostState.
const HOST_BASE_GAP_MS = 150;      // floor: minimum spacing to one host
const HOST_MAX_GAP_MS = 60_000;    // ceiling: a host is never paced slower
const HOST_BACKOFF_FACTOR = 4;     // gap multiplier on a 429
const HOST_DECAY_FACTOR = 0.8;     // gap decay per clean response, toward base
const RETRY_AFTER_CAP_MS = 300_000; // honour Retry-After, but never past 5 min
const MAX_ATTEMPTS = 2;            // one requeue after a 429, then record it

// ── response bounds ─────────────────────────────────────────────────────────
// Bounds on COUNT and BYTES only. No bound alters the content of a value.
const MAX_RAW_BYTES = 65_536;      // discover_raw ceiling
const MAX_VERSIONS = 64;           // protocol_versions element ceiling
const MAX_BODY_BYTES = 262_144;    // stop reading a response body past this

type DiscoverStatus = 'ok' | 'unsupported' | 'error' | 'session_required' | 'not_attempted';

/**
 * How protocol_versions on a row was obtained. NEVER merged — see migration
 * 20260809000018. 'advertised' is a typed spec field
 * (DiscoverResult.supportedVersions, S:683); 'rejection' is parsed out of a 400
 * error message and is weaker evidence. NULL = this row carries no list.
 */
type VersionSource = 'advertised' | 'rejection' | null;

interface DiscoverRow {
  server_id: string;
  endpoint_url: string;
  probed_at: string;
  http_status: number | null;
  response_time_ms: number | null;
  error_class: ErrorClass;
  tls_valid: boolean | null;
  note: string | null;
  probe_method: 'POST' | null;
  content_type: string | null;
  protocol_versions: string[] | null;
  version_source: VersionSource;
  discover_status: DiscoverStatus;
  discover_raw: unknown | null;
}

export interface Item { serverId: string; url: string; host: string; attempts: number }

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ── the JSON-RPC request ────────────────────────────────────────────────────
// Every claim below is anchored to a line in the spec so the next reader can
// VERIFY rather than trust. Source: modelcontextprotocol/modelcontextprotocol
// @ main — schema/2026-07-28/schema.ts (S) and
// docs/specification/2026-07-28/basic/transports/streamable-http.mdx (T).
//
//   S:665-667  interface DiscoverRequest extends JSONRPCRequest {
//                method: "server/discover";
//                params: RequestParams;      <- REQUIRED, no `?`
//              }
//   S:179-180  interface RequestParams { _meta: RequestMetaObject }   <- REQUIRED
//   S:76       "io.modelcontextprotocol/protocolVersion": string;     <- REQUIRED
//              (S:69 documents it: "The MCP Protocol Version being used for
//              this request. Required.")
//   S:98       "io.modelcontextprotocol/clientCapabilities": ClientCapabilities;
//              <- REQUIRED, no `?`. We advertise {} because we implement no
//              client capabilities; that is true, not a placeholder.
//   S:90       "io.modelcontextprotocol/clientInfo"?: Implementation;  <- OPTIONAL,
//              but the schema says clients SHOULD send it on every request, and
//              identifying ourselves is the same courtesy as the User-Agent.
//   T:250-259  "Every POST request to the MCP endpoint **MUST** include an
//              `MCP-Protocol-Version` header. […] The header value **MUST**
//              match the `io.modelcontextprotocol/protocolVersion` field
//              carried in the request body's `_meta`. If the values do not
//              match, the server **MUST** reject the request with
//              `400 Bad Request` and a `HeaderMismatch` JSON-RPC error"
//   T:286-292  Standard Request Headers: `Mcp-Method` mirrors `method` and is
//              REQUIRED for "All requests". `Mcp-Name` mirrors
//              `params.name`/`params.uri` and is required only for tools/call,
//              resources/read and prompts/get — so NOT for server/discover.
//   T:76       the client MUST send an Accept header listing both JSON and SSE.
//
// The first version of this worker sent a bare {jsonrpc,id,method} with no
// params and neither header. A conformant server MUST answer that 400, so the
// whole sweep would have recorded discover_status='error'.
//
// ONE CONSTANT FEEDS BOTH PLACES. The header/_meta agreement in T:250-259 is
// not restated in two literals that could drift — MCP_PROTOCOL_VERSION is
// interpolated into the body and sent as the header, so a HeaderMismatch is
// unrepresentable rather than merely avoided.
const MCP_PROTOCOL_VERSION = '2026-07-28';
const DISCOVER_METHOD = 'server/discover';

const DISCOVER_BODY = JSON.stringify({
  jsonrpc: '2.0',
  id: 1,
  method: DISCOVER_METHOD,
  params: {
    _meta: {
      'io.modelcontextprotocol/protocolVersion': MCP_PROTOCOL_VERSION,
      'io.modelcontextprotocol/clientInfo': { name: 'aive-mcp-discover', version: '1.0.0' },
      'io.modelcontextprotocol/clientCapabilities': {},
    },
  },
});

/** Headers for the discover POST. Exported so the verify script asserts the
 *  exact wire form rather than a copy of it. */
export function discoverHeaders(): Record<string, string> {
  return {
    'Content-Type': 'application/json',
    // T:76 — both media types, because a compliant server may answer either.
    Accept: 'application/json, text/event-stream',
    // T:250-259 — REQUIRED on every POST, and must equal the _meta value above.
    'MCP-Protocol-Version': MCP_PROTOCOL_VERSION,
    // T:286-292 — REQUIRED for all requests; mirrors the JSON-RPC method.
    'Mcp-Method': DISCOVER_METHOD,
    'User-Agent': USER_AGENT,
    // Still absent, deliberately: no Authorization, no cookies, and no
    // Mcp-Session-Id (sessions removed — T:685-686 says a current server must
    // ignore it and must not mint or echo session IDs).
  };
}

export const DISCOVER_WIRE_FORM = { MCP_PROTOCOL_VERSION, DISCOVER_METHOD, DISCOVER_BODY };

/**
 * Candidate locations for the advertised revision list.
 *
 * THE FIRST ENTRY IS THE SPEC'D ONE, looked up rather than guessed:
 *   interface DiscoverResult { supportedVersions: string[]; ... }
 *   — modelcontextprotocol/modelcontextprotocol @ main,
 *     schema/2026-07-28/schema.ts:683
 *     "MCP Protocol Versions this server supports. The client should choose a
 *      version from this list for use in subsequent requests."
 *   Canonical example (schema/2026-07-28/examples/DiscoverResult/
 *   server-capabilities-discovery.json): {"supportedVersions": ["2026-07-28"]}
 *   Note the type is DiscoverResult, NOT ServerDiscoverResult.
 *
 * `supportedVersions` was ABSENT from this list until 2026-08-09, which would
 * have made every conformant server record discover_status='ok' with an EMPTY
 * protocol_versions — indistinguishable at a glance from "advertised nothing",
 * and read by mcp_server_era as 'unknown' for the whole corpus. The fallbacks
 * below are kept anyway, in order, for pre-release servers and for revisions
 * that rename the field: the first path yielding a string or an array of
 * strings wins.
 *
 * If NONE match while the server did return a well-formed JSON-RPC result, the
 * row is still discover_status='ok' with protocol_versions = '{}' — "answered,
 * advertised nothing we could read" — and discover_raw holds the whole result
 * so the list can be re-derived by query, WITHOUT re-probing 9,000 servers.
 * That is the entire reason discover_raw exists (migration 20260809000012). An
 * empty array here is never a claim that the server advertises no version.
 */
/** The spec'd path. A hit here is normal and is deliberately NOT noted. */
const SPEC_VERSION_PATH = 'supportedVersions';

const VERSION_PATHS: { label: string; get: (r: any) => unknown }[] = [
  { label: SPEC_VERSION_PATH,             get: (r) => r?.supportedVersions },   // SPEC: DiscoverResult, 2026-07-28
  { label: 'protocolVersions',            get: (r) => r?.protocolVersions },
  { label: 'protocol_versions',           get: (r) => r?.protocol_versions },
  { label: 'supportedProtocolVersions',   get: (r) => r?.supportedProtocolVersions },
  { label: 'supported_protocol_versions', get: (r) => r?.supported_protocol_versions },
  { label: 'versions',                    get: (r) => r?.versions },
  { label: '_meta.protocolVersions',      get: (r) => r?._meta?.protocolVersions },
  { label: 'protocolVersion',             get: (r) => r?.protocolVersion },
  { label: '_meta.protocolVersion',       get: (r) => r?._meta?.protocolVersion },
];

/** Extract versions VERBATIM. Returns the array and which path matched. */
function extractVersions(result: unknown): { versions: string[]; via: string | null; capped: boolean } {
  for (const p of VERSION_PATHS) {
    const v = p.get(result);
    if (typeof v === 'string') return { versions: [v], via: p.label, capped: false };
    if (Array.isArray(v)) {
      // Strings only — a text[] cannot hold anything else. Non-strings are
      // NOT coerced (coercion would invent a value); they are left behind and
      // remain visible in discover_raw.
      const strs = v.filter((x): x is string => typeof x === 'string');
      if (strs.length > 0) {
        return {
          versions: strs.slice(0, MAX_VERSIONS),
          via: p.label,
          capped: strs.length > MAX_VERSIONS,
        };
      }
    }
  }
  return { versions: [], via: null, capped: false };
}

/**
 * ── versions harvested from a REJECTION ─────────────────────────────────────
 *
 * Pinning MCP-Protocol-Version: 2026-07-28 makes every older server refuse the
 * request, and the spec requires that refusal to be informative: a server not
 * implementing the requested version "MUST respond with `400 Bad Request` and
 * an UnsupportedProtocolVersionError listing its supported versions"
 * (T:264-266). Servers do exactly that — 4 of 7 rejections in the 2026-08-09
 * smoke run enumerated their full range. Those are the pre-2026-07-28
 * population, which is the population an era read most needs; discarding the
 * list filed ~1,500 extrapolated servers under 'no_answer' while the response
 * had just named their whole range.
 *
 * A list obtained this way is recorded with version_source='rejection', NEVER
 * merged with an 'advertised' one — it is first-party but weaker: the wording is
 * per-implementation, the list may be abbreviated, and a prose parse is a prose
 * parse. Migration 20260809000018 carries the column and the reasoning.
 *
 * SHAPES OBSERVED IN THE WILD, all -32600 or -32000, all handled by one regex:
 *   "Unsupported MCP-Protocol-Version: 2026-07-28. Supported versions: 2025-11-25, 2025-06-18"
 *   "Bad Request: Unsupported protocol version: 2026-07-28. Supported versions: 2024-11-05, 2025-03-26, 2025-06-18, 2025-11-25"
 *   "Bad Request: Unsupported protocol version: 2026-07-28 (supported versions: 2025-11-25, 2025-06-18, 2025-03-26, 2024-11-05, 2024-10-07)"
 *
 * DELIBERATELY NOT OVER-FITTED. One anchor — the phrase "supported version(s):"
 * — then split on list separators. The JSON-RPC error CODE is not matched on:
 * two codes were already observed for the same fact, so keying on them would
 * break on the third. If a body does not match, nothing is recorded and
 * discover_raw keeps the whole body for a later re-derivation; a miss is never
 * an empty list.
 *
 * `\bsupported` cannot match inside "Unsupported" (no word boundary between
 * "n" and "s"), so the "Unsupported protocol version: 2026-07-28" clause that
 * precedes the real list is not mistaken for it.
 */
const REJECTION_VERSIONS_RE = /\bsupported\s+versions?\b\s*:\s*([^)\]}\n]+)/i;

/**
 * Servers that refuse for want of a SESSION. Sessions existed in revisions
 * 2025-03-26..2025-11-25 and were removed in 2026-07-28, where a server "MUST
 * ignore [an Mcp-Session-Id header], and [must] not mint or echo session IDs"
 * (T:685-686). So any rejection that turns on a session id dates the sender to
 * that older range — one-way evidence, recorded as discover_status
 * 'session_required' rather than buried in 'error'.
 *
 * Observed: "Bad Request: Missing session ID" and "Bad Request: No valid
 * session ID or initialization request". The rule is deliberately the broad one
 * — a rejection MENTIONING a session id at all — because the specific wording
 * is per-implementation and a 2026-07-28 server has no reason to mention
 * sessions when refusing anything.
 */
const SESSION_DEMAND_RE = /session[\s_-]?id/i;

/** The JSON-RPC error message, whatever shape the error took. One observed body
 *  was `{"error":"Unknown MCP method"}` — a bare string, not an object. */
function errorMessage(envelope: any): string {
  const e = envelope?.error;
  if (typeof e === 'string') return e;
  if (e && typeof e.message === 'string') return e.message;
  if (typeof envelope?.message === 'string') return envelope.message;
  return '';
}

/**
 * Parse a version list out of a rejection message. VERBATIM: whitespace and
 * trailing list punctuation are treated as DELIMITERS, but nothing inside a
 * token is altered — no case folding, no date validation, no reformatting, and
 * nothing dropped for looking wrong. `2024-10-07` turned up in the wild and is
 * not a published revision; it is stored as-is, and mcp_server_era's ISO guard
 * bands it 'unknown'. A server advertising a nonexistent revision is a finding,
 * not noise to be cleaned.
 */
export function versionsFromRejection(message: string): { versions: string[]; capped: boolean } {
  const m = REJECTION_VERSIONS_RE.exec(message);
  if (!m) return { versions: [], capped: false };
  const tokens = m[1]
    .split(/[,;]/)
    // Trim surrounding whitespace, then strip trailing list punctuation
    // (". ) ] }") which delimits the list rather than belonging to a value.
    // Leaving a trailing "." would make an otherwise-valid revision fail the
    // ISO guard — a false 'unknown' caused by our own parsing.
    .map((t) => t.trim().replace(/[.)\]}]+$/, '').trim())
    .filter((t) => t.length > 0);
  return { versions: tokens.slice(0, MAX_VERSIONS), capped: tokens.length > MAX_VERSIONS };
}

/** Does this rejection turn on a missing session? See SESSION_DEMAND_RE. */
export function demandsSession(message: string): boolean {
  return SESSION_DEMAND_RE.test(message);
}

/** Bound discover_raw by BYTES. Over the cap, record the omission honestly. */
function boundRaw(result: unknown): { raw: unknown; note: string | null } {
  let serialized: string;
  try {
    serialized = JSON.stringify(result);
  } catch {
    return { raw: { _aive_unserializable: true }, note: 'discover result was not serializable' };
  }
  if (serialized === undefined) return { raw: null, note: null };
  const bytes = Buffer.byteLength(serialized, 'utf8');
  if (bytes <= MAX_RAW_BYTES) return { raw: result, note: null };
  // Never store a truncated JSON string — it would parse as garbage or fail
  // the jsonb cast. Store a marker that says exactly what was dropped.
  return {
    raw: { _aive_omitted: 'result exceeded MAX_RAW_BYTES', bytes, cap: MAX_RAW_BYTES },
    note: `discover_raw omitted — result was ${bytes} bytes (cap ${MAX_RAW_BYTES})`,
  };
}

/**
 * Read a response body with a hard byte ceiling, so one hostile endpoint
 * cannot stream the runner out of memory. Returns the text read so far.
 */
async function readBounded(res: Response): Promise<string> {
  if (!res.body) return '';
  const reader = res.body.getReader();
  // Streaming decode rather than collecting Buffers: no intermediate array to
  // concatenate, and no dependency on Buffer's shifting @types/node signature.
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
    out += decoder.decode(); // flush any trailing multi-byte sequence
  } finally {
    try { await reader.cancel(); } catch { /* already closed */ }
  }
  return out;
}

/**
 * Parse a JSON-RPC envelope out of a body that may be plain JSON or an SSE
 * stream (a spec-compliant server may answer either, which is why Accept
 * advertises both). For SSE, the first `data:` payload that parses as an
 * object wins. Returns null when nothing parses.
 */
function parseEnvelope(body: string, contentType: string | null): any | null {
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
    } catch { /* keep scanning — a partial frame is not a failure */ }
  }
  return null;
}

// ── per-host adaptive scheduler ─────────────────────────────────────────────
// Designed in, not bolted on. Two facts drive it: the corpus is 7,296 hosts
// but gateway.pipeworx.io alone holds 1,311 endpoints and server.smithery.ai
// 216 — 15.6% of the work on 0.03% of the hosts. So:
//   1. work is queued PER HOST and dispatched round-robin, never as one flat
//      list. A flat list ordered by name would let one mega-host monopolise
//      every worker slot while 7,000 small hosts wait.
//   2. pacing state lives on the HOST, not the request. A 429 raises that
//      host's gap and defers that host; every other host keeps its own pace.
//   3. per-host concurrency is 1. Global concurrency only ever spreads ACROSS
//      hosts, so no host sees parallel requests from this worker at all.
interface HostState {
  host: string;
  queue: Item[];
  gapMs: number;
  nextAt: number;   // epoch ms — earliest a request to this host may start
  busy: boolean;    // per-host concurrency 1
  requests: number;
  n429: number;
  maxGapMs: number;
}

/**
 * Pacing knobs, injectable so they are not hard-coded for testability and so a
 * future tuning pass has one place to change. Defaults are the module
 * constants; scripts/verify-host-backoff.ts lowers the ceiling to keep the
 * synthetic 429 demo fast without changing the logic under test.
 */
export interface HostSchedulerOptions {
  baseGapMs?: number;
  maxGapMs?: number;
  backoffFactor?: number;
  decayFactor?: number;
  retryAfterCapMs?: number;
  /**
   * Wall-clock deadline (epoch ms) after which no NEW work is dispatched.
   *
   * This exists because of a real failure mode, not defensively: a host that
   * 429s every request escalates to the ceiling, and at a 60s gap a mega-host
   * holding 1,311 endpoints would need ~21 hours. Without a deadline the
   * runner is killed by the workflow timeout mid-sweep, which loses the final
   * unflushed chunk and reports nothing about why. With one, the sweep stops
   * dispatching, flushes what it has, and says how many endpoints it never
   * reached. Endpoints not reached simply get no row today — they are never
   * recorded as failures they did not have.
   */
  deadlineAt?: number;
}

export class HostScheduler {
  private hosts: HostState[] = [];
  private byHost = new Map<string, HostState>();
  private cursor = 0;
  private o: Required<Omit<HostSchedulerOptions, 'deadlineAt'>> & { deadlineAt: number };
  pending = 0;
  /** Endpoints never dispatched because the deadline passed. */
  undispatched = 0;
  deadlineHit = false;

  constructor(opts: HostSchedulerOptions = {}) {
    this.o = {
      baseGapMs: opts.baseGapMs ?? HOST_BASE_GAP_MS,
      maxGapMs: opts.maxGapMs ?? HOST_MAX_GAP_MS,
      backoffFactor: opts.backoffFactor ?? HOST_BACKOFF_FACTOR,
      decayFactor: opts.decayFactor ?? HOST_DECAY_FACTOR,
      retryAfterCapMs: opts.retryAfterCapMs ?? RETRY_AFTER_CAP_MS,
      deadlineAt: opts.deadlineAt ?? Number.POSITIVE_INFINITY,
    };
  }

  get baseGapMs(): number { return this.o.baseGapMs; }
  get maxGapMs(): number { return this.o.maxGapMs; }

  add(item: Item): void {
    let s = this.byHost.get(item.host);
    if (!s) {
      s = {
        host: item.host, queue: [], gapMs: this.o.baseGapMs, nextAt: 0,
        busy: false, requests: 0, n429: 0, maxGapMs: this.o.baseGapMs,
      };
      this.byHost.set(item.host, s);
      this.hosts.push(s);
    }
    s.queue.push(item);
    this.pending++;
  }

  /** Requeue after a 429 — front of that host's queue, so it is retried once
   *  the host's raised gap has elapsed rather than at the end of the sweep. */
  requeue(item: Item): void {
    const s = this.byHost.get(item.host);
    if (!s) return;
    s.queue.unshift(item);
    this.pending++;
  }

  /**
   * Atomically reserve the next due item (single event loop — no await between
   * the check and the reservation, so N workers cannot double-book a host).
   * Returns an item, or a wait hint, or null when the sweep is finished.
   */
  claim(now: number): { item: Item; state: HostState } | { waitMs: number } | null {
    if (this.pending === 0) return null;
    // Deadline: stop dispatching, report the shortfall, let the run finish
    // cleanly rather than be killed by the workflow timeout.
    if (now >= this.o.deadlineAt) {
      if (!this.deadlineHit) {
        this.deadlineHit = true;
        this.undispatched = this.pending;
      }
      return null;
    }
    let soonest = Infinity;
    // Round-robin from the cursor so no host is structurally favoured.
    for (let i = 0; i < this.hosts.length; i++) {
      const s = this.hosts[(this.cursor + i) % this.hosts.length];
      if (s.busy || s.queue.length === 0) continue;
      if (s.nextAt <= now) {
        this.cursor = (this.cursor + i + 1) % this.hosts.length;
        const item = s.queue.shift()!;
        this.pending--;
        s.busy = true;
        s.nextAt = now + s.gapMs;
        return { item, state: s };
      }
      if (s.nextAt < soonest) soonest = s.nextAt;
    }
    // Nothing due: either everything is in flight, or every ready host is
    // still cooling down. Sleep to the soonest deadline, bounded so a worker
    // re-evaluates promptly when another finishes.
    const waitMs = soonest === Infinity ? 50 : Math.min(Math.max(soonest - now, 5), 250);
    return { waitMs };
  }

  /** Clean response: relax this host's gap back toward the floor. */
  release(s: HostState, ok: boolean): void {
    s.busy = false;
    s.requests++;
    if (ok) s.gapMs = Math.max(this.o.baseGapMs, Math.round(s.gapMs * this.o.decayFactor));
  }

  /** 429: raise THIS HOST's gap and defer THIS HOST. Never a global stall. */
  penalise(s: HostState, retryAfterMs: number | null, now: number): void {
    s.n429++;
    s.gapMs = Math.min(this.o.maxGapMs, Math.max(this.o.baseGapMs, s.gapMs) * this.o.backoffFactor);
    s.maxGapMs = Math.max(s.maxGapMs, s.gapMs);
    const wait = retryAfterMs !== null ? Math.min(retryAfterMs, this.o.retryAfterCapMs) : s.gapMs;
    s.nextAt = Math.max(s.nextAt, now + wait);
  }

  stats() {
    const paced = this.hosts.filter((h) => h.maxGapMs > this.o.baseGapMs);
    return {
      hosts: this.hosts.length,
      hostsBackedOff: paced.length,
      total429: this.hosts.reduce((a, h) => a + h.n429, 0),
      worstGapMs: this.hosts.reduce((a, h) => Math.max(a, h.maxGapMs), this.o.baseGapMs),
      undispatched: this.undispatched,
      deadlineHit: this.deadlineHit,
      top: [...this.hosts].sort((a, b) => b.requests - a.requests).slice(0, 3)
        .map((h) => `${h.host}=${h.requests}${h.n429 ? `/429x${h.n429}` : ''}`),
    };
  }
}

/**
 * The dispatch loop, extracted so it is testable rather than buried in main().
 * `perform` does one request; `onRow` receives each completed row. The 429 path
 * lives HERE, in one place: penalise the host, release it, requeue the item.
 * scripts/verify-host-backoff.ts drives this exact function against a local
 * server that returns 429, so the behaviour that is demonstrated is the
 * behaviour that ships — not a re-implementation of it in a test.
 */
export async function runSweep(
  sched: HostScheduler,
  concurrency: number,
  perform: (item: Item) => Promise<Outcome>,
  onRow: (row: DiscoverRow) => Promise<void> | void,
  /**
   * Backstop for a `perform` that throws despite its contract. Optional so the
   * two verify scripts that call this with four arguments keep working.
   */
  onThrow?: (item: Item, err: unknown) => void,
): Promise<number> {
  let done = 0;
  const worker = async () => {
    for (;;) {
      const claimed = sched.claim(Date.now());
      if (claimed === null) return;
      if ('waitMs' in claimed) { await sleep(claimed.waitMs); continue; }
      const { item, state } = claimed;
      item.attempts++;

      // SECOND LAYER of per-endpoint isolation. `perform` is contracted never to
      // throw — discoverSafely() in main() guarantees a row for every outcome —
      // and this catch is the backstop for when that contract is broken anyway.
      //
      // Why it has to exist: this await had no guard, so a single endpoint's
      // throw propagated out of the worker, rejected the Promise.all below, and
      // took every other in-flight worker with it. Promise.all rejects on the
      // FIRST failure and abandons the rest — that is how one stalled body ended
      // an 11,006-endpoint sweep at endpoint 1,500.
      //
      // The host is released BEFORE anything else. A throw that skipped
      // sched.release() would leak a permanently-held concurrency slot, and
      // enough of those would wedge the sweep into claiming nothing while
      // looking alive. `false` because a throw is not evidence the host is
      // healthy, so its adaptive gap must not decay on the strength of it.
      let outcome: Outcome;
      try {
        outcome = await perform(item);
      } catch (err) {
        sched.release(state, false);
        onThrow?.(item, err);
        continue;
      }

      if (outcome.kind === 'retry') {
        sched.penalise(state, outcome.retryAfterMs, Date.now());
        sched.release(state, false);
        sched.requeue(item);
        continue;
      }
      // A 429 that has exhausted its retries still raises the host's gap: the
      // signal is about the HOST, so it must outlive the request that saw it.
      if (outcome.row.error_class === 'rate_limited') {
        sched.penalise(state, null, Date.now());
        sched.release(state, false);
      } else {
        sched.release(state, true);
      }

      await onRow(outcome.row);
      done++;
    }
  };
  await Promise.all(Array.from({ length: concurrency }, worker));
  return done;
}

// ── one endpoint ────────────────────────────────────────────────────────────
export type Outcome =
  | { kind: 'row'; row: DiscoverRow }
  | { kind: 'retry'; retryAfterMs: number | null; row: DiscoverRow };

export async function discoverOnce(
  item: Item,
  probeMethodValue: 'POST' | null,
  methodNote: string | null,
): Promise<Outcome> {
  const probedAt = new Date().toISOString();
  const notes: string[] = methodNote ? [methodNote] : [];
  const base: DiscoverRow = {
    server_id: item.serverId, endpoint_url: item.url, probed_at: probedAt,
    http_status: null, response_time_ms: null, error_class: 'other',
    tls_valid: null, note: null, probe_method: probeMethodValue,
    content_type: null, protocol_versions: null, version_source: null,
    discover_status: 'error', discover_raw: null,
  };
  const finish = (r: Partial<DiscoverRow>): DiscoverRow => ({
    ...base, ...r,
    note: notes.length ? notes.join('; ').slice(0, 400) : null,
  });

  // No request is made for these. discover_status='not_attempted' is the
  // documented value for "reached the worker, no request sent" — distinct from
  // NULL, which means the row predates the instrument.
  if (/\{[^}]*\}/.test(item.url)) {
    notes.push('unsubstituted template placeholder — no request made');
    return { kind: 'row', row: finish({ error_class: 'template_placeholder', discover_status: 'not_attempted', probe_method: null }) };
  }
  let parsed: URL;
  try { parsed = new URL(item.url); } catch {
    notes.push(`invalid URL: ${item.url.slice(0, 160)}`);
    return { kind: 'row', row: finish({ error_class: 'other', discover_status: 'not_attempted', probe_method: null }) };
  }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
    notes.push(`non-http(s) scheme ${parsed.protocol} — no request made`);
    return { kind: 'row', row: finish({ error_class: 'other', discover_status: 'not_attempted', probe_method: null }) };
  }
  const isHttps = parsed.protocol === 'https:';

  const t0 = Date.now();
  let res: Response;
  try {
    res = await fetch(parsed.href, {
      method: 'POST',
      redirect: 'follow',
      signal: AbortSignal.timeout(TIMEOUT_MS),
      headers: discoverHeaders(),
      body: DISCOVER_BODY,
    });
  } catch (err) {
    const { cls, tls, raw } = classifyException(err);
    if (cls === 'other') notes.push(raw.slice(0, 200));
    return {
      kind: 'row',
      row: finish({
        response_time_ms: Date.now() - t0, error_class: cls,
        tls_valid: tls ?? (cls === 'tls_error' ? false : null),
        discover_status: 'error',
      }),
    };
  }

  const elapsed = Date.now() - t0;
  const contentType = res.headers.get('content-type')?.slice(0, 256) ?? null;
  const transport = classifyStatus(res.status);
  const shared: Partial<DiscoverRow> = {
    http_status: res.status, response_time_ms: elapsed, error_class: transport,
    tls_valid: isHttps ? true : null, content_type: contentType,
  };

  // 429 — a HOST signal, not a per-request one. Body is not needed.
  if (res.status === 429) {
    const retryAfterMs = parseRetryAfter(res.headers.get('retry-after'), Date.now());
    try { await res.body?.cancel(); } catch { /* none */ }
    const canRetry = item.attempts < MAX_ATTEMPTS;
    notes.push(
      canRetry
        ? '429 — host backed off and this endpoint requeued once'
        : `429 on attempt ${item.attempts} — host backed off, not retried again`,
    );
    const row = finish({ ...shared, discover_status: 'error' });
    return canRetry ? { kind: 'retry', retryAfterMs, row } : { kind: 'row', row };
  }

  // THE ONE AWAIT IN THIS FUNCTION THAT USED TO BE UNGUARDED, and the cause of
  // the 2026-08-11 04:12Z sweep dying after 1,500 of 11,006 endpoints.
  //
  // The AbortSignal.timeout attached to the fetch above is STILL ARMED here.
  // fetch() settles as soon as headers arrive, so an endpoint that answers
  // promptly and then stalls mid-body has its stream aborted at TIMEOUT_MS and
  // reader.read() inside readBounded rejects with DOMException [TimeoutError].
  // readBounded has try/FINALLY and no catch, so that rejection passes straight
  // through it; with nothing here it escaped discoverOnce, escaped the worker
  // loop, rejected Promise.all and killed every in-flight worker. One endpoint
  // out of eleven thousand was enough.
  //
  // The stack for that crash carried no application frames at all — the
  // DOMException is constructed in the abort timer's callback, so it reads as
  // Timeout._onTimeout -> listOnTimeout and points at nothing you can fix. This
  // note exists so the next person to meet that trace need not re-derive it.
  //
  // A stalled body is a transport fact like any other, so it becomes a ROW.
  // classifyException maps TimeoutError/AbortError to 'timeout', already in the
  // CHECK vocabulary, so no schema change is needed. Everything learned from the
  // headers — status, content-type, elapsed — is preserved through `shared`: the
  // response did arrive, only its body never finished.
  let body: string;
  try {
    body = await readBounded(res);
  } catch (err) {
    const { cls, tls, raw } = classifyException(err);
    notes.push(`body read aborted after ${Date.now() - t0}ms: ${raw.slice(0, 160)}`);
    return {
      kind: 'row',
      row: finish({
        ...shared,
        error_class: cls,
        tls_valid: tls ?? (isHttps ? true : null),
        discover_status: 'error',
      }),
    };
  }
  const envelope = parseEnvelope(body, contentType);

  // Transport failed (4xx/5xx). Three distinguishable facts live in here and
  // they are kept apart:
  //   - 405/501/-32601  the endpoint will not take this METHOD  -> 'unsupported'
  //   - a session demand the sender is a 2025-03-26..2025-11-25
  //                      server                                 -> 'session_required'
  //   - anything else                                           -> 'error'
  // A 404 is deliberately NOT 'unsupported': it says the path is wrong, which is
  // a different fact. Independently of status, a rejection that ENUMERATES the
  // server's supported versions is harvested into protocol_versions with
  // version_source='rejection' — the call still failed, so the status stays
  // 'error', but the response was informative and that data is the only era
  // evidence a pre-2026-07-28 server can give when asked at 2026-07-28.
  if (res.status >= 400) {
    const rpcCode = typeof envelope?.error === 'object' ? envelope?.error?.code : undefined;
    const msg = errorMessage(envelope);
    const methodUnsupported = res.status === 405 || res.status === 501 || rpcCode === -32601;
    const sessionDemanded = msg !== '' && demandsSession(msg);
    const { versions, capped } = versionsFromRejection(msg);

    notes.push(`HTTP ${res.status}${rpcCode !== undefined ? ` jsonrpc ${rpcCode}` : ''}`);
    // session_required wins over 'unsupported': it is the more specific and the
    // only era-bearing one of the two. They have not been observed together.
    const status: DiscoverStatus =
      sessionDemanded ? 'session_required' : methodUnsupported ? 'unsupported' : 'error';
    if (sessionDemanded) notes.push('rejection demands a session — pre-2026-07-28 server');
    if (versions.length > 0) notes.push(`${versions.length} version(s) harvested from rejection`);
    if (capped) notes.push(`version list capped at ${MAX_VERSIONS}`);

    const bounded = envelope ? boundRaw(envelope) : { raw: null, note: null };
    if (bounded.note) notes.push(bounded.note);
    return {
      kind: 'row',
      row: finish({
        ...shared,
        discover_status: status,
        protocol_versions: versions.length > 0 ? versions : null,
        version_source: versions.length > 0 ? 'rejection' : null,
        discover_raw: bounded.raw,
      }),
    };
  }

  // 2xx. The transport succeeded, so error_class stays 'ok' whatever the
  // protocol says — the two layers are recorded separately on purpose.
  if (!envelope) {
    notes.push(`2xx with unparseable body (${body.length} chars, ${contentType ?? 'no content-type'})`);
    return { kind: 'row', row: finish({ ...shared, discover_status: 'error' }) };
  }
  // A 2xx carrying a JSON-RPC error. Same three-way split as the 4xx branch —
  // a server MAY answer 200 with an error envelope, and the era-bearing facts
  // are worth the same there as they are behind a 400.
  if (envelope.error) {
    const code = typeof envelope.error === 'object' ? envelope.error?.code : undefined;
    const msg = errorMessage(envelope);
    const methodUnsupported = code === -32601; // JSON-RPC "Method not found"
    const sessionDemanded = msg !== '' && demandsSession(msg);
    const { versions, capped } = versionsFromRejection(msg);

    notes.push(`jsonrpc error ${code ?? '?'}${msg ? `: ${msg.slice(0, 120)}` : ''}`);
    const status: DiscoverStatus =
      sessionDemanded ? 'session_required' : methodUnsupported ? 'unsupported' : 'error';
    if (sessionDemanded) notes.push('rejection demands a session — pre-2026-07-28 server');
    if (versions.length > 0) notes.push(`${versions.length} version(s) harvested from rejection`);
    if (capped) notes.push(`version list capped at ${MAX_VERSIONS}`);

    const bounded = boundRaw(envelope.error);
    if (bounded.note) notes.push(bounded.note);
    return {
      kind: 'row',
      row: finish({
        ...shared,
        discover_status: status,
        protocol_versions: versions.length > 0 ? versions : null,
        version_source: versions.length > 0 ? 'rejection' : null,
        discover_raw: bounded.raw,
      }),
    };
  }

  const result = envelope.result;
  if (result === undefined) {
    notes.push('2xx JSON-RPC envelope with neither result nor error');
    return { kind: 'row', row: finish({ ...shared, discover_status: 'error' }) };
  }

  const { versions, via, capped } = extractVersions(result);
  if (via === null) {
    notes.push('discover answered but no readable version field — see discover_raw');
  } else if (via !== SPEC_VERSION_PATH) {
    // A server answering on a NON-spec path is itself a finding worth keeping.
    // The spec path is not noted, or every conformant row would carry the note.
    notes.push(`versions via non-spec path ${via}`);
  }
  if (capped) notes.push(`version list capped at ${MAX_VERSIONS}`);
  const bounded = boundRaw(result);
  if (bounded.note) notes.push(bounded.note);

  return {
    kind: 'row',
    row: finish({
      ...shared,
      discover_status: 'ok',
      protocol_versions: versions,   // VERBATIM — never normalised
      // A typed spec field (S:683), so this is the strong provenance. Set even
      // when the list is empty is WRONG — an empty list is no list, so
      // version_source stays NULL and never claims a source for nothing.
      version_source: versions.length > 0 ? 'advertised' : null,
      discover_raw: bounded.raw,
    }),
  };
}

// ── scan_runs bookkeeping (same pattern as the other workers) ───────────────
async function openScanRun(startedAt: Date): Promise<string | null> {
  try {
    const rows = await q<{ id: string }>(
      "INSERT INTO scan_runs (started_at, status) VALUES ($1, 'running') RETURNING id",
      [startedAt.toISOString()],
    );
    return rows[0]?.id ?? null;
  } catch (err) {
    console.error('[discover-mcp-servers] scan_runs insert failed:', err instanceof Error ? err.message : err);
    return null;
  }
}

async function closeScanRun(id: string | null, serversReturned: number | null, status: string): Promise<void> {
  if (!id) return;
  try {
    await q(
      'UPDATE scan_runs SET finished_at = now(), pages_fetched = NULL, servers_returned = $2, status = $3 WHERE id = $1',
      [id, serversReturned, status],
    );
  } catch (err) {
    console.error('[discover-mcp-servers] scan_runs update failed:', err instanceof Error ? err.message : err);
  }
}

/**
 * PREFLIGHT: does the live probe_method CHECK admit 'POST'?
 *
 * Migration 20260809000010 constrained probe_method to HEAD|GET, because at
 * the time no worker sent anything else. Rather than write a lie (NULL means
 * "no request was made") or crash 10,000 inserts, this asks the database and
 * adapts: with the widened CHECK the rows record probe_method='POST'; without
 * it they record NULL and carry the method in note, explicitly flagged. The
 * widening is a one-line migration in eXaive/aive-platform.
 */
async function resolveProbeMethod(): Promise<{ value: 'POST' | null; note: string | null }> {
  try {
    const rows = await q<{ def: string }>(
      `SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint
        WHERE conrelid = 'public.mcp_endpoint_probes'::regclass
          AND conname = 'mcp_endpoint_probes_probe_method_check'`,
    );
    const def = rows[0]?.def ?? '';
    if (def === '') {
      console.log("[discover-mcp-servers] preflight: no probe_method CHECK found — recording probe_method='POST'");
      return { value: 'POST', note: null };
    }
    if (def.includes("'POST'")) {
      console.log("[discover-mcp-servers] preflight: CHECK admits POST — recording probe_method='POST'");
      return { value: 'POST', note: null };
    }
    console.warn(
      '[discover-mcp-servers] preflight: probe_method CHECK does NOT admit POST ' +
      `(${def}) — writing probe_method=NULL with the method in note. ` +
      'Widen the CHECK in eXaive/aive-platform to record it properly.',
    );
    return {
      value: null,
      note: 'POST server/discover (probe_method NULL: CHECK does not admit POST)',
    };
  } catch (err) {
    // Cannot read the catalog: choose the option that cannot fail the insert.
    console.warn('[discover-mcp-servers] preflight: could not read probe_method CHECK:',
      err instanceof Error ? err.message : err);
    return { value: null, note: 'POST server/discover (probe_method NULL: CHECK unreadable at preflight)' };
  }
}

// ── main ────────────────────────────────────────────────────────────────────
export async function discoverMcpServers(): Promise<{
  attempted: number; written: number; errors: number;
  byStatus: Record<string, number>; hostStats: ReturnType<HostScheduler['stats']>;
}> {
  const startedAt = new Date();
  const scanId = await openScanRun(startedAt);
  let errors = 0;

  const concurrencyParsed = parseLimit(process.env.DISCOVER_CONCURRENCY, DEFAULT_CONCURRENCY);
  if (!concurrencyParsed.ok || concurrencyParsed.value === null) {
    throw new Error(`DISCOVER_CONCURRENCY unreadable: ${concurrencyParsed.problem}`);
  }
  const concurrency = concurrencyParsed.value;

  const { value: probeMethodValue, note: methodNote } = await resolveProbeMethod();

  // Deleted servers are skipped: their endpoints are expected dead and asking
  // them for a protocol version would pollute the era read with noise.
  const servers = await q<{ id: string; remotes: { url?: string }[] }>(
    "SELECT id, remotes FROM mcp_servers WHERE status != 'deleted' AND remotes IS NOT NULL AND jsonb_array_length(remotes) > 0 ORDER BY name",
  );

  let items: Item[] = [];
  for (const s of servers) {
    for (const r of s.remotes ?? []) {
      if (r && typeof r.url === 'string' && r.url.length > 0) {
        let host = '';
        try { host = new URL(r.url).hostname.toLowerCase(); } catch { host = `__unparseable__${r.url.slice(0, 40)}`; }
        items.push({ serverId: s.id, url: r.url, host, attempts: 0 });
      }
    }
  }

  const limitParsed = parseLimit(process.env.DISCOVER_LIMIT, Number.MAX_SAFE_INTEGER);
  if (!limitParsed.ok || limitParsed.value === null) {
    throw new Error(`DISCOVER_LIMIT unreadable: ${limitParsed.problem}`);
  }
  let limited = false;
  if (items.length > limitParsed.value) {
    console.log(`[discover-mcp-servers] DISCOVER_LIMIT=${limitParsed.value} — discovering first ${limitParsed.value} of ${items.length} endpoints (SMOKE RUN, NOT a full sweep)`);
    items = items.slice(0, limitParsed.value);
    limited = true;
  }

  // Deadline sits UNDER the workflow's timeout-minutes (180) so the run ends on
  // its own terms and flushes, instead of being killed with a chunk in hand.
  const deadlineParsed = parseLimit(process.env.DISCOVER_DEADLINE_MINUTES, 150);
  if (!deadlineParsed.ok || deadlineParsed.value === null) {
    throw new Error(`DISCOVER_DEADLINE_MINUTES unreadable: ${deadlineParsed.problem}`);
  }
  const deadlineAt = startedAt.getTime() + deadlineParsed.value * 60_000;

  const sched = new HostScheduler({ deadlineAt });
  for (const it of items) sched.add(it);
  console.log(
    `[discover-mcp-servers] ${servers.length} servers → ${items.length} endpoints across ${sched.stats().hosts} hosts; ` +
    `concurrency ${concurrency}, per-host gap ${sched.baseGapMs}ms adaptive to ${sched.maxGapMs}ms, ` +
    `deadline ${deadlineParsed.value}min`,
  );

  const COLS = [
    'server_id', 'endpoint_url', 'probed_at', 'http_status', 'response_time_ms',
    'error_class', 'tls_valid', 'note', 'probe_method', 'content_type',
    'protocol_versions', 'version_source', 'discover_status', 'discover_raw',
  ];
  const byStatus: Record<string, number> = {};
  let written = 0;
  let pending: DiscoverRow[] = [];

  const flush = async () => {
    if (pending.length === 0) return;
    const chunk = pending;
    pending = [];
    try {
      await insertRows('mcp_endpoint_probes', COLS, chunk.map((r) => [
        r.server_id, r.endpoint_url, r.probed_at, r.http_status, r.response_time_ms,
        r.error_class, r.tls_valid, r.note, r.probe_method, r.content_type,
        // text[] goes through node-pg as a JS array (it maps to a Postgres
        // array literal correctly); jsonb MUST be pre-serialized per the
        // lib/ingest/db contract or node-pg mangles it into a record literal.
        r.protocol_versions,
        r.version_source,
        r.discover_status,
        r.discover_raw === null || r.discover_raw === undefined ? null : JSON.stringify(r.discover_raw),
      ]));
      written += chunk.length;
    } catch (err) {
      errors++;
      console.error(`[discover-mcp-servers] insert chunk (${chunk.length} rows) failed:`, err instanceof Error ? err.message : err);
    }
  };

  let seen = 0;
  let thrown = 0;

  const hostOf = (url: string): string => {
    try { return new URL(url).hostname.toLowerCase(); } catch { return '(unparseable)'; }
  };

  /**
   * FIRST LAYER of per-endpoint isolation, and the contract runSweep's backstop
   * relies on: THIS NEVER THROWS.
   *
   * discoverOnce already classifies every transport outcome into a row itself.
   * Anything that still escapes it is a defect in this worker rather than a fact
   * about the endpoint — and it becomes a row anyway, so it is visible in the
   * DATA and not only in a log line that scrolls past.
   *
   * error_class is 'other' because the CHECK vocabulary on
   * mcp_endpoint_probes.error_class admits nine values and has none for "the
   * prober itself threw"; 'other' is the vocabulary's designated slot for an
   * unmatched failure whose raw text goes in note. The note carries a stable
   * `probe_exception:` prefix so the class is queryable without a schema change:
   *
   *     select endpoint_url, note from mcp_endpoint_probes
   *      where note like 'probe_exception:%';
   *
   * A dedicated error_class would need a migration in eXaive/aive-platform to
   * widen that CHECK. One is written and NOT applied — see the report.
   */
  const discoverSafely = async (item: Item): Promise<Outcome> => {
    try {
      return await discoverOnce(item, probeMethodValue, methodNote);
    } catch (err) {
      thrown++;
      const msg = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
      // THE INSTRUMENTATION THE 2026-08-11 POST-MORTEM DID NOT HAVE. The endpoint
      // that ended that sweep wrote no row and emitted no log line, so it was
      // unidentifiable from every surviving artefact — the DB, the run log and
      // the stack all pointed nowhere. URL and host, on every throw, always.
      console.error(
        `[discover-mcp-servers] endpoint THREW (recorded as a row, sweep continues) ` +
        `host=${hostOf(item.url)} url=${item.url} attempts=${item.attempts} — ${msg}`,
      );
      if (err instanceof Error && err.stack) console.error(err.stack);
      return {
        kind: 'row',
        row: {
          server_id: item.serverId, endpoint_url: item.url,
          probed_at: new Date().toISOString(),
          http_status: null, response_time_ms: null,
          error_class: 'other', tls_valid: null,
          note: `probe_exception: ${msg}`.slice(0, 400),
          probe_method: probeMethodValue, content_type: null,
          protocol_versions: null, version_source: null,
          discover_status: 'error', discover_raw: null,
        },
      };
    }
  };

  // `done` and the abort are declared out here so the finally below can report
  // them whether the sweep finished or died partway.
  let done = 0;
  let sweepError: unknown = null;
  const hostStatsAtEnd = () => sched.stats();

  try {
    done = await runSweep(
      sched,
      concurrency,
      discoverSafely,
      async (row) => {
        byStatus[row.discover_status] = (byStatus[row.discover_status] ?? 0) + 1;
        pending.push(row);
        seen++;
        if (pending.length >= INSERT_CHUNK) await flush();
        if (seen % 500 === 0) {
          const s = sched.stats();
          console.log(`[discover-mcp-servers] ${seen}/${items.length} · 429s=${s.total429} · hosts backed off=${s.hostsBackedOff} · worst gap=${s.worstGapMs}ms`);
        }
      },
      (item, err) => {
        // Reached only if discoverSafely's own contract is broken. Logged
        // separately from the row-producing path so the two are never confused.
        console.error(
          `[discover-mcp-servers] BACKSTOP: perform() threw despite its contract ` +
          `host=${hostOf(item.url)} url=${item.url} — ` +
          `${err instanceof Error ? `${err.name}: ${err.message}` : String(err)} ` +
          `(no row written for this endpoint)`,
        );
      },
    );
  } catch (err) {
    sweepError = err;
    console.error(
      '[discover-mcp-servers] SWEEP ABORTED — flushing what completed and closing the run:',
      err instanceof Error ? err.stack ?? err.message : err,
    );
  } finally {
    // BOTH OF THESE USED TO SIT AFTER runSweep WITH NOTHING PROTECTING THEM.
    // On 2026-08-11 that cost a 500-row partial chunk that had already been
    // probed, and left scan_runs 2680f1c2 at status='running' with finished_at
    // NULL — a run that reads as still in progress to anything checking
    // freshness, rather than as the failure it was. In a finally, a throw still
    // banks completed work and still closes the books on it.
    await flush();
    const hs = hostStatsAtEnd();
    const statusStr = Object.entries(byStatus).sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k}=${v}`).join(' ');
    // `done` is only assigned when runSweep RETURNS, so on an abort it is still 0
    // while rows have plainly been written — the injected-fault test produced
    // "attempted=0 written=8", which reads as an impossibility. `seen` counts
    // every probe that reached onRow, so it is the honest figure on both paths
    // and equals `done` on a clean run.
    const attempted = sweepError ? seen : done;
    const status =
      `${sweepError ? 'failed' : errors > 0 ? 'error' : 'ok'} attempted=${attempted} written=${written} ${statusStr}` +
      `${thrown > 0 ? ` thrown=${thrown}` : ''}` +
      `${limited ? ' SMOKE' : ''}${hs.deadlineHit ? ` DEADLINE undispatched=${hs.undispatched}` : ''}` +
      `${sweepError ? ` ABORTED: ${sweepError instanceof Error ? `${sweepError.name}: ${sweepError.message}` : String(sweepError)}` : ''}`;
    await closeScanRun(scanId, written, status.slice(0, 500));
  }

  // A sweep that aborted is a failed run, not a short one. Rethrow AFTER the
  // finally has flushed and closed, so the workflow still exits non-zero.
  if (sweepError) throw sweepError;

  const hostStats = sched.stats();
  const statusStr = Object.entries(byStatus).sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k}=${v}`).join(' ');

  // A partial sweep is stated, never implied. Silent truncation would read as
  // full coverage on the next panel that counts these rows.
  if (hostStats.deadlineHit) {
    console.warn(
      `[discover-mcp-servers] DEADLINE reached after ${deadlineParsed.value}min — ` +
      `${hostStats.undispatched} of ${items.length} endpoints were never dispatched and have NO row today. ` +
      `This is partial coverage, not a clean sweep.`,
    );
  }

  await logIngestionPg({
    sourceSlug: SOURCE_SLUG,
    startedAt,
    itemsFetched: done,
    itemsNew: written,
    itemsFailed: errors,
    metadata: {
      endpoints: items.length, hosts: hostStats.hosts, by_status: byStatus,
      total_429: hostStats.total429, hosts_backed_off: hostStats.hostsBackedOff,
      worst_gap_ms: hostStats.worstGapMs, smoke_run: limited,
      deadline_hit: hostStats.deadlineHit, undispatched: hostStats.undispatched,
      // endpoints whose probe threw and were recorded as probe_exception rows
      endpoints_thrown: thrown,
      probe_method: probeMethodValue ?? 'NULL (CHECK does not admit POST)',
    },
  });

  console.log(`[discover-mcp-servers] done — attempted=${done} written=${written} errors=${errors} thrown=${thrown}`);
  console.log(`[discover-mcp-servers] status breakdown: ${statusStr || '(none)'}`);
  console.log(`[discover-mcp-servers] hosts=${hostStats.hosts} backed_off=${hostStats.hostsBackedOff} 429s=${hostStats.total429} worst_gap=${hostStats.worstGapMs}ms busiest=${hostStats.top.join(' ')}`);

  return { attempted: done, written, errors, byStatus, hostStats };
}

// ── standalone runner (also the GHA workflow entrypoint) ────────────────────
if (require.main === module) {
  discoverMcpServers()
    .then(async (r) => {
      await endIngestPool();
      // NO HOLLOW SUCCESS. A completed sweep that wrote nothing is a failure,
      // not a quiet success: every outcome — including every error — is
      // recorded as a row, so zero rows means the write path never worked.
      if (r.written === 0) {
        console.error(
          `[discover-mcp-servers] FAILED: run completed having written ZERO rows ` +
          `(attempted=${r.attempted}, insert errors=${r.errors}). Exiting non-zero — ` +
          `a sweep that writes nothing is never a success.`,
        );
        process.exit(1);
      }
      process.exit(r.errors > 0 ? 1 : 0);
    })
    .catch(async (e) => { console.error(e); await endIngestPool(); process.exit(1); });
}
