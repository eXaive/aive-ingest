/**
 * probe-mcp-endpoints.ts — reachability probes for MCP remote endpoints.
 *
 * Reads every non-deleted mcp_servers row with at least one remote endpoint
 * and probes each endpoint URL. One mcp_endpoint_probes row per probe.
 * REACHABILITY ONLY, by design:
 *   - HEAD first, GET only when HEAD returns 405
 *   - no credentials of any kind are sent (no Authorization, no cookies,
 *     nothing beyond User-Agent/Accept)
 *   - no MCP protocol negotiation is attempted (no POST, no initialize,
 *     no server/discover)
 *   - at most 2 redirects followed; the final target is recorded
 *
 * Protocol evidence (2026-08-09): each row also records probe_method (what
 * was actually sent: HEAD, or GET after a 405 — never POST), content_type of
 * the terminal response, and response_headers holding ONLY the five-header
 * allowlist in HEADER_ALLOWLIST below. Passive capture from responses the
 * probe already receives — the request surface is unchanged.
 *
 * Identification: requests carry a User-Agent naming AIVE with a contact URL
 * (see USER_AGENT below). Concurrency is capped at 8 with a minimum 100ms
 * gap between requests to the same host; a 429 is recorded as
 * error_class='rate_limited' and backs the host off — never retried hard.
 *
 * error_class is RECORDED at probe time, never inferred later:
 *   dns_failure | connection_refused | timeout | tls_error | rate_limited |
 *   http_error | ok | template_placeholder | other
 * Anything unmatched records 'other' with the raw error string truncated to
 * 200 chars in note — no error is silently swallowed. URLs containing an
 * unsubstituted template placeholder ({host} etc.) record
 * error_class='template_placeholder' WITHOUT a network call. Ephemeral
 * tunnel hosts (trycloudflare.com and similar) are probed normally but
 * flagged in note.
 *
 * Env: AIVE_INGEST_DATABASE_URL (scoped aive_ingest role). Optional:
 * PROBE_FRESHNESS_HOURS (tripwire window, default 48), PROBE_LIMIT
 * (cap endpoint count for smoke tests; unset = full corpus).
 */

import { q, insertRows, endIngestPool } from '../lib/ingest/db';
import { parseLimit } from '../lib/ingest/parseLimit';

const USER_AGENT =
  'AIVE-MCP-EndpointProbe/1.0 (+https://github.com/eXaive/aive-ingest; reachability check only, no auth attempted)';

const CONCURRENCY = 8;          // hard cap — spec'd, do not raise
const MIN_HOST_GAP_MS = 100;    // minimum spacing between requests to one host
const RATE_LIMIT_BACKOFF_MS = 5_000; // extra host spacing after a 429
const TIMEOUT_MS = 10_000;
const MAX_REDIRECTS = 2;
const INSERT_CHUNK = 500;
export const PROBE_VANTAGE_ID = 'github-actions-default';
export const COLLECTOR_ENVIRONMENT = 'github_actions';
export const PROBE_POLICY_VERSION = 'mcp-reachability-v1';
export const ERROR_TAXONOMY_VERSION = 'mcp-reachability-errors-v1';

// Ephemeral tunnel hosts: probed normally, flagged in note (a tunnel that is
// up today proves nothing about tomorrow — the flag lets readers segment).
const TUNNEL_HOST_SUFFIXES = [
  'trycloudflare.com', 'ngrok.io', 'ngrok-free.app', 'ngrok.app', 'ngrok.dev',
  'loca.lt', 'serveo.net', 'localhost.run',
];

type ErrorClass =
  | 'dns_failure' | 'connection_refused' | 'timeout' | 'tls_error'
  | 'rate_limited' | 'http_error' | 'ok' | 'template_placeholder' | 'other';

// response_headers allowlist — ONLY these five are ever stored, lowercase.
//   server / www-authenticate : infra + auth-scheme identification
//   x-accel-buffering         : SSE-behind-proxy signal
//   mcp-protocol-version      : infra identification and corroboration only —
//                               NOT era evidence. Revision 2026-07-28 REQUIRES
//                               this header on every POST, so its presence
//                               dates a server not at all (migration
//                               20260809000017, which retracted the earlier
//                               claim in …0013 that it did).
//   mcp-session-id            : ONE-WAY era evidence. Sessions were removed in
//                               2026-07-28 and a conformant server must ignore
//                               the header and never mint or echo one, so a
//                               response carrying it is running an older
//                               revision. Presence proves that; ABSENCE PROVES
//                               NOTHING.
// Arbitrary headers are NEVER stored: bounded row size, no accidental
// capture of anything reflective or sensitive. Values truncated to 256.
const HEADER_ALLOWLIST = [
  'server', 'www-authenticate', 'x-accel-buffering',
  'mcp-protocol-version', 'mcp-session-id',
] as const;
const HEADER_VALUE_MAX = 256;

function pickAllowlistedHeaders(h: Headers): Record<string, string> | null {
  const out: Record<string, string> = {};
  for (const name of HEADER_ALLOWLIST) {
    const v = h.get(name);
    if (v !== null) out[name] = v.slice(0, HEADER_VALUE_MAX);
  }
  return Object.keys(out).length > 0 ? out : null;
}

export interface ProbeRow {
  server_id: string;
  endpoint_url: string;
  probed_at: string;
  http_status: number | null;
  response_time_ms: number | null;
  error_class: ErrorClass;
  tls_valid: boolean | null;
  redirect_target: string | null;
  note: string | null;
  // Protocol evidence. probe_method is the method of the attempt this row
  // records ('HEAD' | 'GET', even when that attempt threw); null = no
  // request was made (template placeholder / invalid URL / non-http scheme).
  probe_method: 'HEAD' | 'GET' | null;
  content_type: string | null;
  response_headers: Record<string, string> | null;
}

// ── per-host pacing gate ─────────────────────────────────────────────────────
// Reserves the next allowed slot for a host atomically (single event loop),
// so 8 concurrent workers never violate the per-host minimum gap.
const hostGate = new Map<string, number>();
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function paceHost(host: string, extraMs = 0): Promise<void> {
  const now = Date.now();
  const next = hostGate.get(host) ?? 0;
  const start = Math.max(now, next);
  hostGate.set(host, start + MIN_HOST_GAP_MS + extraMs);
  if (start > now) await sleep(start - now);
}

function backoffHost(host: string, ms: number): void {
  hostGate.set(host, Math.max(hostGate.get(host) ?? 0, Date.now() + ms));
}

// ── error classification ─────────────────────────────────────────────────────
const TLS_CODE_HINTS = [
  'ERR_TLS', 'ERR_SSL', 'UNABLE_TO_VERIFY', 'CERT_', 'DEPTH_ZERO_SELF_SIGNED',
  'SELF_SIGNED_CERT', 'HOSTNAME_MISMATCH', 'ERR_OSSL',
];

function classifyException(err: unknown): { cls: ErrorClass; tls: boolean | null; raw: string } {
  const e = err as { name?: string; code?: string; message?: string; cause?: { code?: string; message?: string } };
  const code = e?.cause?.code ?? e?.code ?? '';
  const raw = `${code} ${e?.cause?.message ?? e?.message ?? String(err)}`.trim();
  if (e?.name === 'TimeoutError' || e?.name === 'AbortError' || code === 'ETIMEDOUT' || code === 'UND_ERR_CONNECT_TIMEOUT' || code === 'UND_ERR_HEADERS_TIMEOUT') {
    return { cls: 'timeout', tls: null, raw };
  }
  if (code === 'ENOTFOUND' || code === 'EAI_AGAIN') return { cls: 'dns_failure', tls: null, raw };
  if (code === 'ECONNREFUSED') return { cls: 'connection_refused', tls: null, raw };
  if (TLS_CODE_HINTS.some((h) => code.includes(h)) || /certificate|tls|ssl/i.test(raw)) {
    return { cls: 'tls_error', tls: false, raw };
  }
  return { cls: 'other', tls: null, raw };
}

// ── single-endpoint probe ────────────────────────────────────────────────────
async function fetchOnce(url: string, method: 'HEAD' | 'GET') {
  const res = await fetch(url, {
    method,
    redirect: 'manual',
    signal: AbortSignal.timeout(TIMEOUT_MS),
    headers: { 'User-Agent': USER_AGENT, Accept: '*/*' },
  });
  // Drain nothing: we only need status + headers. Cancel any body.
  try { await res.body?.cancel(); } catch { /* already consumed/none */ }
  return res;
}

export async function probeEndpoint(serverId: string, url: string): Promise<ProbeRow> {
  const probedAt = new Date().toISOString();
  const base: ProbeRow = {
    server_id: serverId, endpoint_url: url, probed_at: probedAt,
    http_status: null, response_time_ms: null, error_class: 'other',
    tls_valid: null, redirect_target: null, note: null,
    probe_method: null, content_type: null, response_headers: null,
  };

  // Template placeholders: recorded, never fetched.
  if (/\{[^}]*\}/.test(url)) {
    return { ...base, error_class: 'template_placeholder', note: 'unsubstituted template placeholder — no network call made' };
  }

  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return { ...base, error_class: 'other', note: `invalid URL: ${url.slice(0, 160)}` };
  }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
    return { ...base, error_class: 'other', note: `non-http(s) scheme ${parsed.protocol} — not probed` };
  }

  const tunnel = TUNNEL_HOST_SUFFIXES.find((s) => parsed.hostname === s || parsed.hostname.endsWith('.' + s));
  const notes: string[] = tunnel ? [`ephemeral tunnel host (${tunnel})`] : [];
  const isHttps = parsed.protocol === 'https:';

  let current = parsed;
  let redirects = 0;
  let redirectTarget: string | null = null;
  // Method of the most recent attempt — recorded even when the attempt
  // throws (probe_method is evidence of what we SENT, not what answered).
  let method: 'HEAD' | 'GET' = 'HEAD';

  // Passive evidence from a response the probe already holds. Terminal
  // responses only — callers below invoke this exactly where they record
  // http_status, so evidence always describes the same response as the row.
  const evidence = (res: Response) => ({
    probe_method: method,
    content_type: res.headers.get('content-type')?.slice(0, HEADER_VALUE_MAX) ?? null,
    response_headers: pickAllowlistedHeaders(res.headers),
  });

  while (true) {
    await paceHost(current.hostname);
    const t0 = Date.now();
    let res: Response;
    try {
      method = 'HEAD';
      res = await fetchOnce(current.href, 'HEAD');
      // HEAD → GET fallback only on 405 Method Not Allowed.
      if (res.status === 405) {
        await paceHost(current.hostname);
        method = 'GET';
        res = await fetchOnce(current.href, 'GET');
        notes.push('HEAD returned 405; probed with GET');
      }
    } catch (err) {
      const { cls, tls, raw } = classifyException(err);
      if (cls === 'other') notes.push(raw.slice(0, 200));
      return {
        ...base, response_time_ms: Date.now() - t0, error_class: cls,
        tls_valid: tls ?? (cls === 'tls_error' ? false : null),
        redirect_target: redirectTarget, note: notes.length ? notes.join('; ').slice(0, 400) : null,
        probe_method: method,
      };
    }
    const elapsed = Date.now() - t0;

    // Redirect: follow at most MAX_REDIRECTS, recording the final target.
    if (res.status >= 300 && res.status < 400 && res.headers.get('location')) {
      const loc = new URL(res.headers.get('location')!, current.href);
      redirectTarget = loc.href;
      if (redirects < MAX_REDIRECTS) {
        redirects++;
        current = loc;
        continue;
      }
      // Redirect budget exhausted: record the last 3xx as the outcome.
      return {
        ...base, http_status: res.status, response_time_ms: elapsed,
        error_class: 'ok', tls_valid: isHttps ? true : null,
        redirect_target: redirectTarget,
        note: [...notes, `redirect budget (${MAX_REDIRECTS}) exhausted — not followed further`].join('; ').slice(0, 400),
        ...evidence(res),
      };
    }

    // Terminal response.
    let cls: ErrorClass;
    if (res.status === 429) {
      cls = 'rate_limited';
      backoffHost(current.hostname, RATE_LIMIT_BACKOFF_MS);
      notes.push(`429 — host backed off ${RATE_LIMIT_BACKOFF_MS}ms, not retried`);
    } else if (res.status >= 400) {
      cls = 'http_error';
    } else {
      cls = 'ok';
    }
    return {
      ...base, http_status: res.status, response_time_ms: elapsed, error_class: cls,
      tls_valid: (isHttps || current.protocol === 'https:') ? true : null,
      redirect_target: redirectTarget,
      note: notes.length ? notes.join('; ').slice(0, 400) : null,
      ...evidence(res),
    };
  }
}

// ── scan_runs bookkeeping (same pattern as the four ingest workers) ─────────
async function openScanRun(startedAt: Date, query: typeof q = q): Promise<string | null> {
  try {
    const rows = await query<{ id: string }>(
      "INSERT INTO scan_runs (started_at, status) VALUES ($1, 'running') RETURNING id",
      [startedAt.toISOString()],
    );
    return rows[0]?.id ?? null;
  } catch (err) {
    console.error('[probe-mcp-endpoints] scan_runs insert failed:', err instanceof Error ? err.message : err);
    return null;
  }
}

async function closeScanRun(id: string | null, fields: { pages_fetched: number | null; servers_returned: number | null; status: string }, query: typeof q = q): Promise<void> {
  if (!id) return;
  try {
    await query(
      'UPDATE scan_runs SET finished_at = now(), pages_fetched = $2, servers_returned = $3, status = $4 WHERE id = $1',
      [id, fields.pages_fetched, fields.servers_returned, fields.status],
    );
  } catch (err) {
    console.error('[probe-mcp-endpoints] scan_runs update failed:', err instanceof Error ? err.message : err);
  }
}

type ProbeRunState = 'RUNNING' | 'COMPLETE' | 'PARTIAL' | 'FAILED' | 'CANCELLED';

export interface ProbeRunProvenance {
  scheduledFor: string | null;
  triggerKind: 'SCHEDULED' | 'MANUAL';
  collectorVersion: string;
  externalWorkflowRunId: string | null;
  externalWorkflowRunAttempt: number | null;
}

export interface ProbeExecutionDependencies {
  query: typeof q;
  insert: typeof insertRows;
  probe: typeof probeEndpoint;
  now: () => Date;
}

const defaultDependencies: ProbeExecutionDependencies = {
  query: q,
  insert: insertRows,
  probe: probeEndpoint,
  now: () => new Date(),
};

function provenanceFromEnvironment(): ProbeRunProvenance {
  const trigger = process.env.AIVE_PROBE_TRIGGER_KIND === 'SCHEDULED' ? 'SCHEDULED' : 'MANUAL';
  const attemptRaw = process.env.AIVE_PROBE_WORKFLOW_RUN_ATTEMPT;
  const attempt = attemptRaw && /^\d+$/.test(attemptRaw) && Number(attemptRaw) > 0 ? Number(attemptRaw) : null;
  const scheduledRaw = process.env.AIVE_PROBE_SCHEDULED_FOR?.trim();
  const scheduledFor = scheduledRaw && Number.isFinite(Date.parse(scheduledRaw)) ? new Date(scheduledRaw).toISOString() : null;
  return {
    scheduledFor,
    triggerKind: trigger,
    collectorVersion: process.env.AIVE_PROBE_COLLECTOR_VERSION?.trim() || 'local-unversioned',
    externalWorkflowRunId: process.env.AIVE_PROBE_WORKFLOW_RUN_ID?.trim() || null,
    externalWorkflowRunAttempt: attempt,
  };
}

async function openProbeRun(
  startedAt: Date,
  provenance: ProbeRunProvenance,
  query: typeof q,
): Promise<string> {
  const rows = await query<{ id: string }>(
    `INSERT INTO mcp_probe_runs (
       started_at, scheduled_for, trigger_kind, state,
       probe_vantage_id, collector_environment, collector_version,
       probe_policy_version, error_taxonomy_version,
       external_workflow_run_id, external_workflow_run_attempt
     ) VALUES ($1,$2,$3,'RUNNING',$4,$5,$6,$7,$8,$9,$10) RETURNING id`,
    [
      startedAt.toISOString(), provenance.scheduledFor, provenance.triggerKind,
      PROBE_VANTAGE_ID, COLLECTOR_ENVIRONMENT, provenance.collectorVersion,
      PROBE_POLICY_VERSION, ERROR_TAXONOMY_VERSION,
      provenance.externalWorkflowRunId, provenance.externalWorkflowRunAttempt,
    ],
  );
  const id = rows[0]?.id;
  if (!id) throw new Error('mcp_probe_runs insert returned no run id; refusing to probe without durable run identity');
  return id;
}

type ProbeRunCounts = {
  expected: number; eligible: number; attempted: number; completed: number;
  persisted: number; notAttempted: number; failedInternal: number; excluded: number;
};

async function updateProbeRun(
  id: string,
  state: ProbeRunState,
  counts: ProbeRunCounts,
  failureCode: string | null,
  query: typeof q,
): Promise<void> {
  await query(
    `UPDATE mcp_probe_runs SET
       completed_at = CASE WHEN $2 = 'RUNNING' THEN NULL ELSE now() END,
       state = $2,
       expected_endpoint_count = $3,
       eligible_endpoint_count = $4,
       attempted_endpoint_count = $5,
       completed_endpoint_count = $6,
       persisted_endpoint_count = $7,
       not_attempted_endpoint_count = $8,
       failed_internal_count = $9,
       excluded_endpoint_count = $10,
       failure_code = $11
     WHERE id = $1`,
    [id, state, counts.expected, counts.eligible, counts.attempted, counts.completed,
      counts.persisted, counts.notAttempted, counts.failedInternal, counts.excluded, failureCode],
  );
}

// ── freshness tripwire (same pattern as the ingest workers) ─────────────────
const FRESHNESS_PARSED = parseLimit(process.env.PROBE_FRESHNESS_HOURS, 48);

export async function assertProbeFreshness(): Promise<void> {
  if (!FRESHNESS_PARSED.ok || FRESHNESS_PARSED.value === null) {
    throw new Error(`PROBE_FRESHNESS_HOURS unreadable: ${FRESHNESS_PARSED.problem}`);
  }
  const rows = await q<{ newest: string | null }>('SELECT max(probed_at)::text AS newest FROM mcp_endpoint_probes');
  const newestMs = rows[0]?.newest ? Date.parse(rows[0].newest) : 0;
  const ageHours = (Date.now() - newestMs) / 3_600_000;
  if (ageHours > FRESHNESS_PARSED.value) {
    throw new Error(
      `mcp_endpoint_probes stale: newest probe ${rows[0]?.newest ?? 'NONE'} is ${ageHours.toFixed(1)}h old (limit ${FRESHNESS_PARSED.value}h)`,
    );
  }
  console.log(`[probe-mcp-endpoints] freshness ok — newest probe ${ageHours.toFixed(1)}h old`);
}

// ── main ─────────────────────────────────────────────────────────────────────
export async function probeMcpEndpoints(
  dependencies: ProbeExecutionDependencies = defaultDependencies,
  provenance: ProbeRunProvenance = provenanceFromEnvironment(),
): Promise<{
  probed: number; written: number; skippedDeletedServers: number; errors: number;
  byClass: Record<string, number>; probeRunId: string;
}> {
  const startedAt = dependencies.now();
  // Mandatory and fail-closed: no endpoint selection or network activity may
  // begin until the authoritative run identity is durable.
  const probeRunId = await openProbeRun(startedAt, provenance, dependencies.query);
  const scanId = await openScanRun(startedAt, dependencies.query);
  let errors = 0;
  const counts: ProbeRunCounts = {
    expected: 0, eligible: 0, attempted: 0, completed: 0,
    persisted: 0, notAttempted: 0, failedInternal: 0, excluded: 0,
  };

  try {

  // Deleted servers are skipped entirely (their endpoints are expected dead;
  // probing them would pollute the reachability signal). Count reported.
  const skippedRows = await dependencies.query<{ n: string }>(
    "SELECT count(*) AS n FROM mcp_servers WHERE status = 'deleted' AND remotes IS NOT NULL AND jsonb_array_length(remotes) > 0",
  );
  const skippedDeletedServers = Number(skippedRows[0]?.n ?? 0);

  const servers = await dependencies.query<{ id: string; remotes: { url?: string }[] }>(
    "SELECT id, remotes FROM mcp_servers WHERE status != 'deleted' AND remotes IS NOT NULL AND jsonb_array_length(remotes) > 0 ORDER BY name",
  );

  let items: { serverId: string; url: string }[] = [];
  /* DE-DUP ON (server_id, url) -- the effective unique key within a run, since
     probe_run_id and observation_kind are constant across it.

     A server may declare ONE url under several transports: 87 of the 88
     collisions on run #36 (2026-09-05) were the same address listed as both
     "sse" and "streamable-http". Transport is neither probed nor stored (it is
     absent from ProbeRow and COLS), so the second probe issues an identical
     request and produces an identical row. Skipping it removes redundant
     network work, not evidence. */
  const seen = new Set<string>();
  let duplicateEndpoints = 0;
  for (const s of servers) {
    for (const r of s.remotes ?? []) {
      if (!r || typeof r.url !== 'string' || r.url.length === 0) continue;
      // NUL separator: cannot occur in a uuid or a url, so no key can collide
      // across a boundary the way a "/" or "|" separator could.
      const key = `${s.id}\u0000${r.url}`;
      if (seen.has(key)) { duplicateEndpoints++; continue; }
      seen.add(key);
      items.push({ serverId: s.id, url: r.url });
    }
  }
  if (duplicateEndpoints > 0) {
    console.log(`[probe-mcp-endpoints] ${duplicateEndpoints} duplicate (server,url) endpoint(s) collapsed — same address declared under multiple transports`);
  }

  counts.expected = items.length;
  const limitParsed = parseLimit(process.env.PROBE_LIMIT, Number.MAX_SAFE_INTEGER);
  if (!limitParsed.ok || limitParsed.value === null) {
    throw new Error(`PROBE_LIMIT unreadable: ${limitParsed.problem}`);
  }
  if (items.length > limitParsed.value) {
    console.log(`[probe-mcp-endpoints] PROBE_LIMIT=${limitParsed.value} — probing first ${limitParsed.value} of ${items.length} endpoints (smoke run, NOT a full scan)`);
    items = items.slice(0, limitParsed.value);
  }
  counts.eligible = items.length;
  counts.excluded = counts.expected - counts.eligible;
  await updateProbeRun(probeRunId, 'RUNNING', counts, null, dependencies.query);

  console.log(`[probe-mcp-endpoints] ${servers.length} servers → ${items.length} endpoints; ${skippedDeletedServers} deleted servers skipped`);

  // Results flush to the DB progressively (every INSERT_CHUNK completed
  // probes), same posture as the ingest workers' during-run chunk writes: a
  // process death mid-sweep keeps everything probed so far instead of losing
  // the hour. Insert failures are run errors (red run), never dropped silently.
  const COLS = ['server_id', 'endpoint_url', 'probed_at', 'http_status', 'response_time_ms', 'error_class', 'tls_valid', 'redirect_target', 'note', 'probe_method', 'content_type', 'response_headers', 'probe_run_id', 'observation_kind'];
  const byClass: Record<string, number> = {};
  let written = 0;
  let pending: ProbeRow[] = [];

  const flush = async () => {
    if (pending.length === 0) return;
    const chunk = pending;
    pending = [];
    try {
      const inserted = await dependencies.insert('mcp_endpoint_probes', COLS, chunk.map((r) => [
        r.server_id, r.endpoint_url, r.probed_at, r.http_status, r.response_time_ms,
        r.error_class, r.tls_valid, r.redirect_target, r.note,
        // jsonb pre-serialized per lib/ingest/db contract (node-pg would
        // otherwise mangle objects into Postgres array/record literals).
        r.probe_method, r.content_type,
        r.response_headers ? JSON.stringify(r.response_headers) : null,
        probeRunId, 'REACHABILITY',
      /* Column list AND predicate, both copied from the arbiter index in
         aive-platform migration 20260904150000_mcp_probe_run_evidence.sql:
           CREATE UNIQUE INDEX mcp_endpoint_probes_run_endpoint_kind_unique
             ON public.mcp_endpoint_probes
                (probe_run_id, server_id, endpoint_url, observation_kind)
             WHERE probe_run_id IS NOT NULL;
         The index is PARTIAL, so the predicate is not decoration -- without it
         Postgres cannot infer the index and the whole statement fails at plan
         time with 42P10, which is how run #37 persisted nothing. */
      ]), 'probe_run_id, server_id, endpoint_url, observation_kind', 'probe_run_id IS NOT NULL');
      /* Count what LANDED, not what was sent. DO NOTHING makes those differ,
         and persisted feeds the COMPLETE/PARTIAL run state -- crediting a
         skipped row would report a lossy run as complete.

         On run #36 (2026-09-05) 88 duplicate rows aborted 17 whole 500-row
         chunks, discarding 8,300 good rows for 88 bad ones. DO NOTHING caps
         that blast radius at the offending row. */
      written += inserted;
      counts.persisted += inserted;
      if (inserted < chunk.length) {
        /* Deliberate: a collision surviving the dedup above is genuinely
           unexpected, so it counts as failedInternal and the run reports
           PARTIAL rather than COMPLETE. A false COMPLETE on a lossy run is
           exactly what the run-state machinery exists to catch. */
        counts.failedInternal += chunk.length - inserted;
        console.error(`[probe-mcp-endpoints] ${chunk.length - inserted} row(s) in a ${chunk.length}-row chunk hit the run-unique index and were skipped`);
      }
    } catch (err) {
      errors++;
      counts.failedInternal += chunk.length;
      console.error(`[probe-mcp-endpoints] insert chunk (${chunk.length} rows) failed:`, err instanceof Error ? err.message : err);
    }
  };

  let cursor = 0;
  let done = 0;
  const workerLoop = async () => {
    while (true) {
      const i = cursor++;
      if (i >= items.length) return;
      const row = await dependencies.probe(items[i].serverId, items[i].url);
      byClass[row.error_class] = (byClass[row.error_class] ?? 0) + 1;
      if (row.probe_method === null) counts.notAttempted++;
      else counts.attempted++;
      pending.push(row);
      done++;
      counts.completed++;
      if (pending.length >= INSERT_CHUNK) await flush();
      if (done % 500 === 0) console.log(`[probe-mcp-endpoints] ${done}/${items.length} probed`);
    }
  };
  await Promise.all(Array.from({ length: CONCURRENCY }, workerLoop));
  await flush();
  const classStr = Object.entries(byClass).sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k}=${v}`).join(' ');
  const status = `${errors > 0 ? 'error' : 'ok'} probed=${items.length} ${classStr} skipped_deleted_servers=${skippedDeletedServers}`;
  const runState: ProbeRunState = counts.persisted === counts.completed
    ? 'COMPLETE'
    : counts.persisted > 0 ? 'PARTIAL' : 'FAILED';
  await updateProbeRun(probeRunId, runState, counts, errors > 0 ? 'PROBE_ROW_PERSISTENCE_FAILED' : null, dependencies.query);
  await closeScanRun(scanId, { pages_fetched: null, servers_returned: written, status }, dependencies.query);

  console.log(`[probe-mcp-endpoints] done — probed=${items.length} written=${written} errors=${errors}`);
  console.log(`[probe-mcp-endpoints] breakdown: ${classStr}`);
  return { probed: items.length, written, skippedDeletedServers, errors, byClass, probeRunId };
  } catch (err) {
    // Any completed rows not already persisted or attributed to a failed
    // insert are censored collector losses (for example an unexpected worker
    // exception with rows still pending in memory).
    counts.failedInternal = counts.completed - counts.persisted;
    const terminalState: ProbeRunState = counts.persisted > 0 ? 'PARTIAL' : 'FAILED';
    try {
      await updateProbeRun(probeRunId, terminalState, counts, 'COLLECTOR_ABORTED', dependencies.query);
    } catch (closeErr) {
      console.error('[probe-mcp-endpoints] failed to finalize authoritative run:', closeErr instanceof Error ? closeErr.message : closeErr);
    }
    await closeScanRun(scanId, {
      pages_fetched: null,
      servers_returned: counts.persisted,
      status: `error probed=${counts.completed} collector_aborted`,
    }, dependencies.query);
    throw err;
  }
}

// ── standalone runner (also the GHA daily-workflow entrypoint) ──────────────
if (require.main === module) {
  probeMcpEndpoints()
    .then(async (r) => {
      await assertProbeFreshness();
      await endIngestPool();
      process.exit(r.errors > 0 ? 1 : 0);
    })
    .catch(async (e) => { console.error(e); await endIngestPool(); process.exit(1); });
}
