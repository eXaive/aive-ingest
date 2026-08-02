/**
 * probe-mcp-endpoints.ts — reachability probes for MCP remote endpoints.
 *
 * Reads every non-deleted mcp_servers row with at least one remote endpoint
 * and probes each endpoint URL. One mcp_endpoint_probes row per probe.
 * REACHABILITY ONLY, by design:
 *   - HEAD first, GET only when HEAD returns 405
 *   - no credentials of any kind are sent (no Authorization, no cookies,
 *     nothing beyond User-Agent/Accept)
 *   - no MCP protocol negotiation is attempted (no POST, no initialize)
 *   - at most 2 redirects followed; the final target is recorded
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

// Ephemeral tunnel hosts: probed normally, flagged in note (a tunnel that is
// up today proves nothing about tomorrow — the flag lets readers segment).
const TUNNEL_HOST_SUFFIXES = [
  'trycloudflare.com', 'ngrok.io', 'ngrok-free.app', 'ngrok.app', 'ngrok.dev',
  'loca.lt', 'serveo.net', 'localhost.run',
];

type ErrorClass =
  | 'dns_failure' | 'connection_refused' | 'timeout' | 'tls_error'
  | 'rate_limited' | 'http_error' | 'ok' | 'template_placeholder' | 'other';

interface ProbeRow {
  server_id: string;
  endpoint_url: string;
  probed_at: string;
  http_status: number | null;
  response_time_ms: number | null;
  error_class: ErrorClass;
  tls_valid: boolean | null;
  redirect_target: string | null;
  note: string | null;
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

async function probeEndpoint(serverId: string, url: string): Promise<ProbeRow> {
  const probedAt = new Date().toISOString();
  const base: ProbeRow = {
    server_id: serverId, endpoint_url: url, probed_at: probedAt,
    http_status: null, response_time_ms: null, error_class: 'other',
    tls_valid: null, redirect_target: null, note: null,
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

  while (true) {
    await paceHost(current.hostname);
    const t0 = Date.now();
    let res: Response;
    try {
      res = await fetchOnce(current.href, 'HEAD');
      // HEAD → GET fallback only on 405 Method Not Allowed.
      if (res.status === 405) {
        await paceHost(current.hostname);
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
    };
  }
}

// ── scan_runs bookkeeping (same pattern as the four ingest workers) ─────────
async function openScanRun(startedAt: Date): Promise<string | null> {
  try {
    const rows = await q<{ id: string }>(
      "INSERT INTO scan_runs (started_at, status) VALUES ($1, 'running') RETURNING id",
      [startedAt.toISOString()],
    );
    return rows[0]?.id ?? null;
  } catch (err) {
    console.error('[probe-mcp-endpoints] scan_runs insert failed:', err instanceof Error ? err.message : err);
    return null;
  }
}

async function closeScanRun(id: string | null, fields: { pages_fetched: number | null; servers_returned: number | null; status: string }): Promise<void> {
  if (!id) return;
  try {
    await q(
      'UPDATE scan_runs SET finished_at = now(), pages_fetched = $2, servers_returned = $3, status = $4 WHERE id = $1',
      [id, fields.pages_fetched, fields.servers_returned, fields.status],
    );
  } catch (err) {
    console.error('[probe-mcp-endpoints] scan_runs update failed:', err instanceof Error ? err.message : err);
  }
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
export async function probeMcpEndpoints(): Promise<{
  probed: number; written: number; skippedDeletedServers: number; errors: number;
  byClass: Record<string, number>;
}> {
  const startedAt = new Date();
  const scanId = await openScanRun(startedAt);
  let errors = 0;

  // Deleted servers are skipped entirely (their endpoints are expected dead;
  // probing them would pollute the reachability signal). Count reported.
  const skippedRows = await q<{ n: string }>(
    "SELECT count(*) AS n FROM mcp_servers WHERE status = 'deleted' AND remotes IS NOT NULL AND jsonb_array_length(remotes) > 0",
  );
  const skippedDeletedServers = Number(skippedRows[0]?.n ?? 0);

  const servers = await q<{ id: string; remotes: { url?: string }[] }>(
    "SELECT id, remotes FROM mcp_servers WHERE status != 'deleted' AND remotes IS NOT NULL AND jsonb_array_length(remotes) > 0 ORDER BY name",
  );

  let items: { serverId: string; url: string }[] = [];
  for (const s of servers) {
    for (const r of s.remotes ?? []) {
      if (r && typeof r.url === 'string' && r.url.length > 0) items.push({ serverId: s.id, url: r.url });
    }
  }

  const limitParsed = parseLimit(process.env.PROBE_LIMIT, Number.MAX_SAFE_INTEGER);
  if (!limitParsed.ok || limitParsed.value === null) {
    throw new Error(`PROBE_LIMIT unreadable: ${limitParsed.problem}`);
  }
  if (items.length > limitParsed.value) {
    console.log(`[probe-mcp-endpoints] PROBE_LIMIT=${limitParsed.value} — probing first ${limitParsed.value} of ${items.length} endpoints (smoke run, NOT a full scan)`);
    items = items.slice(0, limitParsed.value);
  }

  console.log(`[probe-mcp-endpoints] ${servers.length} servers → ${items.length} endpoints; ${skippedDeletedServers} deleted servers skipped`);

  // Results flush to the DB progressively (every INSERT_CHUNK completed
  // probes), same posture as the ingest workers' during-run chunk writes: a
  // process death mid-sweep keeps everything probed so far instead of losing
  // the hour. Insert failures are run errors (red run), never dropped silently.
  const COLS = ['server_id', 'endpoint_url', 'probed_at', 'http_status', 'response_time_ms', 'error_class', 'tls_valid', 'redirect_target', 'note'];
  const byClass: Record<string, number> = {};
  let written = 0;
  let pending: ProbeRow[] = [];

  const flush = async () => {
    if (pending.length === 0) return;
    const chunk = pending;
    pending = [];
    try {
      await insertRows('mcp_endpoint_probes', COLS, chunk.map((r) => [
        r.server_id, r.endpoint_url, r.probed_at, r.http_status, r.response_time_ms,
        r.error_class, r.tls_valid, r.redirect_target, r.note,
      ]));
      written += chunk.length;
    } catch (err) {
      errors++;
      console.error(`[probe-mcp-endpoints] insert chunk (${chunk.length} rows) failed:`, err instanceof Error ? err.message : err);
    }
  };

  let cursor = 0;
  let done = 0;
  const workerLoop = async () => {
    while (true) {
      const i = cursor++;
      if (i >= items.length) return;
      const row = await probeEndpoint(items[i].serverId, items[i].url);
      byClass[row.error_class] = (byClass[row.error_class] ?? 0) + 1;
      pending.push(row);
      done++;
      if (pending.length >= INSERT_CHUNK) await flush();
      if (done % 500 === 0) console.log(`[probe-mcp-endpoints] ${done}/${items.length} probed`);
    }
  };
  await Promise.all(Array.from({ length: CONCURRENCY }, workerLoop));
  await flush();
  const classStr = Object.entries(byClass).sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k}=${v}`).join(' ');
  const status = `${errors > 0 ? 'error' : 'ok'} probed=${items.length} ${classStr} skipped_deleted_servers=${skippedDeletedServers}`;
  await closeScanRun(scanId, { pages_fetched: null, servers_returned: written, status });

  console.log(`[probe-mcp-endpoints] done — probed=${items.length} written=${written} errors=${errors}`);
  console.log(`[probe-mcp-endpoints] breakdown: ${classStr}`);
  return { probed: items.length, written, skippedDeletedServers, errors, byClass };
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
