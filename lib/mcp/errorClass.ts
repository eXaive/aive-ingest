/**
 * lib/mcp/errorClass.ts — the mcp_endpoint_probes.error_class vocabulary and
 * the exception classifier, extracted so more than one worker can write the
 * column without either one guessing.
 *
 * error_class is a CHECK-constrained column (nine values, migration
 * 20260802000001) and is NOT NULL. It records what happened at the TRANSPORT
 * layer for the request this row describes — never a protocol judgement. A
 * server that answers "method not found" is error_class='ok': the network did
 * its job. Protocol outcomes live in discover_status (migration
 * 20260809000012).
 *
 * DELIBERATE DUPLICATION, stated so it is not mistaken for an oversight:
 * workers/probe-mcp-endpoints.ts carries its own copy of this classifier.
 * That file is intentionally NOT modified by the change that added this
 * module — it is the load-bearing daily reachability sweep and its request
 * surface is spec'd HEAD/GET-only. A follow-up may switch it to this import;
 * until then the two copies must be kept in step, and this file is the one to
 * change first.
 */

export type ErrorClass =
  | 'dns_failure' | 'connection_refused' | 'timeout' | 'tls_error'
  | 'rate_limited' | 'http_error' | 'ok' | 'template_placeholder' | 'other';

const TLS_CODE_HINTS = [
  'ERR_TLS', 'ERR_SSL', 'UNABLE_TO_VERIFY', 'CERT_', 'DEPTH_ZERO_SELF_SIGNED',
  'SELF_SIGNED_CERT', 'HOSTNAME_MISMATCH', 'ERR_OSSL',
];

/**
 * Classify a thrown fetch error. Anything unmatched returns 'other' WITH the
 * raw string, so no failure is silently swallowed — the caller is expected to
 * record `raw` in note.
 */
export function classifyException(err: unknown): {
  cls: ErrorClass;
  tls: boolean | null;
  raw: string;
} {
  const e = err as {
    name?: string; code?: string; message?: string;
    cause?: { code?: string; message?: string };
  };
  const code = e?.cause?.code ?? e?.code ?? '';
  const raw = `${code} ${e?.cause?.message ?? e?.message ?? String(err)}`.trim();

  if (
    e?.name === 'TimeoutError' || e?.name === 'AbortError' ||
    code === 'ETIMEDOUT' || code === 'UND_ERR_CONNECT_TIMEOUT' ||
    code === 'UND_ERR_HEADERS_TIMEOUT' || code === 'UND_ERR_BODY_TIMEOUT'
  ) {
    return { cls: 'timeout', tls: null, raw };
  }
  if (code === 'ENOTFOUND' || code === 'EAI_AGAIN') return { cls: 'dns_failure', tls: null, raw };
  if (code === 'ECONNREFUSED') return { cls: 'connection_refused', tls: null, raw };
  if (code === 'ECONNRESET' || code === 'EPIPE' || code === 'UND_ERR_SOCKET') {
    return { cls: 'connection_refused', tls: null, raw };
  }
  if (TLS_CODE_HINTS.some((h) => code.includes(h)) || /certificate|tls|ssl/i.test(raw)) {
    return { cls: 'tls_error', tls: false, raw };
  }
  return { cls: 'other', tls: null, raw };
}

/** Transport class for a response that ARRIVED. 4xx/5xx is a transport success. */
export function classifyStatus(status: number): ErrorClass {
  if (status === 429) return 'rate_limited';
  if (status >= 400) return 'http_error';
  return 'ok';
}

/**
 * Retry-After, in ms, or null. Accepts both forms the header allows: a
 * delta-seconds integer and an HTTP-date. Never returns a negative value;
 * an unparseable header returns null rather than 0, so the caller falls back
 * to its own backoff instead of treating junk as "retry immediately".
 */
export function parseRetryAfter(value: string | null, nowMs: number): number | null {
  if (!value) return null;
  const trimmed = value.trim();
  if (/^\d+$/.test(trimmed)) return Number(trimmed) * 1000;
  const at = Date.parse(trimmed);
  if (Number.isNaN(at)) return null;
  return Math.max(0, at - nowMs);
}
