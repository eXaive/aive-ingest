/**
 * Outbound request guard for collectors that contact registry-supplied URLs (SSRF protection).
 *
 * WHY (MCP collection audit, 2026-11-05). Endpoint URLs come straight from the public MCP registry, which anyone
 * can publish to. The probe, discover, tools/list and resources/prompts collectors fetched them with no address
 * check, and three of them used redirect: 'follow'. They run on GitHub-hosted runners that hold
 * AIVE_INGEST_DATABASE_URL. A server registered at http://169.254.169.254/... or http://127.0.0.1:5432, or a public
 * URL that redirects there, would make AIVE's runner send requests into its own network and store the answers as
 * public evidence.
 *
 * WHAT IT DOES
 *   1. Only http: and https: URLs.
 *   2. Resolves the host (every A/AAAA record; IP literals are checked directly) and refuses the request if ANY
 *      address is loopback, private, link-local, CGNAT, multicast, documentation, benchmarking, reserved or
 *      unspecified, including IPv4 embedded in IPv6 (mapped, NAT64, 6to4, Teredo).
 *   3. Follows redirects itself (redirect: 'manual'), at most MAX_REDIRECTS, re-running 1-2 on every hop with
 *      fetch's own method rules (303 -> GET; 301/302 POST -> GET; 307/308 keep method and body).
 *
 * RESIDUAL RISK, stated honestly: DNS can change between this check and the connection fetch makes (DNS rebinding,
 * TOCTOU). Closing that needs a connect-time lookup hook (an undici Agent), which is not available without adding a
 * dependency. This guard removes the direct and redirect paths; rebinding needs an attacker-controlled
 * authoritative DNS server racing a sub-second window.
 *
 * Blocked requests throw SsrfBlockedError. `hop === 0` means NO request was made (callers record not_attempted);
 * a later hop means a redirect was refused (callers record an error). The note text starts with "ssrf_blocked:".
 * Uses only node: built-ins. No new dependency.
 */
import { BlockList, isIP } from 'node:net';
import { lookup as dnsLookup } from 'node:dns/promises';

export const MAX_REDIRECTS = 5;

const blocked = new BlockList();
for (const [net, bits] of [
  ['0.0.0.0', 8], ['10.0.0.0', 8], ['100.64.0.0', 10], ['127.0.0.0', 8], ['169.254.0.0', 16], ['172.16.0.0', 12],
  ['192.0.0.0', 24], ['192.0.2.0', 24], ['192.88.99.0', 24], ['192.168.0.0', 16], ['198.18.0.0', 15],
  ['198.51.100.0', 24], ['203.0.113.0', 24], ['224.0.0.0', 4], ['240.0.0.0', 4],
] as const) blocked.addSubnet(net, bits, 'ipv4');
for (const [net, bits] of [
  ['::', 128], ['::1', 128], ['100::', 64], ['2001::', 32], ['2001:db8::', 32], ['2001:10::', 28], ['2002::', 16],
  ['fc00::', 7], ['fe80::', 10], ['fec0::', 10], ['ff00::', 8],
] as const) blocked.addSubnet(net, bits, 'ipv6');

/** IPv4 carried inside an IPv6 address (::ffff:a.b.c.d mapped, 64:ff9b::/96 NAT64), else null. */
function embeddedIPv4(ip: string): string | null {
  const lower = ip.toLowerCase();
  const dotted = lower.match(/^(?:::ffff:|64:ff9b::)(\d{1,3}(?:\.\d{1,3}){3})$/);
  if (dotted) return dotted[1];
  const hex = lower.match(/^(?:::ffff:|64:ff9b::)([0-9a-f]{1,4}):([0-9a-f]{1,4})$/);
  if (hex) {
    const hi = parseInt(hex[1], 16), lo = parseInt(hex[2], 16);
    return `${hi >> 8}.${hi & 255}.${lo >> 8}.${lo & 255}`;
  }
  return null;
}

/** True when `ip` must never be contacted. Unknown or unparseable input is treated as blocked. */
export function isBlockedAddress(ip: string): boolean {
  const bare = ip.replace(/^\[|\]$/g, '').split('%')[0]; // brackets and IPv6 zone ids
  const family = isIP(bare);
  if (family === 4) return blocked.check(bare, 'ipv4');
  if (family === 6) {
    const v4 = embeddedIPv4(bare);
    if (v4) return blocked.check(v4, 'ipv4');
    return blocked.check(bare, 'ipv6');
  }
  return true;
}

export class SsrfBlockedError extends Error {
  readonly hop: number;
  readonly reason: 'scheme' | 'blocked_address' | 'redirect_limit' | 'bad_redirect';
  constructor(reason: SsrfBlockedError['reason'], hop: number, detail: string) {
    super(`ssrf_blocked: ${reason} at hop ${hop}: ${detail}`.slice(0, 300));
    this.name = 'SsrfBlockedError';
    this.reason = reason;
    this.hop = hop;
  }
}

type Lookup = (host: string) => Promise<string[]>;
const systemLookup: Lookup = async (host) => (await dnsLookup(host, { all: true, verbatim: true })).map((a) => a.address);

/** Test-only policy. Set only by verifiers through setSsrfTestPolicy; no environment variable can change it. */
let testPolicy: { lookup?: Lookup; allowLoopback?: boolean } | null = null;
export function setSsrfTestPolicy(policy: { lookup?: Lookup; allowLoopback?: boolean } | null): void { testPolicy = policy; }

const isLoopback = (ip: string) => { const b = ip.replace(/^\[|\]$/g, ''); return b === '::1' || /^127\./.test(b) || b === '::ffff:127.0.0.1'; };

/** Throws SsrfBlockedError for a URL that must not be contacted; DNS errors propagate unchanged (dns_failure). */
export async function assertOutboundAllowed(url: URL, hop = 0): Promise<void> {
  if (url.protocol !== 'http:' && url.protocol !== 'https:') throw new SsrfBlockedError('scheme', hop, url.protocol);
  const host = url.hostname.replace(/^\[|\]$/g, '');
  const addresses = isIP(host) ? [host] : await (testPolicy?.lookup ?? systemLookup)(host);
  if (addresses.length === 0) throw Object.assign(new Error(`getaddrinfo ENOTFOUND ${host}`), { code: 'ENOTFOUND' });
  const bad = addresses.filter((ip) => isBlockedAddress(ip) && !(testPolicy?.allowLoopback && isLoopback(ip)));
  if (bad.length > 0) throw new SsrfBlockedError('blocked_address', hop, `${host} -> ${bad.join(',')}`);
}

/**
 * fetch() with the guard applied to the first request and every redirect hop. `init.redirect` is ignored (always
 * manual). Returns the first non-redirect response, or the last 3xx if it carries no Location header.
 */
export async function guardedFetch(input: string, init: RequestInit = {}, fetchImpl?: typeof fetch): Promise<Response> {
  const doFetch = fetchImpl ?? globalThis.fetch;
  let url = new URL(input);
  let method = (init.method ?? 'GET').toUpperCase();
  let body = init.body;
  for (let hop = 0; ; hop++) {
    await assertOutboundAllowed(url, hop);
    const res = await doFetch(url.href, { ...init, method, body, redirect: 'manual' });
    const location = res.status >= 300 && res.status < 400 ? res.headers.get('location') : null;
    if (!location) return res;
    await res.body?.cancel().catch(() => undefined);
    if (hop >= MAX_REDIRECTS) throw new SsrfBlockedError('redirect_limit', hop + 1, `more than ${MAX_REDIRECTS} redirects`);
    let next: URL;
    try { next = new URL(location, url); } catch { throw new SsrfBlockedError('bad_redirect', hop + 1, location.slice(0, 120)); }
    if (res.status === 303 || ((res.status === 301 || res.status === 302) && method === 'POST')) {
      if (method !== 'HEAD') method = 'GET';
      body = undefined;
    }
    url = next;
  }
}
