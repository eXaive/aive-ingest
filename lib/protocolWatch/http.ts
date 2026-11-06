/**
 * Bounded, polite HTTP for the protocol watch (Phase 2). Every request goes through:
 *   - a HOST ALLOWLIST (api.github.com, datatracker.ietf.org; nothing else, ever);
 *   - lib/net/ssrfGuard guardedFetch (address check on every hop, manual redirects);
 *   - a per-request timeout and a response-size cap;
 *   - per-host pacing (minimum gap between requests to the same host);
 *   - a per-run request budget;
 *   - bounded retry with backoff for 429/5xx/network errors only, honouring Retry-After up to a cap;
 *   - a GitHub rate-limit floor: below it, GitHub requests stop for the run (sources are SKIPPED, not failed).
 * 404/410 are returned to the caller (UNAVAILABLE), never retried and never treated as deletion.
 */
import { guardedFetch } from '../net/ssrfGuard';

export const ALLOWED_HOSTS: ReadonlySet<string> = new Set(['api.github.com', 'datatracker.ietf.org']);
export const REQUEST_TIMEOUT_MS = 15_000;
export const MAX_RESPONSE_BYTES = 1_000_000;
export const HOST_MIN_GAP_MS: Record<string, number> = { 'api.github.com': 750, 'datatracker.ietf.org': 2_000 };
export const RETRY_BACKOFF_MS: readonly number[] = [2_000, 8_000];
export const RETRY_AFTER_CAP_MS = 60_000;
/** Remaining-quota floor. With the Actions token (1,000/hour) keep 100 in reserve; anonymous (60/hour) keeps 10. */
export const GITHUB_RATE_FLOOR = 100;
export const GITHUB_RATE_FLOOR_ANONYMOUS = 10;
export const DEFAULT_REQUEST_BUDGET = 150;
const USER_AGENT = 'AIVE-ProtocolWatch/1.0 (+https://github.com/eXaive/aive-ingest; public metadata only)';

export class WatchHttpError extends Error {
  constructor(readonly code: 'HOST_NOT_ALLOWED' | 'BUDGET_EXHAUSTED' | 'RATE_LIMIT_FLOOR' | 'TIMEOUT' | 'NETWORK' | 'TOO_LARGE' | 'BAD_JSON' | 'SSRF_BLOCKED' | `HTTP_${number}`, readonly status: number | null = null, detail = '') {
    super(`${code}${detail ? `: ${detail}` : ''}`.slice(0, 300));
    this.name = 'WatchHttpError';
  }
}

export interface HttpDeps {
  fetchImpl?: typeof fetch;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
  /** Test-only: extra allowed hosts (loopback fixtures). Never set from the environment. */
  extraHosts?: ReadonlySet<string>;
  /** Test-only: override the scheme+host for allowlisted hosts (loopback fixture server). */
  rewrite?: (url: URL) => URL;
  /** Test-only: shorter request timeout. */
  timeoutMs?: number;
  /** Override the GitHub floor (defaults by token presence). */
  githubRateFloor?: number;
}

export class WatchHttp {
  requests = 0;
  githubStopped = false;
  private lastAt = new Map<string, number>();
  private chain = new Map<string, Promise<void>>();
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly now: () => number;
  constructor(private readonly budget = DEFAULT_REQUEST_BUDGET, private readonly deps: HttpDeps = {}) {
    this.sleep = deps.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
    this.now = deps.now ?? Date.now;
  }

  /** Serialises requests per host and enforces the minimum gap. */
  private async pace(host: string): Promise<void> {
    const prev = this.chain.get(host) ?? Promise.resolve();
    let release!: () => void;
    const next = new Promise<void>((r) => { release = r; });
    this.chain.set(host, prev.then(() => next));
    await prev;
    const gap = HOST_MIN_GAP_MS[host] ?? 2_000;
    const wait = (this.lastAt.get(host) ?? 0) + gap - this.now();
    if (wait > 0) await this.sleep(wait);
    this.lastAt.set(host, this.now());
    release();
  }

  async getJson<T>(href: string, headers: Record<string, string> = {}): Promise<{ status: number; body: T | null; headers: Headers }> {
    const url = new URL(href);
    if (url.protocol !== 'https:' || !(ALLOWED_HOSTS.has(url.hostname) || this.deps.extraHosts?.has(url.host))) throw new WatchHttpError('HOST_NOT_ALLOWED', null, url.host);
    const isGithub = url.hostname === 'api.github.com';
    for (let attempt = 0; ; attempt++) {
      if (isGithub && this.githubStopped) throw new WatchHttpError('RATE_LIMIT_FLOOR');
      if (this.requests >= this.budget) throw new WatchHttpError('BUDGET_EXHAUSTED');
      await this.pace(url.host);
      this.requests++;
      const target = this.deps.rewrite ? this.deps.rewrite(url) : url;
      let res: Response;
      try {
        res = await guardedFetch(target.href, { headers: { 'User-Agent': USER_AGENT, Accept: 'application/json', ...headers }, signal: AbortSignal.timeout(this.deps.timeoutMs ?? REQUEST_TIMEOUT_MS) }, this.deps.fetchImpl);
      } catch (err) {
        const e = err as { name?: string; message?: string };
        if (e?.name === 'SsrfBlockedError') throw new WatchHttpError('SSRF_BLOCKED', null, e.message);
        const timeout = e?.name === 'TimeoutError' || e?.name === 'AbortError';
        if (attempt < RETRY_BACKOFF_MS.length) { await this.sleep(RETRY_BACKOFF_MS[attempt]); continue; }
        throw new WatchHttpError(timeout ? 'TIMEOUT' : 'NETWORK', null, String(e?.message ?? '').slice(0, 120));
      }
      if (isGithub) {
        const remaining = Number(res.headers.get('x-ratelimit-remaining'));
        if (Number.isFinite(remaining) && res.headers.get('x-ratelimit-remaining') !== null && remaining < (this.deps.githubRateFloor ?? (process.env.GITHUB_TOKEN ? GITHUB_RATE_FLOOR : GITHUB_RATE_FLOOR_ANONYMOUS))) this.githubStopped = true;
      }
      const retryable = res.status === 429 || res.status >= 500 || (isGithub && res.status === 403 && res.headers.get('x-ratelimit-remaining') === '0');
      if (retryable) {
        await res.body?.cancel().catch(() => undefined);
        if (isGithub && res.status === 403) { this.githubStopped = true; throw new WatchHttpError('RATE_LIMIT_FLOOR', 403); }
        if (attempt < RETRY_BACKOFF_MS.length) {
          const ra = Number(res.headers.get('retry-after'));
          // A server asking for a longer pause than the cap is respected by NOT retrying in this run.
          if (Number.isFinite(ra) && ra * 1000 > RETRY_AFTER_CAP_MS) throw new WatchHttpError(`HTTP_${res.status}`, res.status, 'retry-after beyond cap');
          const wait = Number.isFinite(ra) && ra > 0 ? ra * 1000 : RETRY_BACKOFF_MS[attempt];
          await this.sleep(wait);
          continue;
        }
        throw new WatchHttpError(`HTTP_${res.status}`, res.status);
      }
      if (res.status === 404 || res.status === 410) { await res.body?.cancel().catch(() => undefined); return { status: res.status, body: null, headers: res.headers }; }
      if (res.status < 200 || res.status >= 300) { await res.body?.cancel().catch(() => undefined); throw new WatchHttpError(`HTTP_${res.status}`, res.status); }
      const declared = Number(res.headers.get('content-length'));
      if (Number.isFinite(declared) && declared > MAX_RESPONSE_BYTES) { await res.body?.cancel().catch(() => undefined); throw new WatchHttpError('TOO_LARGE', res.status); }
      const text = await res.text();
      if (text.length > MAX_RESPONSE_BYTES) throw new WatchHttpError('TOO_LARGE', res.status);
      try { return { status: res.status, body: JSON.parse(text) as T, headers: res.headers }; }
      catch { throw new WatchHttpError('BAD_JSON', res.status); }
    }
  }
}
