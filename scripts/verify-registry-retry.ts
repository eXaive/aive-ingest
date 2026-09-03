/**
 * Proves the retry ladder and the partial-scan handoff against a REAL HTTP
 * server, not a stubbed fetch.
 *
 * WHY A REAL SERVER. The thing that broke on 2026-09-02 was the interaction
 * between a status code, a retry predicate and a `!res.ok` throw. A stubbed
 * fetch would let me assert my own understanding of that interaction rather
 * than the interaction itself. A loopback server exercises the actual Response
 * object, the actual header parsing and the actual control flow.
 *
 * WHAT IS PROVEN
 *   1. A transient 500 is retried and the scan continues  <- the 09-02 failure
 *   2. A 503 with Retry-After honours the header
 *   3. A 429 still retries (the original behaviour is not regressed)
 *   4. A 404 is NOT retried -- asking again cannot fix a bad request
 *   5. Retries exhausted returns a PARTIAL result carrying the pages already
 *      fetched, instead of discarding them
 *
 * Run: npx tsx scripts/verify-registry-retry.ts
 * No database, no network, no credentials.
 */

import assert from 'node:assert/strict';
import http from 'node:http';
import { AddressInfo } from 'node:net';

import { fetchPageWithRetry, fetchAllItems, isRetryableStatus, type ScanProgress } from '../workers/ingest-mcp-registry';

const newProgress = (): ScanProgress => ({
  pages: 0, pagesAttempted: 0, retries: 0, retries429: 0, retries5xx: 0,
  fetchMs: 0, sleepMs: 0, backoffMs: 0, lastCursor: null,
});

/** A server that answers `plan` in order, then 200s forever. */
function server(plan: { status: number; retryAfter?: string }[], body: () => string) {
  let i = 0;
  const requests: string[] = [];
  const s = http.createServer((req, res) => {
    requests.push(req.url ?? '');
    const step = plan[i++];
    if (step && step.status !== 200) {
      if (step.retryAfter) res.setHeader('retry-after', step.retryAfter);
      res.writeHead(step.status, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ title: 'Internal Server Error', status: step.status }));
      return;
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(body());
  });
  return { s, requests, url: () => `http://127.0.0.1:${(s.address() as AddressInfo).port}/` };
}

const listen = (s: http.Server) => new Promise<void>((r) => s.listen(0, '127.0.0.1', () => r()));
const close = (s: http.Server) => new Promise<void>((r) => s.close(() => r()));

async function main(): Promise<void> {
  // ── 0. The predicate itself ───────────────────────────────────────────────
  for (const s of [429, 500, 502, 503, 504]) {
    assert.equal(isRetryableStatus(s), true, `${s} must be retryable`);
  }
  for (const s of [200, 201, 304, 400, 401, 403, 404, 422]) {
    assert.equal(isRetryableStatus(s), false, `${s} must NOT be retried`);
  }
  console.log('  [1/5] retry predicate: 429 + 5xx retried, 4xx and 2xx not');

  // ── 1. A transient 500 is absorbed — the exact 09-02 failure ──────────────
  {
    const { s, requests, url } = server([{ status: 500 }], () => '{"servers":[],"metadata":{}}');
    await listen(s);
    const p = newProgress();
    const t0 = Date.now();
    const res = await fetchPageWithRetry(url(), p);
    const elapsed = Date.now() - t0;
    await close(s);

    assert.equal(res.status, 200, 'a transient 500 must end in a 200, not propagate');
    assert.equal(requests.length, 2, 'the SAME page is re-requested exactly once');
    assert.equal(p.retries, 1);
    assert.equal(p.retries5xx, 1, 'counted as a 5xx retry');
    assert.equal(p.retries429, 0, 'not miscounted as rate limiting');
    assert.ok(elapsed >= 5_000, `first backoff step is 5s (took ${elapsed}ms)`);
    console.log(`  [2/5] transient 500 retried and recovered (${requests.length} requests, ${elapsed}ms)`);
  }

  // ── 2. Retry-After honoured on a 503 ──────────────────────────────────────
  {
    const { s, url } = server([{ status: 503, retryAfter: '1' }], () => '{"servers":[],"metadata":{}}');
    await listen(s);
    const p = newProgress();
    const t0 = Date.now();
    const res = await fetchPageWithRetry(url(), p);
    const elapsed = Date.now() - t0;
    await close(s);

    assert.equal(res.status, 200);
    assert.equal(p.retries5xx, 1);
    // Retry-After: 1 must beat the 5s schedule, or the header is being ignored.
    assert.ok(elapsed < 4_000, `Retry-After: 1 must override the 5s step (took ${elapsed}ms)`);
    console.log(`  [3/5] Retry-After honoured over the backoff schedule (${elapsed}ms)`);
  }

  // ── 3. 429 still retries; 404 still does not ──────────────────────────────
  {
    const a = server([{ status: 429, retryAfter: '1' }], () => '{"servers":[],"metadata":{}}');
    await listen(a.s);
    const pa = newProgress();
    const ra = await fetchPageWithRetry(a.url(), pa);
    await close(a.s);
    assert.equal(ra.status, 200);
    assert.equal(pa.retries429, 1, '429 handling must not regress');
    assert.equal(pa.retries5xx, 0);

    const b = server([{ status: 404 }], () => '{"servers":[],"metadata":{}}');
    await listen(b.s);
    const pb = newProgress();
    const t0 = Date.now();
    const rb = await fetchPageWithRetry(b.url(), pb);
    const elapsed = Date.now() - t0;
    await close(b.s);
    assert.equal(rb.status, 404, 'a 404 is returned immediately');
    assert.equal(b.requests.length, 1, 'a 404 is asked exactly once');
    assert.equal(pb.retries, 0);
    assert.ok(elapsed < 2_000, 'a 404 must not sit through a backoff');
    console.log('  [4/5] 429 retried, 404 failed fast and unretried');
  }

  // ── 4. Exhaustion returns a PARTIAL, keeping the pages already fetched ────
  {
    /* Page 1 succeeds with a nextCursor, then every request 500s. The old code
       threw here and the caller reported fetched:0. The contract now is that
       the one good page comes back with complete:false. */
    let served = 0;
    const s = http.createServer((_req, res) => {
      served++;
      if (served === 1) {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({
          servers: [{ server: { name: 'io.example/one', version: '1.0.0' } }],
          metadata: { nextCursor: 'io.example/one' },
        }));
        return;
      }
      res.writeHead(500, { 'content-type': 'application/json' });
      res.end('{"title":"Internal Server Error","status":500}');
    });
    await listen(s);
    const port = (s.address() as AddressInfo).port;

    const p = newProgress();
    const outcome = await fetchAllItems(p, null, null, `http://127.0.0.1:${port}`);

    await close(s);

    assert.equal(outcome.complete, false, 'the sweep did not finish');
    assert.equal(outcome.items.length, 1, 'THE FETCHED PAGE IS KEPT, not discarded');
    assert.equal(p.pages, 1, 'one page completed');
    assert.ok(outcome.failure?.includes('500'), 'the reason is carried, not swallowed');
    assert.equal(outcome.resumeCursor, 'io.example/one', 'the resume point is recorded');
    assert.equal(p.retries5xx, 3, 'the full ladder was walked before giving up');
    console.log(`  [5/5] exhaustion returned PARTIAL keeping ${outcome.items.length} server(s), resume_cursor=${outcome.resumeCursor}`);
  }

  console.log('MCP registry retry and partial-scan contract: passed');
}

main().catch((e) => { console.error(e); process.exit(1); });
