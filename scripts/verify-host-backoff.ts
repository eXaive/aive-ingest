/**
 * scripts/verify-host-backoff.ts — prove per-host adaptive pacing against a
 * SYNTHETIC 429. No database, no network beyond loopback.
 *
 * Runs the SHIPPED code paths — HostScheduler, runSweep and discoverOnce from
 * workers/discover-mcp-servers.ts — against a local HTTP server, so what is
 * demonstrated is what runs in production rather than a re-implementation.
 *
 * Two hostnames are used to prove the backoff is scoped to a HOST and not
 * global: 127.0.0.1 and localhost resolve to the same loopback server but are
 * distinct hostnames, so the scheduler treats them as two hosts.
 *   127.0.0.1  -> /throttled : always 429, with Retry-After
 *   localhost  -> /ok        : always a valid server/discover result
 *
 * PASSES when: the throttled host's gap escalates and its 429s are counted, the
 * clean host stays at the base gap, and the clean host's requests still complete
 * (i.e. one host being throttled does not stall the sweep).
 *
 * Run: npx tsx scripts/verify-host-backoff.ts
 */

import http from 'node:http';
import {
  HostScheduler, runSweep, discoverOnce, DISCOVER_WIRE_FORM, type Item,
} from '../workers/discover-mcp-servers';

const THROTTLED_HOST = '127.0.0.1';
const CLEAN_HOST = 'localhost';
const RETRY_AFTER_SECONDS = 1;

let throttledHits = 0;
let cleanHits = 0;

/** First request seen on the clean host, captured OFF THE WIRE so the wire form
 *  is asserted rather than a copy of the constants that produced it. */
let captured: { method: string; headers: http.IncomingHttpHeaders; body: string } | null = null;

const server = http.createServer((req, res) => {
  if (req.url === '/ok' && captured === null) {
    // setEncoding + string concat: no Buffer.concat, whose @types/node
    // signature shifts between versions (same reason as readBounded).
    req.setEncoding('utf8');
    let body = '';
    req.on('data', (c: string) => { body += c; });
    req.on('end', () => {
      captured = { method: req.method ?? '', headers: req.headers, body };
    });
  }
  if (req.url === '/throttled') {
    throttledHits++;
    res.writeHead(429, { 'Content-Type': 'application/json', 'Retry-After': String(RETRY_AFTER_SECONDS) });
    res.end(JSON.stringify({ jsonrpc: '2.0', id: 1, error: { code: -32000, message: 'slow down' } }));
    return;
  }
  if (req.url === '/ok') {
    cleanHits++;
    res.writeHead(200, { 'Content-Type': 'application/json' });
    // Deliberately mixed shape: one well-formed revision and one malformed
    // value, to show the worker stores BOTH verbatim and leaves the shape
    // judgement to public.mcp_server_era's ISO-date guard.
    res.end(JSON.stringify({
      jsonrpc: '2.0', id: 1,
      result: { protocolVersions: ['2026-07-28', 'garbage'], serverInfo: { name: 'verify-fixture', version: '0.0.0' } },
    }));
    return;
  }
  res.writeHead(404); res.end();
});

function fail(msg: string): never {
  console.error(`\nFAIL: ${msg}`);
  process.exit(1);
}

async function main() {
  await new Promise<void>((r) => server.listen(0, r));
  const addr = server.address();
  if (!addr || typeof addr === 'string') fail('could not bind loopback server');
  const port = addr.port;

  // Same logic, lower ceiling. Production escalates ×4 to a 60s gap, which for
  // a host that 429s every request is correct and slow; the test caps at 400ms
  // so escalation and host-scoping are still proven in seconds. Only the knobs
  // differ — HostScheduler, runSweep and discoverOnce are the shipped code.
  const sched = new HostScheduler({ baseGapMs: 50, maxGapMs: 400, retryAfterCapMs: 1_200 });
  const items: Item[] = [];
  // 6 endpoints on the throttled host, 6 on the clean host.
  for (let i = 0; i < 6; i++) {
    items.push({ serverId: `00000000-0000-0000-0000-0000000000${String(i).padStart(2, '0')}`, url: `http://${THROTTLED_HOST}:${port}/throttled`, host: THROTTLED_HOST, attempts: 0 });
    items.push({ serverId: `11111111-1111-1111-1111-1111111111${String(i).padStart(2, '0')}`, url: `http://${CLEAN_HOST}:${port}/ok`, host: CLEAN_HOST, attempts: 0 });
  }
  for (const it of items) sched.add(it);

  const rows: { host: string; status: string; error_class: string; versions: string[] | null }[] = [];
  const t0 = Date.now();
  const done = await runSweep(sched, 4, (item) => discoverOnce(item, 'POST', null), (row) => {
    rows.push({
      host: new URL(row.endpoint_url).hostname,
      status: row.discover_status,
      error_class: row.error_class,
      versions: row.protocol_versions,
    });
  });
  const elapsed = Date.now() - t0;
  await new Promise<void>((r) => server.close(() => r()));

  const s = sched.stats();
  const throttledRows = rows.filter((r) => r.host === THROTTLED_HOST);
  const cleanRows = rows.filter((r) => r.host === CLEAN_HOST);

  console.log('\n── observed ──────────────────────────────────────────────');
  console.log(`rows completed          : ${done} (${throttledRows.length} throttled host, ${cleanRows.length} clean host)`);
  console.log(`server hits             : /throttled=${throttledHits}  /ok=${cleanHits}`);
  console.log(`429s counted            : ${s.total429}`);
  console.log(`hosts backed off        : ${s.hostsBackedOff} of ${s.hosts}`);
  console.log(`worst per-host gap      : ${s.worstGapMs}ms`);
  console.log(`elapsed                 : ${elapsed}ms`);
  console.log(`throttled discover_status: ${[...new Set(throttledRows.map((r) => r.status))].join(',')} / error_class ${[...new Set(throttledRows.map((r) => r.error_class))].join(',')}`);
  console.log(`clean discover_status    : ${[...new Set(cleanRows.map((r) => r.status))].join(',')} / error_class ${[...new Set(cleanRows.map((r) => r.error_class))].join(',')}`);
  console.log(`versions written verbatim: ${JSON.stringify(cleanRows[0]?.versions)}`);

  // ── the captured request, asserted against the spec'd form ──────────────
  if (!captured) fail('no request was captured on the clean host');
  const cap = captured as { method: string; headers: http.IncomingHttpHeaders; body: string };
  const hdr = (n: string) => {
    const v = cap.headers[n.toLowerCase()];
    return Array.isArray(v) ? v.join(',') : (v ?? '');
  };
  let parsedBody: any = null;
  try { parsedBody = JSON.parse(cap.body); } catch { /* asserted below */ }
  const meta = parsedBody?.params?._meta ?? {};
  const metaVersion = meta['io.modelcontextprotocol/protocolVersion'];
  const headerVersion = hdr('MCP-Protocol-Version');

  console.log('\n── captured request (off the wire) ───────────────────────');
  console.log(`${cap.method} /ok`);
  for (const n of ['content-type', 'accept', 'mcp-protocol-version', 'mcp-method', 'user-agent']) {
    console.log(`  ${n}: ${hdr(n) || '(absent)'}`);
  }
  for (const n of ['authorization', 'mcp-session-id', 'mcp-name', 'cookie']) {
    if (hdr(n)) console.log(`  !! UNEXPECTED ${n}: ${hdr(n)}`);
  }
  console.log(`  body: ${cap.body}`);

  console.log('\n── assertions ────────────────────────────────────────────');
  const checks: [string, boolean][] = [
    // wire form — spec anchors in the worker's DISCOVER_BODY comment block
    ['request method is POST', cap.method === 'POST'],
    ['MCP-Protocol-Version header present and = 2026-07-28 (T:250-259)',
      headerVersion === DISCOVER_WIRE_FORM.MCP_PROTOCOL_VERSION && headerVersion === '2026-07-28'],
    ['Mcp-Method header present and mirrors the method (T:286-292)',
      hdr('Mcp-Method') === DISCOVER_WIRE_FORM.DISCOVER_METHOD && hdr('Mcp-Method') === 'server/discover'],
    ['Accept lists both JSON and SSE (T:76)',
      hdr('accept').includes('application/json') && hdr('accept').includes('text/event-stream')],
    ['body is valid JSON-RPC 2.0 with an id', parsedBody?.jsonrpc === '2.0' && parsedBody?.id !== undefined],
    ['body method is server/discover', parsedBody?.method === 'server/discover'],
    ['params present (S:665-667 — required on DiscoverRequest)',
      parsedBody?.params !== undefined && parsedBody.params !== null],
    ['params._meta present (S:179-180 — required on RequestParams)',
      typeof parsedBody?.params?._meta === 'object' && parsedBody.params._meta !== null],
    ['_meta protocolVersion present (S:76 — required)', metaVersion === '2026-07-28'],
    ['_meta clientCapabilities present (S:98 — required)',
      typeof meta['io.modelcontextprotocol/clientCapabilities'] === 'object'],
    ['_meta clientInfo has name+version (S:90 — optional, SHOULD send)',
      typeof meta['io.modelcontextprotocol/clientInfo']?.name === 'string'
      && typeof meta['io.modelcontextprotocol/clientInfo']?.version === 'string'],
    ['HEADER AND _meta VERSION ARE IDENTICAL — no HeaderMismatch (T:250-259)',
      headerVersion === metaVersion],
    ['no initialize / notifications/initialized sent', parsedBody?.method === 'server/discover'],
    ['no Mcp-Session-Id sent (T:685-686)', hdr('mcp-session-id') === ''],
    ['no Authorization or Cookie sent', hdr('authorization') === '' && hdr('cookie') === ''],
    ['no Mcp-Name sent (T:286-292 — not required for discover)', hdr('mcp-name') === ''],
    // pacing / classification (unchanged)
    ['every endpoint produced exactly one row', done === items.length && rows.length === items.length],
    ['throttled host was penalised (429s counted)', s.total429 > 0],
    ['backoff is scoped to ONE host, not global', s.hostsBackedOff === 1],
    ['throttled gap escalated above the configured floor', s.worstGapMs > sched.baseGapMs],
    ['throttled endpoints retried exactly once each (6 items -> 12 hits)', throttledHits === throttledRows.length * 2],
    ['throttled rows record error_class=rate_limited', throttledRows.every((r) => r.error_class === 'rate_limited')],
    ['throttled rows record discover_status=error', throttledRows.every((r) => r.status === 'error')],
    ['clean host still completed — no global stall', cleanRows.length === 6 && cleanHits === 6],
    ['clean rows record discover_status=ok', cleanRows.every((r) => r.status === 'ok')],
    ['clean rows record error_class=ok', cleanRows.every((r) => r.error_class === 'ok')],
    ['versions stored VERBATIM, malformed value not dropped or rewritten',
      JSON.stringify(cleanRows[0]?.versions) === JSON.stringify(['2026-07-28', 'garbage'])],
    ['Retry-After honoured (elapsed >= one retry window)', elapsed >= RETRY_AFTER_SECONDS * 1000],
  ];
  let ok = true;
  for (const [label, pass] of checks) {
    console.log(`${pass ? 'PASS' : 'FAIL'}  ${label}`);
    if (!pass) ok = false;
  }
  if (!ok) fail('one or more assertions failed');
  console.log('\nAll assertions passed.');
  process.exit(0);
}

main().catch((e) => { console.error(e); server.close(); process.exit(1); });
