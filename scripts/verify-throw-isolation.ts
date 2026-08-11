/**
 * scripts/verify-throw-isolation.ts — prove that no single endpoint can end a
 * sweep. No database, no network beyond loopback.
 *
 * This exists because of the 2026-08-11 04:12Z run: it died after 1,500 of
 * 11,006 endpoints with DOMException [TimeoutError] and a stack containing no
 * application frames. The cause was one endpoint that sent headers promptly and
 * then stalled mid-body. fetch() had already settled, but its
 * AbortSignal.timeout was still armed, so at TIMEOUT_MS the body stream aborted
 * and reader.read() rejected inside readBounded — which has try/FINALLY and no
 * catch. That rejection hit an unguarded `await readBounded(res)`, escaped
 * discoverOnce, escaped the worker loop, rejected Promise.all, and took every
 * other in-flight worker with it.
 *
 * Runs the SHIPPED code paths — HostScheduler, runSweep and discoverOnce — so
 * what is demonstrated is what ships, not a re-implementation.
 *
 * THREE ROUTES, three failure shapes:
 *   /stall   headers + a partial body, then silence forever. Exercises the REAL
 *            abort: the same DOMException, from the same timer, over real fetch.
 *   /ok      a valid server/discover result.
 *   (throw)  an injected exception from `perform` itself, standing in for a
 *            defect in the prober rather than a fact about the endpoint.
 *
 * PASSES when: the stalled endpoint RESOLVES as a row with error_class='timeout'
 * instead of throwing; the injected throw becomes a probe_exception row; and the
 * sweep still completes with a row for every endpoint including the clean ones.
 *
 * Takes ~TIMEOUT_MS (15s) to run, because the abort it proves is a real timeout.
 *
 * Run: npx tsx scripts/verify-throw-isolation.ts
 */

import http from 'node:http';
import {
  HostScheduler, runSweep, discoverOnce, type Item, type Outcome, type DiscoverRow,
} from '../workers/discover-mcp-servers';

const fails: string[] = [];
const check = (label: string, pass: boolean, detail = '') => {
  console.log(`${pass ? 'PASS' : '*** FAIL ***'}  ${label}${detail ? '  — ' + detail : ''}`);
  if (!pass) fails.push(label);
};

// Held open so the stalled sockets are not garbage-collected mid-test.
const stalled: http.ServerResponse[] = [];

const server = http.createServer((req, res) => {
  if (req.url === '/stall') {
    // Headers go out immediately, so fetch() RESOLVES. Then one partial chunk
    // and nothing further — the body never completes and never errors. This is
    // precisely the shape that killed the sweep: a well-behaved-looking response
    // whose body outlives the abort signal still attached to it.
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.write('{"jsonrpc":"2.0","id":1,"result":{"supported');
    stalled.push(res);
    return;
  }
  if (req.url === '/ok') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      jsonrpc: '2.0', id: 1,
      result: { supportedVersions: ['2026-07-28'], serverInfo: { name: 'verify', version: '0' } },
    }));
    return;
  }
  res.writeHead(404); res.end();
});

async function main() {
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', () => r()));
  const port = (server.address() as import('node:net').AddressInfo).port;
  const base = `http://127.0.0.1:${port}`;

  /* ── 1. the real abort, on its own ───────────────────────────────────────── */
  console.log('\n── /stall through discoverOnce (real AbortSignal, ~15s) ──────');
  const stallItem: Item = { serverId: 's-stall', url: `${base}/stall`, attempts: 0 };
  const t0 = Date.now();
  let stallOutcome: Outcome | null = null;
  let threw: unknown = null;
  try {
    stallOutcome = await discoverOnce(stallItem, 'POST', null);
  } catch (e) {
    threw = e;
  }
  const elapsed = Date.now() - t0;
  console.log(`  elapsed ${elapsed}ms`);

  check('a stalled body does NOT throw out of discoverOnce', threw === null,
    threw instanceof Error ? `${threw.name}: ${threw.message}` : String(threw ?? ''));
  if (stallOutcome && stallOutcome.kind === 'row') {
    const r = stallOutcome.row;
    console.log(`  row: error_class=${r.error_class} discover_status=${r.discover_status} http=${r.http_status} note=${r.note}`);
    check('it is classified as a timeout', r.error_class === 'timeout', String(r.error_class));
    check('discover_status is error', r.discover_status === 'error', String(r.discover_status));
    check('the note names the stage (body read)', /body read aborted/.test(r.note ?? ''), r.note ?? '(null)');
    check('header facts learned before the stall are kept', r.http_status === 200,
      `http_status=${r.http_status}`);
    check('it aborted at about TIMEOUT_MS, not instantly', elapsed >= 14_000 && elapsed <= 25_000, `${elapsed}ms`);
  } else {
    check('a stalled body yields a row', false, `kind=${stallOutcome?.kind ?? 'none'}`);
  }

  /* ── 2. isolation: one throw, one stall, and the sweep still finishes ───── */
  console.log('\n── runSweep with an injected throw + a stall + clean rows ─────');
  const items: Item[] = [
    { serverId: 's-ok-1', url: `${base}/ok`, attempts: 0 },
    { serverId: 's-boom', url: `${base}/boom`, attempts: 0 },
    { serverId: 's-ok-2', url: `${base}/ok`, attempts: 0 },
    { serverId: 's-stall-2', url: `${base}/stall`, attempts: 0 },
    { serverId: 's-ok-3', url: `${base}/ok`, attempts: 0 },
  ];
  const sched = new HostScheduler({});
  for (const it of items) sched.add(it);

  let backstopFired = 0;
  // Mirrors discoverSafely() in the worker: never throws, always yields a row.
  const performSafely = async (item: Item): Promise<Outcome> => {
    try {
      if (item.serverId === 's-boom') throw new Error('injected defect in the prober');
      return await discoverOnce(item, 'POST', null);
    } catch (err) {
      const msg = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
      return {
        kind: 'row',
        row: {
          server_id: item.serverId, endpoint_url: item.url,
          probed_at: new Date().toISOString(),
          http_status: null, response_time_ms: null,
          error_class: 'other', tls_valid: null,
          note: `probe_exception: ${msg}`.slice(0, 400),
          probe_method: 'POST', content_type: null,
          protocol_versions: null, version_source: null,
          discover_status: 'error', discover_raw: null,
        },
      };
    }
  };

  const rows: DiscoverRow[] = [];
  let sweepThrew: unknown = null;
  let done = 0;
  try {
    done = await runSweep(sched, 3, performSafely, (row) => { rows.push(row); },
      () => { backstopFired++; });
  } catch (e) {
    sweepThrew = e;
  }

  check('the sweep completed rather than aborting', sweepThrew === null,
    sweepThrew instanceof Error ? sweepThrew.message : String(sweepThrew ?? ''));
  check('every endpoint produced a row', rows.length === items.length,
    `${rows.length}/${items.length}`);
  check('runSweep counted them all', done === items.length, `done=${done}`);
  check('the backstop did NOT need to fire', backstopFired === 0, `${backstopFired}`);

  const boom = rows.find((r) => r.server_id === 's-boom');
  check('the thrown endpoint wrote a probe_exception row', !!boom && /^probe_exception:/.test(boom.note ?? ''),
    boom?.note ?? '(no row)');
  check('the thrown endpoint is error_class=other', boom?.error_class === 'other', String(boom?.error_class));

  const stall2 = rows.find((r) => r.server_id === 's-stall-2');
  check('the stalled endpoint wrote a timeout row', stall2?.error_class === 'timeout', String(stall2?.error_class));

  const oks = rows.filter((r) => r.server_id.startsWith('s-ok-'));
  check('all three clean endpoints still succeeded', oks.length === 3 && oks.every((r) => r.discover_status === 'ok'),
    oks.map((r) => `${r.server_id}=${r.discover_status}`).join(' '));

  /* ── 3. the backstop itself, when the contract IS broken ─────────────────── */
  console.log('\n── runSweep backstop: a perform that really throws ────────────');
  const sched2 = new HostScheduler({});
  sched2.add({ serverId: 's-raw', url: `${base}/ok`, attempts: 0 });
  sched2.add({ serverId: 's-raw-2', url: `${base}/ok`, attempts: 0 });
  let backstop2 = 0;
  let raw2Threw: unknown = null;
  let done2 = 0;
  try {
    done2 = await runSweep(sched2, 2, async () => { throw new Error('contract broken'); },
      () => {}, () => { backstop2++; });
  } catch (e) { raw2Threw = e; }
  check('a throwing perform does not abort the sweep', raw2Threw === null,
    raw2Threw instanceof Error ? raw2Threw.message : String(raw2Threw ?? ''));
  check('the backstop fired for every endpoint', backstop2 === 2, `${backstop2}`);
  check('no rows counted for backstopped endpoints', done2 === 0, `done=${done2}`);

  for (const res of stalled) { try { res.destroy(); } catch { /* already gone */ } }
  server.close();

  console.log(`\n${fails.length === 0 ? 'All isolation checks passed.' : 'FAILED: ' + fails.join(' | ')}`);
  process.exit(fails.length === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); server.close(); process.exit(1); });
