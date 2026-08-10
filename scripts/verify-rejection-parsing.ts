/**
 * scripts/verify-rejection-parsing.ts — prove version harvesting and
 * session detection against the EXACT bodies observed in the wild.
 *
 * Every fixture below is a verbatim copy of a response captured during the
 * 25-endpoint smoke run on 2026-08-09 (mcp_endpoint_probes.discover_raw). No
 * body is invented, simplified, or reformatted — over-fitting to a body we made
 * up would prove nothing.
 *
 * Runs the SHIPPED code: versionsFromRejection / demandsSession for the unit
 * layer, then discoverOnce end-to-end against a loopback server that replays
 * each body with its real HTTP status, so discover_status and version_source
 * are asserted on the row the worker would actually write.
 *
 * Run: npx tsx scripts/verify-rejection-parsing.ts
 */

import http from 'node:http';
import {
  HostScheduler, runSweep, discoverOnce, versionsFromRejection, demandsSession, type Item,
} from '../workers/discover-mcp-servers';

interface Fixture {
  key: string;
  server: string;
  status: number;
  contentType: string;
  body: string;
  expectVersions: string[];
  expectStatus: 'ok' | 'unsupported' | 'error' | 'session_required';
  expectSource: 'advertised' | 'rejection' | null;
}

const FIXTURES: Fixture[] = [
  // ── the four rejections that enumerated their versions ──
  {
    key: 'aarna', server: 'ai.aarna/atars-mcp', status: 400, contentType: 'application/json',
    body: '{"id": "server-error", "error": {"code": -32600, "message": "Bad Request: Unsupported protocol version: 2026-07-28. Supported versions: 2024-11-05, 2025-03-26, 2025-06-18, 2025-11-25"}, "jsonrpc": "2.0"}',
    expectVersions: ['2024-11-05', '2025-03-26', '2025-06-18', '2025-11-25'],
    expectStatus: 'error', expectSource: 'rejection',
  },
  {
    key: 'abmeter', server: 'ai.abmeter/abmeter', status: 400, contentType: 'application/json',
    body: '{"id": 1, "error": {"code": -32000, "message": "Unsupported MCP-Protocol-Version: 2026-07-28. Supported versions: 2025-11-25, 2025-06-18"}, "jsonrpc": "2.0"}',
    expectVersions: ['2025-11-25', '2025-06-18'],
    expectStatus: 'error', expectSource: 'rejection',
  },
  {
    key: 'fabrication', server: 'ai.agenticfabricationnetwork/ufp', status: 400, contentType: 'application/json',
    body: '{"id": null, "error": {"code": -32000, "message": "Bad Request: Unsupported protocol version: 2026-07-28 (supported versions: 2025-11-25, 2025-06-18, 2025-03-26, 2024-11-05, 2024-10-07)"}, "jsonrpc": "2.0"}',
    expectVersions: ['2025-11-25', '2025-06-18', '2025-03-26', '2024-11-05', '2024-10-07'],
    expectStatus: 'error', expectSource: 'rejection',
  },
  {
    key: 'agenticterminal', server: 'ai.agenticterminal/directory', status: 400, contentType: 'application/json',
    body: '{"id": null, "error": {"code": -32000, "message": "Bad Request: Unsupported protocol version: 2026-07-28 (supported versions: 2025-11-25, 2025-06-18, 2025-03-26, 2024-11-05, 2024-10-07)"}, "jsonrpc": "2.0"}',
    expectVersions: ['2025-11-25', '2025-06-18', '2025-03-26', '2024-11-05', '2024-10-07'],
    expectStatus: 'error', expectSource: 'rejection',
  },
  // ── the two session demands ──
  {
    key: 'agentberg', server: 'ai.agentberg/agentberg', status: 400, contentType: 'application/json',
    body: '{"id": "server-error", "error": {"code": -32600, "message": "Bad Request: Missing session ID"}, "jsonrpc": "2.0"}',
    expectVersions: [], expectStatus: 'session_required', expectSource: null,
  },
  {
    key: 'agentic-news', server: 'ai.agentic-news/mcp', status: 400, contentType: 'application/json',
    body: '{"id": null, "error": {"code": -32000, "message": "Bad Request: No valid session ID or initialization request"}, "jsonrpc": "2.0"}',
    expectVersions: [], expectStatus: 'session_required', expectSource: null,
  },
  // ── the non-JSON-RPC body: error is a bare STRING, must not crash ──
  {
    key: 'adoraads', server: 'ai.adoraads/beauty', status: 400, contentType: 'application/json',
    body: '{"error": "Unknown MCP method"}',
    expectVersions: [], expectStatus: 'error', expectSource: null,
  },
  // ── a method-not-found rejection stays 'unsupported' ──
  {
    key: 'afmr', server: 'ai.afmr/discovery', status: 200, contentType: 'application/json',
    body: '{"jsonrpc":"2.0","id":1,"error":{"code": -32601, "message": "Method not found"}}',
    expectVersions: [], expectStatus: 'unsupported', expectSource: null,
  },
  // ── a success, for the 'advertised' provenance ──
  {
    key: 'goji', server: 'agency.goji/goji', status: 200, contentType: 'application/json',
    body: '{"jsonrpc":"2.0","id":1,"result":{"resultType":"complete","supportedVersions":["2026-07-28"],"capabilities":{}}}',
    expectVersions: ['2026-07-28'], expectStatus: 'ok', expectSource: 'advertised',
  },
];

function fail(msg: string): never { console.error(`\nFAIL: ${msg}`); process.exit(1); }

const server = http.createServer((req, res) => {
  const key = (req.url ?? '').replace(/^\//, '');
  const f = FIXTURES.find((x) => x.key === key);
  req.resume();
  if (!f) { res.writeHead(404); res.end(); return; }
  res.writeHead(f.status, { 'Content-Type': f.contentType });
  res.end(f.body);
});

async function main() {
  const checks: [string, boolean][] = [];

  // ── unit layer: the parser alone, on the raw messages ──────────────────────
  console.log('── parser unit checks (message -> versions) ───────────────');
  for (const f of FIXTURES) {
    const msg = JSON.parse(f.body).error;
    const text = typeof msg === 'string' ? msg : (msg?.message ?? '');
    const got = versionsFromRejection(text).versions;
    const want = f.expectSource === 'rejection' ? f.expectVersions : [];
    const okv = JSON.stringify(got) === JSON.stringify(want);
    console.log(`${okv ? 'PASS' : 'FAIL'}  ${f.key.padEnd(16)} -> [${got.join(', ')}]`);
    checks.push([`parser: ${f.key} yields its full list verbatim`, okv]);
    if (f.expectStatus === 'session_required') {
      checks.push([`parser: ${f.key} detected as a session demand`, demandsSession(text)]);
    }
  }
  // Anchoring: "Unsupported protocol version: 2026-07-28" must NOT be read as
  // the list, and a body with no list must yield nothing rather than [''].
  checks.push(['parser: "Unsupported protocol version: X" alone yields nothing',
    versionsFromRejection('Bad Request: Unsupported protocol version: 2026-07-28').versions.length === 0]);
  checks.push(['parser: unrelated message yields nothing',
    versionsFromRejection('Internal Server Error').versions.length === 0]);
  checks.push(['parser: abmeter yields EXACTLY two versions',
    versionsFromRejection('Unsupported MCP-Protocol-Version: 2026-07-28. Supported versions: 2025-11-25, 2025-06-18').versions.length === 2]);
  checks.push(['parser: trailing period is stripped as a delimiter, not kept',
    JSON.stringify(versionsFromRejection('Supported versions: 2025-06-18, 2024-11-05.').versions)
      === JSON.stringify(['2025-06-18', '2024-11-05'])]);

  // ── end-to-end: the row discoverOnce would write ───────────────────────────
  await new Promise<void>((r) => server.listen(0, r));
  const addr = server.address();
  if (!addr || typeof addr === 'string') fail('could not bind loopback server');
  const port = addr.port;

  const sched = new HostScheduler({ baseGapMs: 5 });
  const items: Item[] = FIXTURES.map((f, i) => ({
    serverId: `00000000-0000-0000-0000-${String(i).padStart(12, '0')}`,
    url: `http://127.0.0.1:${port}/${f.key}`,
    host: '127.0.0.1', attempts: 0,
  }));
  for (const it of items) sched.add(it);

  const rows: { key: string; status: string; versions: string[] | null; source: string | null; note: string | null }[] = [];
  await runSweep(sched, 4, (item) => discoverOnce(item, 'POST', null), (row) => {
    rows.push({
      key: new URL(row.endpoint_url).pathname.replace(/^\//, ''),
      status: row.discover_status,
      versions: row.protocol_versions,
      source: row.version_source,
      note: row.note,
    });
  });
  await new Promise<void>((r) => server.close(() => r()));

  console.log('\n── end-to-end rows (as written) ──────────────────────────');
  console.log('fixture          | status           | source     | versions');
  console.log('-----------------+------------------+------------+---------------------------------');
  for (const f of FIXTURES) {
    const r = rows.find((x) => x.key === f.key);
    if (!r) { checks.push([`row written for ${f.key}`, false]); continue; }
    console.log(`${f.key.padEnd(16)} | ${r.status.padEnd(16)} | ${String(r.source ?? '-').padEnd(10)} | [${(r.versions ?? []).join(', ')}]`);
    checks.push([`${f.key}: discover_status = ${f.expectStatus}`, r.status === f.expectStatus]);
    checks.push([`${f.key}: version_source = ${f.expectSource ?? 'NULL'}`, (r.source ?? null) === f.expectSource]);
    checks.push([`${f.key}: protocol_versions = expected list`,
      JSON.stringify(r.versions ?? []) === JSON.stringify(f.expectVersions)]);
  }

  // ── the invariants the matrix names ───────────────────────────────────────
  const withVersions = rows.filter((r) => (r.versions ?? []).length > 0);
  checks.push(['version_source populated on EVERY row with non-empty protocol_versions',
    withVersions.length > 0 && withVersions.every((r) => r.source === 'advertised' || r.source === 'rejection')]);
  checks.push(['no row has a version_source without versions',
    rows.every((r) => r.source === null || (r.versions ?? []).length > 0)]);
  checks.push(['rejection-sourced rows are distinguishable from advertised ones',
    withVersions.some((r) => r.source === 'rejection') && withVersions.some((r) => r.source === 'advertised')
    && withVersions.every((r) => r.source !== null)]);
  const fab = rows.find((r) => r.key === 'fabrication');
  checks.push(['2024-10-07 stored VERBATIM (not dropped, not rewritten)',
    (fab?.versions ?? []).includes('2024-10-07')]);
  checks.push(['both session bodies classify session_required, NOT error',
    rows.filter((r) => r.key === 'agentberg' || r.key === 'agentic-news')
        .every((r) => r.status === 'session_required')]);

  console.log('\n── assertions ────────────────────────────────────────────');
  let ok = true;
  for (const [label, pass] of checks) {
    if (!pass) ok = false;
    console.log(`${pass ? 'PASS' : '*** FAIL ***'}  ${label}`);
  }
  if (!ok) fail('one or more assertions failed');
  console.log(`\nAll ${checks.length} assertions passed.`);
  process.exit(0);
}

main().catch((e) => { console.error(e); server.close(); process.exit(1); });
