/**
 * scripts/verify-exclusions.ts -- the operator opt-out path, DEMONSTRATED.
 *
 *   npm run verify:exclusions
 *
 * WHAT THIS IS NOT. It is not a re-implementation of the exclusion rules in a
 * test. Both real workers are imported and RUN, with their database module and
 * global fetch replaced by doubles, so what is checked is the shipping control
 * flow. The central claim -- "no request was sent to the excluded endpoint" --
 * is settled by a fetch spy that records every URL the workers actually dial,
 * not by reading the source and reasoning about it. If someone later moves the
 * exclusion check to after the fetch, this script fails.
 *
 * WHY DOUBLES RATHER THAN A LIVE RUN. A live run would dial real third-party
 * endpoints to prove we did not dial one of them, which is a strange way to
 * demonstrate restraint. And "zero requests to X" is only meaningful if every
 * request is observable, which requires owning fetch.
 *
 * THE SQL HALF LIVES ELSEWHERE. revoked_at withdrawal is a property of the
 * query's WHERE clause, so it is checked against real Postgres by
 * eXaive/aive-platform scripts/verify-exclusions-sql.mjs, inside a transaction
 * that is deliberately rolled back. A fake reader cannot test a predicate it
 * never executes, and pretending otherwise would be the false-pass shape this
 * codebase has already been bitten by twice.
 *
 * The db double THROWS on any SQL it does not recognise. A query that changes
 * shape must fail loudly here rather than quietly return [] and let a check pass
 * for the wrong reason.
 */

import Module from 'node:module';
import path from 'node:path';
import { isExcluded, exclusionNote, type ExclusionSet, type Exclusion } from '../lib/mcp/exclusions';

/* ── reporting ───────────────────────────────────────────────────────────── */
const fails: string[] = [];
let checks = 0;
const check = (label: string, pass: boolean, detail = ''): void => {
  checks++;
  console.log(`${pass ? 'PASS' : '*** FAIL ***'}  ${label}${detail ? '  -- ' + detail : ''}`);
  if (!pass) fails.push(label);
};
const section = (t: string): void => console.log(`\n== ${t} ${'='.repeat(Math.max(0, 66 - t.length))}`);

/* ── fixture corpus ──────────────────────────────────────────────────────── */
const SRV_A = '11111111-1111-4111-8111-111111111111';
const SRV_B = '22222222-2222-4222-8222-222222222222';
const SRV_C = '33333333-3333-4333-8333-333333333333';
const SRV_D = '44444444-4444-4444-8444-444444444444';

const URL_OPEN      = 'https://open.fixture.invalid/mcp';       // never excluded
const URL_BY_HOST   = 'https://byhost.fixture.invalid/mcp';     // excluded, scope=host
const URL_BY_URL    = 'https://byurl.fixture.invalid/mcp';      // excluded, scope=endpoint
const URL_TOOLS_ONLY = 'https://toolsonly.fixture.invalid/mcp'; // excluded for tools ONLY

/**
 * Four rules, chosen so every branch of isExcluded is exercised by a worker run
 * rather than only by the unit checks at the end:
 *   - host scope, mixed case in the stored pattern (DNS is case-insensitive)
 *   - endpoint scope, exact URL
 *   - applies_to='tools', which must NOT stop the discover sweep
 *   - a revoked rule, which the SQL filters out -- represented here by its
 *     absence from the returned rows, and tested as a predicate in the SQL script
 */
const ACTIVE_RULES: Exclusion[] = [
  { scope: 'host',     pattern: 'ByHost.Fixture.Invalid', applies_to: 'all' },
  { scope: 'endpoint', pattern: URL_BY_URL,               applies_to: 'all' },
  { scope: 'endpoint', pattern: URL_TOOLS_ONLY,           applies_to: 'tools' },
  { scope: 'server',   pattern: SRV_D,                    applies_to: 'discover' },
];

/* ── fetch spy ───────────────────────────────────────────────────────────── */
let fetchLog: string[] = [];
const realFetch = globalThis.fetch;
globalThis.fetch = (async (input: any, init?: any) => {
  const url = typeof input === 'string' ? input : String(input?.url ?? input);
  fetchLog.push(url);
  // A plausible body for each worker. Shape correctness is another script's job;
  // what matters here is that a request happened at all, and to which URL.
  const body = JSON.stringify({
    jsonrpc: '2.0', id: 1,
    result: {
      protocolVersions: ['2026-07-28'],
      tools: [{ name: 'fixture_tool', inputSchema: { type: 'object', properties: {} } }],
    },
  });
  void init;
  return new Response(body, { status: 200, headers: { 'content-type': 'application/json' } });
}) as typeof fetch;

/* ── database double ─────────────────────────────────────────────────────── */
interface WrittenProbe { endpoint_url: string; discover_status: string; note: string | null; probe_method: string | null; error_class: string }
interface WrittenCapture { endpoint_url: string; status: string; note: string | null; page_count: number; tool_count: number | null; error_class: string | null; scan_run_id: string | null }

const written = { probes: [] as WrittenProbe[], captures: [] as WrittenCapture[], scanRunStatus: null as string | null };
/** Flipped by the fail-closed phase; loadExclusions must then refuse. */
let exclusionsReadable = true;
/** Counts loads so "loaded ONCE" is an observation, not a claim about the code. */
let exclusionLoads = 0;

const fakeDb = {
  ingestPool() {
    return {
      query: async (text: string) => {
        if (!/FROM mcp_exclusions/.test(text)) {
          throw new Error(`db double: unexpected pool query: ${text.slice(0, 120)}`);
        }
        exclusionLoads++;
        if (!exclusionsReadable) throw new Error('permission denied for table mcp_exclusions');
        // The two predicates the module is contracted to apply. Asserted rather
        // than assumed, because a fake reader that ignored them would let a
        // revoked rule pass this script while still excluding in production.
        if (!/revoked_at IS NULL/.test(text)) throw new Error('db double: query is missing the revoked_at predicate');
        if (!/effective_at <= now\(\)/.test(text)) throw new Error('db double: query is missing the effective_at predicate');
        return { rows: ACTIVE_RULES as unknown[] };
      },
      connect: async () => ({
        query: async (text: string, params?: unknown[]) => {
          if (text === 'BEGIN' || text === 'COMMIT' || text === 'ROLLBACK') return { rows: [] };
          return { rows: await fakeDb.q(text, params) };
        },
        release() { /* fixture connection */ },
      }),
    };
  },
  async q(text: string, params?: unknown[]): Promise<unknown[]> {
    if (/INSERT INTO mcp_tool_collection_runs/.test(text)) return [{ id: 'fixture-tool-collection-run' }];
    if (/UPDATE mcp_tool_collection_runs/.test(text)) return [];
    if (/INSERT INTO scan_runs/.test(text)) return [{ id: 'fixture-scan-run' }];
    if (/UPDATE scan_runs/.test(text)) {
      written.scanRunStatus = String((params ?? [])[2] ?? '');
      return [];
    }
    if (/pg_get_constraintdef/.test(text)) {
      return [{ def: "CHECK (probe_method = ANY (ARRAY['HEAD'::text, 'GET'::text, 'POST'::text]))" }];
    }
    if (/FROM mcp_servers/.test(text)) {
      return [
        { id: SRV_A, remotes: [{ url: URL_OPEN }] },
        { id: SRV_B, remotes: [{ url: URL_BY_HOST }] },
        { id: SRV_C, remotes: [{ url: URL_BY_URL }] },
        { id: SRV_D, remotes: [{ url: 'https://byserver.fixture.invalid/mcp' }] },
      ];
    }
    if (/FROM mcp_endpoint_probes/.test(text) && /WITH run AS/.test(text)) {
      return [
        { endpoint_url: URL_OPEN,       server_id: SRV_A, discover_status: 'ok' },
        { endpoint_url: URL_BY_HOST,    server_id: SRV_B, discover_status: 'ok' },
        { endpoint_url: URL_TOOLS_ONLY, server_id: SRV_C, discover_status: 'ok' },
      ];
    }
    if (/INSERT INTO mcp_tool_captures/.test(text)) {
      const p = params ?? [];
      written.captures.push({
        endpoint_url: String(p[1]), status: String(p[3]),
        error_class: p[6] === null || p[6] === undefined ? null : String(p[6]),
        tool_count: p[7] === null || p[7] === undefined ? null : Number(p[7]),
        page_count: Number(p[8]), note: p[16] === null || p[16] === undefined ? null : String(p[16]),
        // $18. Read positionally from the parameter array the worker actually
        // sends, so this observes the shipping INSERT rather than trusting that
        // the column list and the values stayed in step.
        scan_run_id: p[17] === null || p[17] === undefined ? null : String(p[17]),
      });
      return [{ id: `cap-${written.captures.length}` }];
    }
    if (/INSERT INTO mcp_tools/.test(text)) return [];
    throw new Error(`db double: unrecognised SQL (refusing to return [] and let a check pass): ${text.slice(0, 140)}`);
  },
  // Signature mirrors lib/ingest/db.ts insertRows: the 4th arg is the opt-in
  // ON CONFLICT target, and the return is the count ACTUALLY inserted. This
  // double never conflicts, so every row it accepts counts as written -- which
  // is what lets the caller's written/persisted arithmetic be exercised here.
  async insertRows(table: string, cols: string[], rows: unknown[][], _conflictTarget?: string): Promise<number> {
    if (table === 'mcp_endpoint_probes') {
      const ix = (c: string) => cols.indexOf(c);
      for (const r of rows) {
        written.probes.push({
          endpoint_url: String(r[ix('endpoint_url')]),
          discover_status: String(r[ix('discover_status')]),
          note: r[ix('note')] === null ? null : String(r[ix('note')]),
          probe_method: r[ix('probe_method')] === null ? null : String(r[ix('probe_method')]),
          error_class: String(r[ix('error_class')]),
        });
      }
      return rows.length;
    }
    if (table === 'mcp_tools') return rows.length; // not what this script is about
    throw new Error(`db double: unexpected insertRows table ${table}`);
  },
  async endIngestPool(): Promise<void> { /* nothing to close */ },
  async logIngestionPg(): Promise<void> { /* not under test */ },
  async upsertRows(): Promise<void> { throw new Error('db double: upsertRows not expected'); },
  async withRollback(): Promise<void> { throw new Error('db double: withRollback not expected'); },
};

/**
 * Intercept '../lib/ingest/db' for the workers. Done through require.cache with
 * the resolved absolute path, so the workers get the double no matter which
 * relative specifier they use.
 */
const dbPath = require.resolve(path.join(__dirname, '..', 'lib', 'ingest', 'db'));
require.cache[dbPath] = {
  id: dbPath, filename: dbPath, loaded: true, exports: fakeDb,
  children: [], paths: [], path: path.dirname(dbPath), parent: undefined,
} as unknown as NodeJS.Module;
void Module;

// Required AFTER the interception, so the workers close over the double.
// eslint-disable-next-line @typescript-eslint/no-var-requires
const discover = require('../workers/discover-mcp-servers') as typeof import('../workers/discover-mcp-servers');
// eslint-disable-next-line @typescript-eslint/no-var-requires
const tools = require('../workers/collect-mcp-tools') as typeof import('../workers/collect-mcp-tools');

const reset = (): void => {
  fetchLog = [];
  written.probes = [];
  written.captures = [];
  written.scanRunStatus = null;
  exclusionLoads = 0;
};

/* ────────────────────────────────────────────────────────────────────────── */
async function main(): Promise<void> {
  process.env.DISCOVER_CONCURRENCY = '4';
  process.env.DISCOVER_DEADLINE_MINUTES = '5';
  process.env.TOOLS_CONCURRENCY = '4';
  process.env.TOOLS_DEADLINE_MINUTES = '5';

  /* ── 1. discover worker, exclusions active ─────────────────────────────── */
  section('discover-mcp-servers: fixture exclusions');
  reset();
  const dres = await discover.discoverMcpServers();

  console.log(`  fetched: ${fetchLog.length ? fetchLog.join(', ') : '(nothing)'}`);
  console.log(`  probe rows: ${written.probes.map((p) => `${p.endpoint_url}=${p.discover_status}`).join(', ')}`);

  check('ZERO requests to the host-scoped exclusion',
    !fetchLog.some((u) => u.includes('byhost.fixture.invalid')),
    fetchLog.filter((u) => u.includes('byhost')).join(', ') || 'none seen');
  check('ZERO requests to the endpoint-scoped exclusion',
    !fetchLog.some((u) => u === URL_BY_URL));
  check('ZERO requests to the server-scoped exclusion',
    !fetchLog.some((u) => u.includes('byserver.fixture.invalid')));
  check('the non-excluded endpoint WAS dialled (the spy can see requests at all)',
    fetchLog.some((u) => u === URL_OPEN),
    'a spy that sees nothing would pass every check above for the wrong reason');

  const probeOf = (u: string) => written.probes.find((p) => p.endpoint_url === u);
  for (const [label, url] of [['host-scoped', URL_BY_HOST], ['endpoint-scoped', URL_BY_URL],
    ['server-scoped', 'https://byserver.fixture.invalid/mcp']] as const) {
    const row = probeOf(url);
    check(`a row IS still written for the ${label} exclusion (recorded, not dropped)`, !!row);
    check(`  ${label} row says discover_status='not_attempted'`, row?.discover_status === 'not_attempted', String(row?.discover_status));
    check(`  ${label} row carries the queryable 'excluded:' note prefix`, !!row?.note?.startsWith('excluded:'), row?.note ?? 'no note');
    check(`  ${label} row asserts no method was used (probe_method NULL)`, row?.probe_method === null, String(row?.probe_method));
  }

  check("applies_to='tools' does NOT stop the discover sweep",
    !written.probes.some((p) => p.endpoint_url === URL_TOOLS_ONLY && p.discover_status === 'not_attempted'),
    'URL_TOOLS_ONLY is not in the discover corpus, so its absence here is expected');
  check('the exclusion set is loaded exactly ONCE per run', exclusionLoads === 1, `${exclusionLoads} load(s)`);
  check('every corpus endpoint produced exactly one row', written.probes.length === 4, `${written.probes.length} rows for 4 endpoints`);
  check('the run reports what it declined to measure', /excluded=3/.test(written.scanRunStatus ?? ''), written.scanRunStatus ?? 'no status');
  check('excluded rows count toward written, so nothing vanishes', dres.written === 4, `written=${dres.written}`);

  /* ── 2. tools worker, exclusions active ────────────────────────────────── */
  section('collect-mcp-tools: fixture exclusions');
  reset();
  const tres = await tools.collectMcpTools();

  console.log(`  fetched: ${fetchLog.length ? fetchLog.join(', ') : '(nothing)'}`);
  console.log(`  captures: ${written.captures.map((c) => `${c.endpoint_url}=${c.status}`).join(', ')}`);

  check('ZERO tools/list requests to the host-scoped exclusion',
    !fetchLog.some((u) => u.includes('byhost.fixture.invalid')));
  check("ZERO tools/list requests to the applies_to='tools' exclusion",
    !fetchLog.some((u) => u === URL_TOOLS_ONLY));
  check('the non-excluded endpoint WAS dialled', fetchLog.some((u) => u === URL_OPEN));

  const capOf = (u: string) => written.captures.find((c) => c.endpoint_url === u);
  for (const [label, url] of [['host-scoped', URL_BY_HOST], ["applies_to='tools'", URL_TOOLS_ONLY]] as const) {
    const cap = capOf(url);
    check(`a capture IS still written for the ${label} exclusion`, !!cap);
    check(`  ${label} capture says status='excluded' (not not_attempted)`, cap?.status === 'excluded', String(cap?.status));
    check(`  ${label} capture carries the 'excluded:' note prefix`, !!cap?.note?.startsWith('excluded:'), cap?.note ?? 'no note');
    check(`  ${label} capture issued no request (page_count 0)`, cap?.page_count === 0, String(cap?.page_count));
    check(`  ${label} capture observed no transport outcome (error_class NULL)`, cap?.error_class === null, String(cap?.error_class));
    check(`  ${label} capture claims no tool_count (CHECK admits one only on ok)`, cap?.tool_count === null, String(cap?.tool_count));
  }
  check('the exclusion set is loaded exactly ONCE per run', exclusionLoads === 1, `${exclusionLoads} load(s)`);
  check('every target produced exactly one capture', written.captures.length === 3, `${written.captures.length} for 3 targets`);
  check('excluded captures count toward captures written', tres.captures === 3, `captures=${tres.captures}`);

  /* ── scan_run_id: a bounded or partial run must be identifiable from the
   * capture rows ALONE. Read off the INSERT parameters the worker actually
   * sent -- $18 by position -- so a column list that drifted out of step with
   * its values would fail here rather than write NULLs in production. */
  const runIds = written.captures.map((c) => c.scan_run_id);
  const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  console.log(`  scan_run_id on each capture: ${runIds.map((r) => (r ?? 'NULL')).join(', ')}`);
  check('every capture carries a non-NULL scan_run_id', runIds.every((r) => r !== null && r !== 'null'));
  check('scan_run_id is a well-formed UUID', runIds.every((r) => !!r && UUID_RE.test(r)), String(runIds[0]));
  check('all captures in ONE run share ONE scan_run_id', new Set(runIds).size === 1, `${new Set(runIds).size} distinct`);
  check('the EXCLUDED captures are stamped too (they use the same write path)',
    written.captures.filter((c) => c.status === 'excluded').every((c) => c.scan_run_id === runIds[0]),
    `${written.captures.filter((c) => c.status === 'excluded').length} excluded rows`);
  // Two runs must not collide, or grouping by the column proves nothing.
  const firstRunId = runIds[0];
  written.captures = [];
  await tools.collectMcpTools();
  const secondRunId = written.captures[0]?.scan_run_id ?? null;
  check('a SECOND run gets a DIFFERENT scan_run_id', !!secondRunId && secondRunId !== firstRunId,
    `${firstRunId} vs ${secondRunId}`);

  /* ── 3. fail closed ───────────────────────────────────────────────────── */
  section('fail closed: mcp_exclusions unreadable');
  exclusionsReadable = false;

  reset();
  let discoverRefused = false;
  let discoverMsg = '';
  try { await discover.discoverMcpServers(); } catch (e) { discoverRefused = true; discoverMsg = e instanceof Error ? e.message : String(e); }
  check('discover REFUSES to run when the table is unreadable', discoverRefused, discoverMsg.slice(0, 90));
  check('discover sent ZERO requests while refusing', fetchLog.length === 0, `${fetchLog.length} request(s)`);
  check('discover wrote ZERO probe rows while refusing', written.probes.length === 0, `${written.probes.length} row(s)`);
  check('the refusal names the consequence, not just the error', /REFUSING TO SWEEP/.test(discoverMsg));
  check('the scan run is CLOSED as failed, not left reading as running',
    /^failed exclusions_unreadable/.test(written.scanRunStatus ?? ''), written.scanRunStatus ?? 'never closed');

  reset();
  let toolsRefused = false;
  let toolsMsg = '';
  try { await tools.collectMcpTools(); } catch (e) { toolsRefused = true; toolsMsg = e instanceof Error ? e.message : String(e); }
  check('tools REFUSES to run when the table is unreadable', toolsRefused, toolsMsg.slice(0, 90));
  check('tools sent ZERO requests while refusing', fetchLog.length === 0, `${fetchLog.length} request(s)`);
  check('tools wrote ZERO captures while refusing', written.captures.length === 0, `${written.captures.length} capture(s)`);

  exclusionsReadable = true;

  /* ── 4. matching semantics ────────────────────────────────────────────── */
  section('isExcluded: matching semantics');
  const { loadExclusions } = require('../lib/mcp/exclusions') as typeof import('../lib/mcp/exclusions');
  const set: ExclusionSet = await loadExclusions(fakeDb.ingestPool() as any);
  const t = (url: string, host: string, serverId = SRV_A) => ({ url, host, serverId });

  check('host match is case-insensitive in BOTH directions',
    isExcluded(t(URL_BY_HOST, 'BYHOST.FIXTURE.INVALID'), set, 'discover').excluded &&
    isExcluded(t(URL_BY_HOST, 'byhost.fixture.invalid'), set, 'tools').excluded);
  check('a host match reports scope=host, not scope=endpoint',
    (isExcluded(t(URL_BY_HOST, 'byhost.fixture.invalid'), set, 'tools') as any).scope === 'host');
  check('matching is EXACT: a subdomain of an excluded host is NOT excluded',
    !isExcluded(t('https://sub.byhost.fixture.invalid/mcp', 'sub.byhost.fixture.invalid'), set, 'tools').excluded,
    'a prefix or suffix rule would exclude endpoints nobody asked about');
  check('matching is EXACT: a path under an excluded URL is NOT the excluded URL',
    !isExcluded(t(URL_BY_URL + '/v2', 'byurl.fixture.invalid', SRV_C), set, 'tools').excluded);
  check("applies_to='tools' excludes tools", isExcluded(t(URL_TOOLS_ONLY, 'toolsonly.fixture.invalid'), set, 'tools').excluded);
  check("applies_to='tools' does NOT exclude discover", !isExcluded(t(URL_TOOLS_ONLY, 'toolsonly.fixture.invalid'), set, 'discover').excluded);
  check("applies_to='discover' does NOT exclude tools", !isExcluded(t(URL_OPEN, 'open.fixture.invalid', SRV_D), set, 'tools').excluded);
  check('server scope matches on server_id alone', isExcluded(t(URL_OPEN, 'open.fixture.invalid', SRV_D), set, 'discover').excluded);
  check('an unlisted target is not excluded', !isExcluded(t(URL_OPEN, 'open.fixture.invalid'), set, 'discover').excluded);
  check('the note names WHICH rule matched, so the list is documentation',
    exclusionNote({ scope: 'host', pattern: 'byhost.fixture.invalid' })
      === 'excluded: operator opt-out matched host=byhost.fixture.invalid');
  check('an empty set excludes nothing',
    !isExcluded(t(URL_BY_URL, 'byurl.fixture.invalid'), { endpoints: new Map(), hosts: new Map(), servers: new Map(), count: 0 }, 'tools').excluded);

  /* ── done ─────────────────────────────────────────────────────────────── */
  globalThis.fetch = realFetch;
  console.log(`\n${fails.length === 0 ? `All ${checks} checks passed.` : `FAILED ${fails.length}/${checks}: ` + fails.join(' | ')}`);
  console.log(
    'NOT COVERED HERE: revoked_at withdrawal is a SQL predicate and is checked ' +
    'against real Postgres by aive-platform scripts/verify-exclusions-sql.mjs.',
  );
  process.exitCode = fails.length === 0 ? 0 : 1;
}

main().catch((e) => { console.error(e); process.exitCode = 1; });
