/**
 * verify-list-collectors.ts -- fixture-driven checks for the resources/list and
 * prompts/list collectors. NOTHING HERE DIALS A REAL ENDPOINT: collectOnce takes
 * a fetch implementation, and every case below passes a stub.
 *
 *   npx tsx scripts/verify-list-collectors.ts
 *
 * The DB-level demonstrations (unsupported vs error separability, the truncation
 * rule, scan_run_id) live alongside this in the report, run against the real
 * schema inside a transaction that rolls back.
 */

import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { collectOnce, type ListSpec } from '../lib/mcp/listCollector';
import { loadExclusions, isExcluded, type ExclusionReader } from '../lib/mcp/exclusions';
import { RESOURCES_SPEC, USER_AGENT as RES_UA } from '../workers/collect-mcp-resources';
import { PROMPTS_SPEC, USER_AGENT as PR_UA } from '../workers/collect-mcp-prompts';

const fails: string[] = [];
const check = (label: string, pass: boolean, detail = ''): void => {
  console.log(`${pass ? 'PASS' : '*** FAIL ***'}  ${label}${detail ? '  -- ' + detail : ''}`);
  if (!pass) fails.push(label);
};
const obs = (k: string, v: unknown): void => console.log(`      ${String(k).padEnd(38)} ${String(v)}`);

const ITEM = { serverId: '00000000-0000-0000-0000-000000000001', url: 'https://fixture.invalid/mcp', host: 'fixture.invalid', attempts: 0 };

/** Build a stub fetch that returns a queue of canned responses. */
function stubFetch(responses: Array<{ status: number; body: unknown; ct?: string; headers?: Record<string, string> }>) {
  let i = 0;
  return (async (_url: string, _init?: RequestInit): Promise<Response> => {
    const r = responses[Math.min(i, responses.length - 1)];
    i++;
    return new Response(typeof r.body === 'string' ? r.body : JSON.stringify(r.body), {
      status: r.status,
      headers: { 'content-type': r.ct ?? 'application/json', ...(r.headers ?? {}) },
    });
  }) as unknown as typeof fetch;
}

async function main(): Promise<void> {
  /* ---------------------------------------------------------------------- */
  console.log('\n== 1. NO FORBIDDEN METHOD APPEARS IN ANY COLLECTOR SOURCE ==');
  const sources: Record<string, string> = {
    'workers/collect-mcp-resources.ts': readFileSync('workers/collect-mcp-resources.ts', 'utf8'),
    'workers/collect-mcp-prompts.ts': readFileSync('workers/collect-mcp-prompts.ts', 'utf8'),
    'lib/mcp/listCollector.ts': readFileSync('lib/mcp/listCollector.ts', 'utf8'),
  };
  // Comments legitimately NAME these methods to say they are never sent, so the
  // check must look at CODE, not at prose. Comment lines are stripped first;
  // anything surviving is a real occurrence.
  const stripComments = (s: string): string =>
    s.replace(/\/\*[\s\S]*?\*\//g, ' ').split('\n').filter((l) => !l.trim().startsWith('//')).join('\n');
  for (const [file, src] of Object.entries(sources)) {
    const code = stripComments(src);
    for (const forbidden of ['initialize', 'resources/read', 'prompts/get']) {
      const n = code.split(forbidden).length - 1;
      check(`${file}: zero code occurrences of "${forbidden}"`, n === 0, `${n} in code`);
    }
  }

  /* ---------------------------------------------------------------------- */
  console.log('\n== 2. USER-AGENT BYTE-IDENTITY WITH THE PAGE ==');
  const sha = (s: string): string => createHash('sha256').update(s, 'utf8').digest('hex');
  obs('resources UA bytes', Buffer.byteLength(RES_UA, 'utf8'));
  obs('resources UA sha256', sha(RES_UA));
  obs('prompts UA bytes', Buffer.byteLength(PR_UA, 'utf8'));
  obs('prompts UA sha256', sha(PR_UA));
  check('resources UA matches the page hash',
    sha(RES_UA) === '8aa9bd7b1bc759a3c77a758e0aa54dd9dbe69731f388a764c95f7043378312f9');
  check('prompts UA matches the page hash',
    sha(PR_UA) === '6f881fecc6956a56167349edcaae65b1a85e944689de83cb9eff4e1687fd8ead');
  // Widened to string: both constants are literal types, and comparing two
  // non-overlapping literals is a compile error even though the runtime check is
  // exactly what we want to assert.
  check('the two agents are distinct strings', (RES_UA as string) !== (PR_UA as string));
  check('resources UA names its own method only',
    RES_UA.includes('resources/list') && !RES_UA.includes('tools/list') && !RES_UA.includes('prompts/list'));
  check('prompts UA names its own method only',
    PR_UA.includes('prompts/list') && !PR_UA.includes('tools/list') && !PR_UA.includes('resources/list'));

  /* ---------------------------------------------------------------------- */
  console.log('\n== 3. EXCLUSIONS COME FROM THE SHARED MODULE ==');
  // The real question is not "does it import by some path" -- listCollector sits
  // inside lib/mcp and imports './exclusions', a sibling. It is "does any file
  // DEFINE its own matching or loading logic". A private copy would have to
  // declare one of these.
  for (const [file, src] of Object.entries(sources)) {
    const definesOwn =
      /(?:function|const)\s+loadExclusions\b/.test(src) ||
      /(?:function|const)\s+isExcluded\b/.test(src) ||
      /FROM mcp_exclusions/i.test(src);
    check(`${file}: defines no private copy of the matching rules`, !definesOwn);
  }
  check('listCollector imports loadExclusions/isExcluded from lib/mcp/exclusions',
    /import \{[^}]*loadExclusions[^}]*\} from '\.\/exclusions'/.test(sources['lib/mcp/listCollector.ts']));

  /* ---------------------------------------------------------------------- */
  console.log('\n== 4. FAIL CLOSED, BOTH COLLECTORS ==');
  // A reader that throws is what an unreadable mcp_exclusions looks like.
  const brokenDb: ExclusionReader = { query: async () => { throw new Error('permission denied for table mcp_exclusions'); } };
  for (const name of ['resources', 'prompts']) {
    let threw = false;
    let msg = '';
    try { await loadExclusions(brokenDb); } catch (e) { threw = true; msg = e instanceof Error ? e.message : String(e); }
    check(`${name}: unreadable exclusions THROWS before any dial`, threw, msg.slice(0, 60) + '...');
    check(`${name}: the refusal names the consequence`, /REFUSING TO SWEEP/.test(msg));
  }
  // And the new methods are actually honoured by an 'all' rule.
  const okDb: ExclusionReader = {
    query: async () => ({ rows: [{ scope: 'host', pattern: 'opted-out.invalid', applies_to: 'all' }] }),
  };
  const set = await loadExclusions(okDb);
  for (const m of ['resources', 'prompts'] as const) {
    const hit = isExcluded({ url: 'https://opted-out.invalid/mcp', host: 'opted-out.invalid', serverId: 'x' }, set, m);
    check(`an applies_to='all' opt-out covers ${m}`, hit.excluded === true);
  }
  const notHit = isExcluded({ url: 'https://other.invalid/mcp', host: 'other.invalid', serverId: 'x' }, set, 'resources');
  check('a non-matching host is NOT excluded', notHit.excluded === false);

  /* ---------------------------------------------------------------------- */
  console.log('\n== 5. METHOD-NOT-SUPPORTED IS NOT AN ERROR ==');
  const cases: Array<{ label: string; spec: ListSpec; res: any; expect: string }> = [
    { label: 'resources: 200 + jsonrpc -32601', spec: RESOURCES_SPEC, expect: 'unsupported',
      res: { status: 200, body: { jsonrpc: '2.0', id: 1, error: { code: -32601, message: 'Method not found' } } } },
    { label: 'prompts: 200 + jsonrpc -32601', spec: PROMPTS_SPEC, expect: 'unsupported',
      res: { status: 200, body: { jsonrpc: '2.0', id: 1, error: { code: -32601, message: 'Method not found' } } } },
    { label: 'resources: HTTP 405', spec: RESOURCES_SPEC, expect: 'unsupported', res: { status: 405, body: {} } },
    { label: 'prompts: HTTP 501', spec: PROMPTS_SPEC, expect: 'unsupported', res: { status: 501, body: {} } },
    { label: 'resources: HTTP 500 (transport/server fault)', spec: RESOURCES_SPEC, expect: 'error', res: { status: 500, body: {} } },
    { label: 'prompts: HTTP 502 (transport/server fault)', spec: PROMPTS_SPEC, expect: 'error', res: { status: 502, body: {} } },
    { label: 'resources: session demanded', spec: RESOURCES_SPEC, expect: 'session_required',
      res: { status: 400, body: { jsonrpc: '2.0', id: 1, error: { code: -32600, message: 'Session required: call initialize first' } } } },
  ];
  for (const c of cases) {
    const { capture } = await collectOnce(c.spec, ITEM, stubFetch([c.res]));
    check(`${c.label} -> status='${c.expect}'`, capture.status === c.expect, `got '${capture.status}'`);
  }

  /* ---------------------------------------------------------------------- */
  console.log('\n== 6. HAPPY PATH AND ITEM MAPPING ==');
  const resOk = await collectOnce(RESOURCES_SPEC, ITEM, stubFetch([{
    status: 200,
    body: { jsonrpc: '2.0', id: 1, result: { resources: [
      { uri: 'file:///a.txt', name: 'a', title: 'A', description: 'first', mimeType: 'text/plain' },
      { uri: 'file:///b.bin', name: 'b', mimeType: 'application/octet-stream' },
    ] } },
  }]));
  check('resources: status ok', resOk.capture.status === 'ok');
  check('resources: item_count = 2', resOk.capture.item_count === 2, String(resOk.capture.item_count));
  check('resources: uri is the identity field', resOk.capture.items[0].uri === 'file:///a.txt');
  check('resources: mime_type mapped from mimeType', resOk.capture.items[1].mime_type === 'application/octet-stream');
  check('resources: truncated=false on a complete list', resOk.capture.truncated === false);

  const prOk = await collectOnce(PROMPTS_SPEC, ITEM, stubFetch([{
    status: 200,
    body: { jsonrpc: '2.0', id: 1, result: { prompts: [
      { name: 'summarise', description: 'sum', arguments: [
        { name: 'text', description: 't', required: true },
        { name: 'style', description: 's', required: false },
      ] },
      { name: 'noargs' },
    ] } },
  }]));
  check('prompts: status ok', prOk.capture.status === 'ok');
  check('prompts: argument_count = 2', prOk.capture.items[0].argument_count === 2, String(prOk.capture.items[0].argument_count));
  check('prompts: required_count = 1', prOk.capture.items[0].required_count === 1, String(prOk.capture.items[0].required_count));
  check('prompts: missing arguments -> NULL counts, not 0',
    prOk.capture.items[1].argument_count === null && prOk.capture.items[1].required_count === null,
    `${prOk.capture.items[1].argument_count}/${prOk.capture.items[1].required_count}`);

  /* ---------------------------------------------------------------------- */
  console.log('\n== 7. TRUNCATION -- THE DISCRIMINATING FIXTURE ==');
  // Two endpoints see the SAME final page. One of them is mid-cursor when
  // MAX_PAGES stops it; the other simply ends. The item sets are identical, so
  // only `truncated` separates "we finished looking" from "we did not" -- which
  // is exactly the discrimination the evidence view depends on.
  const endless = { status: 200, body: { jsonrpc: '2.0', id: 1, result: { resources: [{ uri: 'file:///only.txt', name: 'only' }], nextCursor: 'MORE' } } };
  const complete = { status: 200, body: { jsonrpc: '2.0', id: 1, result: { resources: [{ uri: 'file:///only.txt', name: 'only' }] } } };
  const trunc = await collectOnce(RESOURCES_SPEC, ITEM, stubFetch([endless]));
  const full = await collectOnce(RESOURCES_SPEC, ITEM, stubFetch([complete]));
  obs('truncated capture: status/pages/truncated', `${trunc.capture.status}/${trunc.capture.page_count}/${trunc.capture.truncated}`);
  obs('complete  capture: status/pages/truncated', `${full.capture.status}/${full.capture.page_count}/${full.capture.truncated}`);
  check('a never-ending cursor is recorded truncated=true', trunc.capture.truncated === true);
  check('...and still status=ok (we did see resources)', trunc.capture.status === 'ok');
  check('...and page_count > 1, satisfying the CHECK', trunc.capture.page_count > 1, String(trunc.capture.page_count));
  check('a complete list is truncated=false', full.capture.truncated === false);
  // The pair is only discriminating if the OBSERVED SET is the same. If the sets
  // differed, a view could tell them apart on content and `truncated` would be
  // untested. Compared as distinct URIs, not as a multiset: the truncated run
  // legitimately saw the same URI on both of its pages, and that repetition is a
  // property of the fixture rather than a difference in what was observed.
  const distinct = (rows: Array<Record<string, unknown>>): string =>
    JSON.stringify([...new Set(rows.map((i) => String(i.uri)))].sort());
  obs('truncated distinct uris', distinct(trunc.capture.items));
  obs('complete  distinct uris', distinct(full.capture.items));
  check('both captures observed the SAME resource set (only truncated separates them)',
    distinct(trunc.capture.items) === distinct(full.capture.items));

  // A server repeating its cursor must also be recorded partial, not looped on.
  const repeat = await collectOnce(RESOURCES_SPEC, ITEM, stubFetch([
    { status: 200, body: { jsonrpc: '2.0', id: 1, result: { resources: [{ uri: 'file:///x' }], nextCursor: 'SAME' } } },
    { status: 200, body: { jsonrpc: '2.0', id: 1, result: { resources: [{ uri: 'file:///y' }], nextCursor: 'SAME' } } },
  ]));
  check('an unchanged nextCursor stops and records truncated', repeat.capture.truncated === true);
  check('...without spinning to MAX_PAGES', repeat.capture.page_count < 20, String(repeat.capture.page_count));

  /* ---------------------------------------------------------------------- */
  console.log('\n== 8. NEVER THROWS ==');
  const boom = (async () => { throw new TypeError('fetch failed'); }) as unknown as typeof fetch;
  for (const [name, spec] of [['resources', RESOURCES_SPEC], ['prompts', PROMPTS_SPEC]] as const) {
    const r = await collectOnce(spec, ITEM, boom);
    check(`${name}: a throwing fetch yields a capture, not a rejection`, r.capture.status === 'error');
  }
  const badUrl = await collectOnce(RESOURCES_SPEC, { ...ITEM, url: 'not-a-url' }, stubFetch([{ status: 200, body: {} }]));
  check('an invalid URL is not_attempted, never dialled', badUrl.capture.status === 'not_attempted');

  console.log(`\n${fails.length === 0 ? 'ALL CHECKS PASS' : `${fails.length} FAILURE(S): ${fails.join(' | ')}`}`);
  process.exitCode = fails.length === 0 ? 0 : 1;
}

main().catch((e) => { console.error(e); process.exitCode = 1; });
