/**
 * scripts/verify-tools-collector.ts -- fixture tests for the C1 collector.
 * No database, no network. Runs the SHIPPED functions, not copies of them.
 *
 * Proves the four properties the schema-hash design rests on:
 *   1. schema_hash survives key reordering and whitespace.
 *   2. schema_hash MOVES when a required param is added or a type changes,
 *      because that is the whole basis of catalogue items 3 and 21.
 *   3. contract_hash is unchanged by a description-only edit, so item 3 does not
 *      score a Breaking Change every time someone fixes a docstring.
 *   4. The two known false-drift classes are detected and COUNTED. They are a
 *      measurement, not a filter: the collector records the rate and hides
 *      nothing.
 *
 * Also proves the wire form carries no initialize and no session header, and
 * that a truncated capture is produced rather than a silent short list.
 *
 * Run: npx tsx scripts/verify-tools-collector.ts
 */

import {
  schemaHash, contractHash, canonicalise, paramSet, paramCount, requiredCount,
  maxDepth, falseDriftClasses, tallyFalseDrift, tokenEstimate,
} from '../lib/mcp/schemaHash';
import { toolToRow, parseEnvelope, demandsSession } from '../workers/collect-mcp-tools';

const fails: string[] = [];
const check = (label: string, pass: boolean, detail = ''): void => {
  console.log(`${pass ? 'PASS' : '*** FAIL ***'}  ${label}${detail ? '  -- ' + detail : ''}`);
  if (!pass) fails.push(label);
};

/* ---------------------------------------------------------------------------
 * 1 + 2. schema_hash stability and sensitivity
 * ------------------------------------------------------------------------ */
console.log('\n-- schema_hash: stable against key order and whitespace --');

// Same schema, keys in a different order, and pretty-printed vs minified.
const A = JSON.parse(`{
  "type": "object",
  "properties": {
    "query":  { "type": "string", "description": "what to search for" },
    "limit":  { "type": "integer", "minimum": 1 }
  },
  "required": ["query"]
}`);
const B = JSON.parse(
  '{"required":["query"],"properties":{"limit":{"minimum":1,"type":"integer"},' +
  '"query":{"description":"what to search for","type":"string"}},"type":"object"}',
);

const hA = schemaHash(A), hB = schemaHash(B);
console.log(`  A: ${hA}`);
console.log(`  B: ${hB}`);
check('key reorder + whitespace does not move schema_hash', hA === hB);
check('contract_hash likewise', contractHash(A) === contractHash(B));

// required is a SET, so its element order must not matter either.
const twoReq = { type: 'object', properties: { a: { type: 'string' }, b: { type: 'string' } }, required: ['a', 'b'] };
const twoReqRev = { type: 'object', properties: { b: { type: 'string' }, a: { type: 'string' } }, required: ['b', 'a'] };
check('required[] element order does not move schema_hash', schemaHash(twoReq) === schemaHash(twoReqRev));

// enum is a SET too.
const e1 = { type: 'string', enum: ['a', 'b', 'c'] };
const e2 = { type: 'string', enum: ['c', 'a', 'b'] };
check('enum[] element order does not move schema_hash', schemaHash(e1) === schemaHash(e2));

// A positional array must NOT be reordered.
const p1 = { type: 'array', prefixItems: [{ type: 'string' }, { type: 'integer' }] };
const p2 = { type: 'array', prefixItems: [{ type: 'integer' }, { type: 'string' }] };
check('prefixItems order IS significant (hashes differ)', schemaHash(p1) !== schemaHash(p2));

console.log('\n-- schema_hash: moves on a real change --');
const addRequired = { ...A, required: ['query', 'limit'] };
check('adding a required param moves schema_hash', schemaHash(A) !== schemaHash(addRequired));
check('adding a required param moves contract_hash', contractHash(A) !== contractHash(addRequired),
  'this is the item 3 signal');

const typeChanged = JSON.parse(JSON.stringify(A));
typeChanged.properties.limit.type = 'string';
check('changing a param type moves schema_hash', schemaHash(A) !== schemaHash(typeChanged));
check('changing a param type moves contract_hash', contractHash(A) !== contractHash(typeChanged),
  'the other item 3 signal');

const propAdded = JSON.parse(JSON.stringify(A));
propAdded.properties.offset = { type: 'integer' };
check('adding an optional property moves both hashes',
  schemaHash(A) !== schemaHash(propAdded) && contractHash(A) !== contractHash(propAdded));

/* ---------------------------------------------------------------------------
 * 3. contract_hash ignores annotation-only edits
 * ------------------------------------------------------------------------ */
console.log('\n-- contract_hash: unchanged by a description-only edit --');
const descEdited = JSON.parse(JSON.stringify(A));
descEdited.properties.query.description = 'the search string (rewritten for clarity)';
console.log(`  schema_hash   before ${schemaHash(A).slice(0, 16)}  after ${schemaHash(descEdited).slice(0, 16)}`);
console.log(`  contract_hash before ${contractHash(A).slice(0, 16)}  after ${contractHash(descEdited).slice(0, 16)}`);
check('description edit MOVES schema_hash (general drift is real)', schemaHash(A) !== schemaHash(descEdited));
check('description edit does NOT move contract_hash', contractHash(A) === contractHash(descEdited),
  'so item 3 will not score a Breaking Change for a docstring fix');

const titleEdited = JSON.parse(JSON.stringify(A));
titleEdited.title = 'Search parameters';
check('adding a title does not move contract_hash', contractHash(A) === contractHash(titleEdited));

// x-mcp-header is transport contract, NOT prose, and must survive into the hash.
const hdr = JSON.parse(JSON.stringify(A));
hdr.properties.query['x-mcp-header'] = 'X-Query';
check('x-mcp-header DOES move contract_hash (it is transport contract)',
  contractHash(A) !== contractHash(hdr));

/* ---------------------------------------------------------------------------
 * 4. false-drift classes, detected and counted
 * ------------------------------------------------------------------------ */
console.log('\n-- false-drift classes: detected, counted, not filtered --');

const unionInline = { type: 'object', properties: { v: { type: ['string', 'null'] } } };
const unionComposed = { type: 'object', properties: { v: { anyOf: [{ type: 'string' }, { type: 'null' }] } } };
const dA = falseDriftClasses(unionInline);
const dB = falseDriftClasses(unionComposed);
console.log(`  inline union   -> ${JSON.stringify(dA)}`);
console.log(`  composed union -> ${JSON.stringify(dB)}`);
check('inline type-union detected', dA.typeUnion && !dA.unionViaComposition);
check('composition union detected', dB.unionViaComposition && !dB.typeUnion);
check('the two spellings hash DIFFERENTLY (the false drift being measured)',
  schemaHash(unionInline) !== schemaHash(unionComposed),
  'no canonical form exists without a full JSON Schema normaliser');

const refd = { type: 'object', properties: { a: { $ref: '#/$defs/thing' } }, $defs: { thing: { type: 'string' } } };
const inlined = { type: 'object', properties: { a: { type: 'string' } } };
const dR = falseDriftClasses(refd);
check('$ref detected', dR.hasRef);
check('$defs detected', dR.hasDefs);
check('inline vs $ref hash differently (the other false drift)',
  schemaHash(refd) !== schemaHash(inlined));
check('$ref is NOT resolved (no network, no inlining)',
  JSON.stringify(canonicalise(refd, false)).includes('$ref'));

const totals = tallyFalseDrift([dA, dB, dR, falseDriftClasses(inlined)]);
console.log(`  tally over 4 fixtures: ${JSON.stringify(totals)}`);
check('tally counts each class independently',
  totals.schemas === 4 && totals.typeUnion === 1 && totals.unionViaComposition === 1 &&
  totals.hasRef === 1 && totals.hasDefs === 1);

/* ---------------------------------------------------------------------------
 * shape features for items 19/21/39
 * ------------------------------------------------------------------------ */
console.log('\n-- param_set / counts / depth --');
console.log(`  paramSet(A) = ${JSON.stringify(paramSet(A))}`);
check('paramSet carries name, type and required for each top-level param',
  JSON.stringify(paramSet(A)) === JSON.stringify([
    { name: 'limit', type: 'integer', required: false },
    { name: 'query', type: 'string', required: true },
  ]));
check('paramSet renders a type union as a sorted join',
  paramSet(unionInline)[0].type === 'null|string');
check('paramCount / requiredCount', paramCount(A) === 2 && requiredCount(A) === 1);
const deep = { type: 'object', properties: { a: { type: 'object', properties: { b: { type: 'object', properties: { c: { type: 'string' } } } } } } };
console.log(`  maxDepth(flat)=${maxDepth(A)}  maxDepth(3-level)=${maxDepth(deep)}`);
check('maxDepth increases with nesting', maxDepth(deep) > maxDepth(A));

/* ---------------------------------------------------------------------------
 * toolToRow, on a realistic Tool object
 * ------------------------------------------------------------------------ */
console.log('\n-- toolToRow --');
const tool = {
  name: 'search_docs',
  title: 'Search documentation',
  description: 'Full-text search over the docs corpus.',
  inputSchema: A,
  outputSchema: { type: 'object', properties: { hits: { type: 'array' } } },
  annotations: { readOnlyHint: true },
};
const row = toolToRow(tool);
console.log(`  name=${row.tool_name} params=${row.param_count} required=${row.required_count} depth=${row.max_depth}`);
console.log(`  schema_hash=${row.schema_hash?.slice(0, 16)} contract_hash=${row.contract_hash?.slice(0, 16)}`);
check('toolToRow fills identity and shape', row.tool_name === 'search_docs' && row.param_count === 2 && row.required_count === 1);
check('toolToRow keeps schemas verbatim', JSON.stringify(row.input_schema) === JSON.stringify(A));
check('toolToRow records token estimate', (row.token_estimate ?? 0) > 0);

const noSchema = toolToRow({ name: 'weird' });
check('a tool with no inputSchema hashes to NULL, not to the string "null"',
  noSchema.schema_hash === null && noSchema.contract_hash === null,
  'so a missing schema cannot collide with a literal null schema');

/* ---------------------------------------------------------------------------
 * envelope parsing and session detection
 * ------------------------------------------------------------------------ */
console.log('\n-- envelope parsing --');
const plain = '{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}';
check('plain JSON parses', Array.isArray(parseEnvelope(plain, 'application/json')?.result?.tools));
const sse = `event: message\ndata: ${plain}\n\n`;
check('SSE framing parses', Array.isArray(parseEnvelope(sse, 'text/event-stream')?.result?.tools));
check('garbage returns null, never throws', parseEnvelope('not json', 'application/json') === null);
check('session demand recognised', demandsSession('Mcp-Session-Id required') === true);
check('unrelated message is not a session demand', demandsSession('rate limited') === false);

/* ---------------------------------------------------------------------------
 * wire form: no initialize, no session
 * ------------------------------------------------------------------------ */
console.log('\n-- wire form --');
const src = require('node:fs').readFileSync(require('node:path').join(__dirname, '../workers/collect-mcp-tools.ts'), 'utf8');
const code = src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
check('no "initialize" anywhere in collector CODE', !/initialize/i.test(code),
  'comments may mention it; code must not');
check('no Mcp-Session-Id header sent or read', !/mcp-session-id/i.test(code));
check('no Authorization header', !/authorization/i.test(code));
check('no tools/call', !/tools\/call/i.test(code));
check('Mcp-Method is tools/list', /'Mcp-Method': TOOLS_METHOD/.test(src) && /TOOLS_METHOD = 'tools\/list'/.test(src));
check('MAX_PAGES is 20 and truncation is recorded', /MAX_PAGES = 20/.test(src) && /truncated = true/.test(src));

console.log(`\n${fails.length === 0 ? 'All fixture checks passed.' : 'FAILED: ' + fails.join(' | ')}`);
process.exit(fails.length === 0 ? 0 : 1);
