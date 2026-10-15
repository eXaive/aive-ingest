/**
 * Asserts that inserts stay under Postgres's 65,535 bind-parameter cap.
 *
 * WHY THIS EXISTS. MCP Listing Collection #63-#65 (2026-10-12..14) failed on
 * one endpoint every day: immigrationdb.com/mcp listed 10,000 resources, the
 * resources collector sent them as ONE insert (10,000 x 11 = 110,000 params),
 * the protocol's 16-bit count wrapped to 44,464 and Postgres refused it with
 * "bind message has 44464 parameter formats but 0 parameters".
 *
 * Covers both insert paths:
 *   1. insertRows (lib/ingest/db.ts) -- shared by the resources and prompts
 *      collectors and the registry/governance ingests.
 *   2. persistCaptureAtomically (workers/collect-mcp-tools.ts) -- the tools
 *      collector's own insert, which must stay inside ONE transaction.
 *
 * NO DATABASE. pg.Pool.prototype.query is intercepted and the tools path gets a
 * fake client, so nothing connects and nothing is written.
 *
 * Run: npx tsx scripts/verify-insert-rows-chunking.ts
 */

import assert from 'node:assert/strict';
import pg from 'pg';
import { insertRows, PG_MAX_BIND_PARAMS } from '../lib/ingest/db';
import { persistCaptureAtomically, type CaptureRow } from '../workers/collect-mcp-tools';

process.env.AIVE_INGEST_DATABASE_URL ??= 'postgres://verify:verify@127.0.0.1:5432/verify';

// ── capture statements; rowCount = tuples in the statement, minus any skipped ──
const emitted: { sql: string; values: unknown[] }[] = [];
let skipPerStatement = 0;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
(pg.Pool.prototype as any).query = async function capture(sql: unknown, values: unknown[]) {
  const text = String(sql);
  emitted.push({ sql: text, values: values ?? [] });
  const tuples = (text.match(/\),\(/g)?.length ?? 0) + 1;
  return { rows: [], rowCount: tuples - skipPerStatement };
};

let checks = 0;
const ok = (label: string) => { checks++; console.log(`  PASS  ${label}`); };

const cols = (n: number) => Array.from({ length: n }, (_, i) => `c${i}`);
const rows = (count: number, width: number) =>
  Array.from({ length: count }, (_, r) => Array.from({ length: width }, (_, c) => `${r}:${c}`));

/** Every statement is under the cap, numbers its placeholders from $1, and
 *  together they carry every value exactly once, in order. */
function assertWellFormed(expectedRows: unknown[][], width: number) {
  for (const { sql, values } of emitted) {
    assert.ok(values.length <= PG_MAX_BIND_PARAMS, `statement carries ${values.length} params`);
    assert.equal(values.length % width, 0);
    assert.ok(sql.includes('VALUES ($1,'), 'placeholders restart at $1 in every statement');
    assert.ok(sql.includes(`$${values.length})`), 'last placeholder matches the value count');
    assert.ok(!sql.includes(`$${values.length + 1}`), 'no placeholder past the value count');
  }
  assert.deepEqual(emitted.flatMap((e) => e.values), expectedRows.flat());
}

async function main(): Promise<void> {
  // ── The incident: 10,000 rows x 11 columns ─────────────────────────────────
  {
    emitted.length = 0;
    const data = rows(10_000, 11);
    const inserted = await insertRows('mcp_resources', cols(11), data);
    assert.equal(inserted, 10_000, 'return value is the total across chunks');
    assert.equal(emitted.length, 2, '5,957 + 4,043 rows');
    assert.deepEqual(emitted.map((e) => e.values.length / 11), [5957, 4043]);
    assertWellFormed(data, 11);
    ok('10,000 rows x 11 cols insert fully in 2 statements and return 10,000');
  }

  // ── Empty input ────────────────────────────────────────────────────────────
  {
    emitted.length = 0;
    assert.equal(await insertRows('mcp_resources', cols(11), []), 0);
    assert.equal(emitted.length, 0, 'no statement for no rows');
    ok('empty array returns 0 and sends nothing');
  }

  // ── Boundary: exactly the most rows that fit ───────────────────────────────
  {
    // 11 columns: floor(65535 / 11) = 5,957 rows = 65,527 params.
    emitted.length = 0;
    let data = rows(5957, 11);
    assert.equal(await insertRows('t', cols(11), data), 5957);
    assert.equal(emitted.length, 1, 'at the boundary: one statement');
    assertWellFormed(data, 11);

    emitted.length = 0;
    data = rows(5958, 11);
    assert.equal(await insertRows('t', cols(11), data), 5958);
    assert.equal(emitted.length, 2, 'one past the boundary: two statements');
    assertWellFormed(data, 11);

    // 5 columns divide the cap exactly: 13,107 rows = 65,535 params.
    emitted.length = 0;
    data = rows(13_107, 5);
    assert.equal(await insertRows('t', cols(5), data), 13_107);
    assert.equal(emitted.length, 1);
    assert.equal(emitted[0].values.length, PG_MAX_BIND_PARAMS, 'exactly 65,535 params is accepted as one statement');
    assertWellFormed(data, 5);

    emitted.length = 0;
    data = rows(13_108, 5);
    assert.equal(await insertRows('t', cols(5), data), 13_108);
    assert.equal(emitted.length, 2);
    assertWellFormed(data, 5);
    ok('row counts exactly at the boundary use one statement; one more row splits');
  }

  // ── Conflict clause survives chunking; return value is ACTUAL inserts ──────
  {
    emitted.length = 0;
    skipPerStatement = 1;
    const inserted = await insertRows('t', cols(11), rows(10_000, 11), 'c0', 'c0 IS NOT NULL');
    skipPerStatement = 0;
    assert.equal(emitted.length, 2);
    for (const { sql } of emitted) assert.match(sql, / ON CONFLICT \(c0\) WHERE c0 IS NOT NULL DO NOTHING$/);
    assert.equal(inserted, 9_998, 'sums each chunk\'s rowCount, so conflict-skipped rows are not counted');
    ok('every chunk keeps the ON CONFLICT clause; return value sums actual inserts');
  }

  // ── Small batches are byte-for-byte unchanged ──────────────────────────────
  {
    emitted.length = 0;
    await insertRows('mcp_servers', ['name', 'version'], [['a', '1'], ['b', '2']]);
    assert.equal(emitted.length, 1);
    assert.equal(emitted[0].sql, 'INSERT INTO mcp_servers (name,version) VALUES ($1,$2),($3,$4)');
    ok('a batch under the cap emits the same single statement as before');
  }

  // ── Tools collector: chunked, still one transaction per capture ────────────
  {
    const calls: { sql: string; params: number }[] = [];
    let acquired = 0;
    const acquire = (failOnChunk?: number) => async () => {
      acquired++;
      let chunk = 0;
      return {
        async query(sql: string, values?: unknown[]) {
          calls.push({ sql, params: values?.length ?? 0 });
          if (/INSERT INTO mcp_tool_captures/.test(sql)) return { rows: [{ id: 'capture-one' }] };
          if (/INSERT INTO mcp_tools /.test(sql) && ++chunk === failOnChunk) throw new Error('chunk refused');
          return { rows: [] };
        },
        release() { calls.push({ sql: 'RELEASE', params: 0 }); },
      };
    };
    const kind = (sql: string) =>
      /INSERT INTO mcp_tool_captures/.test(sql) ? 'CAPTURE' : /INSERT INTO mcp_tools /.test(sql) ? 'TOOLS' : sql;

    // 20 columns: floor(65535 / 20) = 3,276 tools per statement.
    await persistCaptureAtomically(capture(4000), 'run', 'compat', acquire());
    assert.equal(acquired, 1, 'one client for the whole capture');
    assert.deepEqual(calls.map((c) => kind(c.sql)), ['BEGIN', 'CAPTURE', 'TOOLS', 'TOOLS', 'COMMIT', 'RELEASE']);
    const toolParams = calls.filter((c) => kind(c.sql) === 'TOOLS').map((c) => c.params);
    assert.deepEqual(toolParams, [3276 * 20, 724 * 20]);
    for (const c of calls.filter((x) => kind(x.sql) === 'TOOLS')) {
      assert.ok(c.sql.includes('VALUES ($1,'), 'tool placeholders restart at $1 per chunk');
      assert.ok(!c.sql.includes(`$${c.params + 1}`));
    }
    ok('4,000 tools: 2 chunks between one BEGIN and one COMMIT on one client');

    calls.length = 0; acquired = 0;
    await assert.rejects(() => persistCaptureAtomically(capture(4000), 'run', 'compat', acquire(2)), /chunk refused/);
    assert.deepEqual(calls.map((c) => kind(c.sql)), ['BEGIN', 'CAPTURE', 'TOOLS', 'TOOLS', 'ROLLBACK', 'RELEASE']);
    ok('a failing later chunk rolls back the capture and the earlier chunk');

    calls.length = 0;
    await persistCaptureAtomically(capture(0), 'run', 'compat', acquire());
    assert.deepEqual(calls.map((c) => kind(c.sql)), ['BEGIN', 'CAPTURE', 'COMMIT', 'RELEASE']);
    ok('a capture with zero tools sends no tool insert');
  }

  console.log(`\nbind-parameter chunking: ${checks} checks passed`);
}

function capture(tools: number): CaptureRow {
  return {
    server_id: '00000000-0000-4000-8000-000000000001', endpoint_url: 'https://big.invalid/mcp',
    captured_at: '2026-10-14T00:00:00.000Z', status: 'ok', http_status: 200,
    response_time_ms: 1, error_class: 'ok', tool_count: tools, page_count: 1,
    truncated: false, raw_json: [{ tools: [] }], raw_bytes: 2, token_estimate: 1,
    cache_ttl_ms: null, protocol_version: '2026-07-28', list_changed: null,
    note: null,
    tools: Array.from({ length: tools }, (_, i) => ({
      tool_name: `tool_${i}`, tool_title: null, description: null,
      input_schema: { type: 'object' }, output_schema: null,
      schema_hash: 'a'.repeat(64), contract_hash: 'b'.repeat(64), param_set: [],
      param_count: 0, required_count: 0, max_depth: 1, type_union: false,
      union_via_composition: false, has_ref: false, has_defs: false,
      annotations: null, token_estimate: 1,
    })),
  };
}

main().catch((e) => { console.error(e instanceof Error ? e.stack : e); process.exit(1); });
