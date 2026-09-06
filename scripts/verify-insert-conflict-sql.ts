/**
 * Asserts the INSERT ... ON CONFLICT SQL that insertRows actually emits, and
 * that the probe worker still passes the partial index's predicate.
 *
 * WHY THIS EXISTS. The same defect shipped GREEN TWICE.
 * mcp_endpoint_probes_run_endpoint_kind_unique is a PARTIAL index:
 *
 *   CREATE UNIQUE INDEX mcp_endpoint_probes_run_endpoint_kind_unique
 *     ON public.mcp_endpoint_probes
 *        (probe_run_id, server_id, endpoint_url, observation_kind)
 *     WHERE probe_run_id IS NOT NULL;
 *
 * Postgres infers the arbiter index from the conflict target, and a partial
 * index is only inferable when the statement REPEATS its predicate. A bare
 * ON CONFLICT (cols) fails at PLAN time with 42P10, "there is no unique or
 * exclusion constraint matching the ON CONFLICT specification" -- so every
 * insert chunk died and run #38 wrote 0 rows across 32 chunks.
 *
 * Nothing in the repo looked at the emitted SQL, so the suite was green both
 * times. This script looks at it.
 *
 * TWO LAYERS, DELIBERATELY. Asserting insertRows alone would still pass if
 * someone dropped the predicate at the CALL SITE, which is exactly how the
 * second occurrence happened:
 *   1. SOURCE  -- the probe worker passes both the target and the predicate,
 *                 and both match the migration's index definition.
 *   2. EMITTED -- insertRows turns those arguments into the expected SQL.
 *
 * NO DATABASE. pg.Pool.prototype.query is intercepted to capture the statement,
 * so nothing connects and nothing is written. Production code is unchanged --
 * this file adds no seam to lib/ingest/db.ts.
 *
 * Run: npx tsx scripts/verify-insert-conflict-sql.ts
 */

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import pg from 'pg';
import { insertRows } from '../lib/ingest/db';

/* Must be set before lib/ingest/db.ts builds a pool. The host is never dialled:
   node-pg connects lazily and query() is intercepted below. */
process.env.AIVE_INGEST_DATABASE_URL ??= 'postgres://verify:verify@127.0.0.1:5432/verify';

// ── capture the SQL instead of executing it ────────────────────────────────
const emitted: { sql: string; values: unknown[] }[] = [];
// eslint-disable-next-line @typescript-eslint/no-explicit-any
(pg.Pool.prototype as any).query = async function capture(sql: unknown, values: unknown[]) {
  emitted.push({ sql: String(sql), values: values ?? [] });
  return { rows: [], rowCount: Array.isArray(values) ? 1 : 0 };
};


const WORKER = 'workers/probe-mcp-endpoints.ts';
const MIGRATION_HINT = 'aive-platform migration 20260904150000_mcp_probe_run_evidence.sql';

/* The arbiter index, transcribed from that migration. If the index is ever
   changed, these two constants must change with it -- and the source assertions
   below will fail until the worker is updated to match. */
const ARBITER_COLS = 'probe_run_id, server_id, endpoint_url, observation_kind';
const ARBITER_WHERE = 'probe_run_id IS NOT NULL';

let checks = 0;
const ok = (label: string) => { checks++; console.log(`  PASS  ${label}`); };

async function main(): Promise<void> {
  // ── LAYER 1: the worker still passes both halves ───────────────────────────
  {
    const source = readFileSync(WORKER, 'utf8');

    // The probe insert call, with its two trailing conflict arguments.
    const call = /dependencies\.insert\(\s*'mcp_endpoint_probes'[\s\S]*?\]\)\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*\)/.exec(source);
    assert.ok(
      call,
      `${WORKER}: could not find the mcp_endpoint_probes insert passing BOTH a conflict target and a predicate.\n` +
      `      A bare ON CONFLICT cannot bind the partial index (${MIGRATION_HINT}) and fails with 42P10 on every chunk.`,
    );
    assert.equal(call[1], ARBITER_COLS, 'conflict target must be the index columns, in index order');
    assert.equal(call[2], ARBITER_WHERE, 'conflict predicate must repeat the partial index WHERE clause');
    ok('probe worker passes the index columns AND the partial-index predicate');
  }

  // ── LAYER 2: those arguments produce the expected statement ────────────────
  {
    emitted.length = 0;
    await insertRows(
      'mcp_endpoint_probes',
      ['server_id', 'endpoint_url', 'probe_run_id', 'observation_kind'],
      [['s1', 'https://a.example', 'r1', 'REACHABILITY'],
       ['s2', 'https://b.example', 'r1', 'REACHABILITY']],
      ARBITER_COLS,
      ARBITER_WHERE,
    );
    assert.equal(emitted.length, 1, 'one statement');
    const { sql } = emitted[0];

    assert.equal(
      sql,
      'INSERT INTO mcp_endpoint_probes (server_id,endpoint_url,probe_run_id,observation_kind) ' +
      'VALUES ($1,$2,$3,$4),($5,$6,$7,$8) ' +
      'ON CONFLICT (probe_run_id, server_id, endpoint_url, observation_kind) ' +
      'WHERE probe_run_id IS NOT NULL DO NOTHING',
    );
    /* ORDER MATTERS: the predicate belongs between the target and DO NOTHING.
       Anywhere else is a syntax error or a different statement. */
    assert.match(sql, /ON CONFLICT \([^)]+\) WHERE probe_run_id IS NOT NULL DO NOTHING$/);
    ok('emitted SQL carries the predicate, in the position Postgres requires');
  }

  // ── The regression, stated as its own check ───────────────────────────────
  {
    emitted.length = 0;
    await insertRows('mcp_endpoint_probes', ['a'], [['x']], ARBITER_COLS); // predicate omitted
    const { sql } = emitted[0];
    assert.doesNotMatch(sql, /WHERE/, 'omitting the predicate must produce a bare clause');
    assert.match(sql, /ON CONFLICT \([^)]+\) DO NOTHING$/);
    ok('omitting the predicate yields the 42P10 shape — the exact bug, reproduced as a contract');
  }

  // ── The other seven callers must be untouched ─────────────────────────────
  {
    emitted.length = 0;
    await insertRows('mcp_servers', ['name', 'version'], [['a', '1'], ['b', '2']]);
    const { sql } = emitted[0];
    assert.equal(sql, 'INSERT INTO mcp_servers (name,version) VALUES ($1,$2),($3,$4)');
    assert.doesNotMatch(sql, /ON CONFLICT/, 'no conflict clause without a target');
    assert.doesNotMatch(sql, /WHERE/, 'no stray WHERE');
    ok('a caller passing no conflict arguments emits a bare INSERT, unchanged');
  }

  // ── Placeholder numbering is unaffected by the clause ─────────────────────
  {
    emitted.length = 0;
    await insertRows('t', ['x', 'y'], [['1', '2'], ['3', '4']]);
    const plain = emitted[0].sql;
    emitted.length = 0;
    await insertRows('t', ['x', 'y'], [['1', '2'], ['3', '4']], 'x', 'x IS NOT NULL');
    const withClause = emitted[0].sql;
    assert.ok(plain.includes('VALUES ($1,$2),($3,$4)'));
    assert.ok(withClause.includes('VALUES ($1,$2),($3,$4)'));
    ok('parameter placeholders identical with and without the conflict clause');
  }

  console.log(`\ninsertRows ON CONFLICT SQL contract: ${checks} checks passed`);
  console.log('NOTE: this asserts the SQL TEXT and the call site. It cannot prove the');
  console.log('      index still exists — that is checked by a live EXPLAIN, see the');
  console.log(`      arbiter definition in ${MIGRATION_HINT}.`);

}

main().catch((e) => { console.error(e instanceof Error ? e.message : e); process.exit(1); });
