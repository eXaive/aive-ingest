/**
 * Asserts the SQL insertRows emits, with particular attention to ON CONFLICT.
 *
 * WHY THIS EXISTS. Run #37 probed everything and persisted ZERO rows. The
 * statement failed at PLAN time with 42P10, "there is no unique or exclusion
 * constraint matching the ON CONFLICT specification", because
 * mcp_endpoint_probes_run_endpoint_kind_unique is a PARTIAL index and a bare
 * ON CONFLICT (cols) cannot infer one. Postgres only matches a partial index
 * when the statement repeats the index predicate.
 *
 * The suite was fully green when that shipped. Every test double for
 * insertRows ignores the conflict arguments and never builds SQL, so nothing
 * in the repo looked at the one string that was wrong. This script does.
 *
 * OFFLINE by default. When AIVE_INGEST_DATABASE_URL is set it ALSO asks a real
 * Postgres to plan both forms, which is the only way to prove the clause
 * actually matches the index rather than merely looking like it does.
 *
 * Run: npx tsx scripts/verify-insert-conflict-sql.ts
 */

import assert from 'node:assert/strict';
import { buildInsertSql } from '../lib/ingest/db';

/* Verbatim from aive-platform migration 20260904150000_mcp_probe_run_evidence.sql:
     CREATE UNIQUE INDEX mcp_endpoint_probes_run_endpoint_kind_unique
       ON public.mcp_endpoint_probes (probe_run_id, server_id, endpoint_url, observation_kind)
       WHERE probe_run_id IS NOT NULL;                                              */
const ARBITER_COLS = 'probe_run_id, server_id, endpoint_url, observation_kind';
const ARBITER_WHERE = 'probe_run_id IS NOT NULL';

const PROBE_COLS = ['server_id', 'endpoint_url', 'probe_run_id', 'observation_kind'];

let checks = 0;
const ok = (label: string) => { checks++; console.log(`  PASS  ${label}`); };

// ── 1. The probe call site's exact statement ───────────────────────────────
{
  const sql = buildInsertSql('mcp_endpoint_probes', PROBE_COLS, 2, ARBITER_COLS, ARBITER_WHERE);
  assert.equal(
    sql,
    'INSERT INTO mcp_endpoint_probes (server_id,endpoint_url,probe_run_id,observation_kind) ' +
    'VALUES ($1,$2,$3,$4),($5,$6,$7,$8) ' +
    'ON CONFLICT (probe_run_id, server_id, endpoint_url, observation_kind) ' +
    'WHERE probe_run_id IS NOT NULL DO NOTHING',
  );
  // The predicate must sit between the target and DO NOTHING -- that ORDER is
  // the whole fix; anywhere else is a syntax error or a different statement.
  assert.match(sql, /ON CONFLICT \([^)]+\) WHERE probe_run_id IS NOT NULL DO NOTHING$/);
  ok('probe INSERT carries the partial-index predicate, in the right place');
}

// ── 2. The clause mirrors the migration's index exactly ────────────────────
{
  const sql = buildInsertSql('mcp_endpoint_probes', PROBE_COLS, 1, ARBITER_COLS, ARBITER_WHERE);
  const m = /ON CONFLICT \(([^)]+)\) WHERE (.+) DO NOTHING$/.exec(sql);
  assert.ok(m, 'conflict clause is parseable');
  assert.deepEqual(
    m[1].split(',').map((s) => s.trim()),
    ['probe_run_id', 'server_id', 'endpoint_url', 'observation_kind'],
    'column list matches the index, in index order',
  );
  assert.equal(m[2], 'probe_run_id IS NOT NULL', 'predicate matches the index predicate');
  ok('conflict target and predicate match the migration index definition');
}

// ── 3. THE REVERSE REGRESSION: non-conflict callers stay untouched ─────────
{
  const plain = buildInsertSql('mcp_servers', ['name', 'version'], 3);
  assert.equal(plain, 'INSERT INTO mcp_servers (name,version) VALUES ($1,$2),($3,$4),($5,$6)');
  assert.doesNotMatch(plain, /ON CONFLICT/, 'no conflict clause without a target');
  assert.doesNotMatch(plain, /WHERE/, 'no stray WHERE');
  ok('a caller passing no conflictTarget emits a bare INSERT');
}

// ── 4. Target without predicate still works (non-partial indexes) ──────────
{
  const sql = buildInsertSql('some_table', ['a', 'b'], 1, 'a, b');
  assert.equal(sql, 'INSERT INTO some_table (a,b) VALUES ($1,$2) ON CONFLICT (a, b) DO NOTHING');
  assert.doesNotMatch(sql, /WHERE/, 'no WHERE when no predicate is given');
  ok('conflictTarget alone still emits the bare form, for total indexes');
}

// ── 5. Predicate without target is a caller bug, not a silent plain INSERT ─
{
  assert.throws(
    () => buildInsertSql('t', ['a'], 1, undefined, 'a IS NOT NULL'),
    /conflictWhere given without conflictTarget/,
  );
  ok('conflictWhere without conflictTarget throws instead of dropping the clause');
}

// ── 6. Placeholder numbering is unaffected by the conflict clause ──────────
{
  const a = buildInsertSql('t', ['x', 'y'], 2);
  const b = buildInsertSql('t', ['x', 'y'], 2, 'x', 'x IS NOT NULL');
  assert.ok(a.includes('VALUES ($1,$2),($3,$4)'));
  assert.ok(b.includes('VALUES ($1,$2),($3,$4)'));
  ok('parameter placeholders identical with and without a conflict clause');
}

// ── 7. LIVE: does Postgres actually pick the arbiter index? ────────────────
async function live(): Promise<void> {
  const url = process.env.AIVE_INGEST_DATABASE_URL?.trim();
  if (!url) {
    console.log('\n  SKIPPED (live): AIVE_INGEST_DATABASE_URL not set — offline assertions only.');
    console.log('  NOTE: only the live check can prove the clause MATCHES the index rather');
    console.log('        than merely resembling it. Run it before trusting a conflict change.');
    return;
  }
  const { q, endIngestPool } = await import('../lib/ingest/db');
  const stmt = (onConflict: string) =>
    `EXPLAIN INSERT INTO public.mcp_endpoint_probes
       (server_id, endpoint_url, probed_at, error_class, probe_run_id, observation_kind)
     SELECT gen_random_uuid(), 'https://verify.invalid', now(), 'ok', gen_random_uuid(), 'REACHABILITY'
     ${onConflict}`;

  // The broken form must still be rejected — if this ever starts working the
  // index has changed and this script's premise needs revisiting.
  let bareFailed = false;
  try {
    await q(stmt(`ON CONFLICT (${ARBITER_COLS}) DO NOTHING`));
  } catch (e) {
    bareFailed = /42P10|no unique or exclusion constraint/i.test(String((e as Error).message));
  }
  assert.ok(bareFailed, 'a bare ON CONFLICT must still fail against the partial index');
  ok('live: bare ON CONFLICT still rejected with 42P10 (the run #37 failure)');

  const plan = await q<{ 'QUERY PLAN': string }>(
    stmt(`ON CONFLICT (${ARBITER_COLS}) WHERE ${ARBITER_WHERE} DO NOTHING`));
  const text = plan.map((r) => r['QUERY PLAN']).join('\n');
  assert.match(text, /Conflict Arbiter Indexes: mcp_endpoint_probes_run_endpoint_kind_unique/);
  assert.match(text, /Conflict Resolution: NOTHING/);
  ok('live: predicate form plans, arbiter = mcp_endpoint_probes_run_endpoint_kind_unique');
  await endIngestPool();
}

live()
  .then(() => console.log(`\ninsertRows ON CONFLICT SQL contract: ${checks} checks passed`))
  .catch((e) => { console.error(e); process.exit(1); });
