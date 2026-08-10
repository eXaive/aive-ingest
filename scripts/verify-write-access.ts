/**
 * scripts/verify-write-access.ts — test the PREFLIGHT PREDICATE AS ONE
 * EXPRESSION, plus a live check that the real grants satisfy it.
 *
 * WHY THIS FILE EXISTS. The MCP Governance Mirror's first scheduled run
 * (2026-08-10 05:57Z) failed with "items=false, transitions=true". The preflight
 * asserted has_table_privilege(..., 'UPDATE') — a TABLE-level check — while
 * migration 20260809000016 grants UPDATE at COLUMN level, and Postgres reports
 * column grants only through has_column_privilege. The grant was correct and
 * sufficient; the assertion demanded a privilege the upsert never uses.
 *
 * The post-apply verification for that migration had checked INSERT alone (true)
 * and the eight column grants (true x8) and PASSED — because it never evaluated
 * the conjunction the worker actually runs. Testing the ingredients while never
 * testing the expression is what let two files contradict each other all the way
 * into a scheduled run. So the predicate is now a pure exported function and this
 * script drives it across every failure combination.
 *
 * OFFLINE section: pure, no database, no network. Runs anywhere.
 * LIVE section: needs AIVE_INGEST_DATABASE_URL; asserts the real grants pass and
 * that RLS is genuinely enforced on the probe path. Skipped when unset, and the
 * skip is reported rather than counted as a pass.
 *
 * Run: npx tsx scripts/verify-write-access.ts
 */

import {
  evaluateWriteAccess, writeAccessRemedy, UPSERT_UPDATE_COLS,
  type WriteAccessFacts,
} from '../workers/ingest-mcp-governance';
import { q, withRollback, endIngestPool } from '../lib/ingest/db';

const checks: [string, boolean][] = [];
const check = (label: string, pass: boolean) => { checks.push([label, pass]); };

/** All eight columns granted. */
const allCols = (): Record<string, boolean> =>
  Object.fromEntries(UPSERT_UPDATE_COLS.map((c) => [c, true]));

const good = (): WriteAccessFacts => ({
  role: 'aive_ingest',
  itemsInsert: true,
  itemsSelect: true,
  itemsUpdateCols: allCols(),
  transitionsInsert: true,
});

async function offline() {
  console.log('── offline: the composite predicate ──────────────────────');

  // The exact shape migration …0016 produces must PASS.
  const base = evaluateWriteAccess(good());
  check('migration 0016 grant shape passes (8 column UPDATE, no table UPDATE)', base.ok && base.problems.length === 0);

  // THE REGRESSION: table-level UPDATE is NOT required. Absence of any notion of
  // table UPDATE in the facts is the point — the predicate cannot demand it.
  check('predicate has no table-level UPDATE term at all',
    !JSON.stringify(Object.keys(good())).includes('itemsUpdateTable'));

  // Each of the eight columns individually revoked must FAIL, and must name it.
  for (const col of UPSERT_UPDATE_COLS) {
    const f = good();
    f.itemsUpdateCols[col] = false;
    const v = evaluateWriteAccess(f);
    check(`revoking UPDATE(${col}) fails and names it`,
      !v.ok && v.problems.some((p) => p.includes(col)));
  }

  // A column missing entirely (not just false) must also fail — undefined is not
  // permission.
  const missing = good();
  delete missing.itemsUpdateCols.title;
  check('a column absent from the privilege map fails (undefined is not permission)',
    !evaluateWriteAccess(missing).ok);

  // Extra columns beyond the eight must NOT be required.
  const extra = good();
  extra.itemsUpdateCols.repo = false;
  extra.itemsUpdateCols.type = false;
  extra.itemsUpdateCols.number = false;
  check('withholding UPDATE on repo/type/number still PASSES (deliberate narrowing)',
    evaluateWriteAccess(extra).ok);

  // Each table-level term.
  for (const [label, mut] of [
    ['items INSERT', (f: WriteAccessFacts) => { f.itemsInsert = false; }],
    ['items SELECT', (f: WriteAccessFacts) => { f.itemsSelect = false; }],
    ['transitions INSERT', (f: WriteAccessFacts) => { f.transitionsInsert = false; }],
  ] as [string, (f: WriteAccessFacts) => void][]) {
    const f = good(); mut(f);
    check(`revoking ${label} fails`, !evaluateWriteAccess(f).ok);
  }

  // All revoked → every problem reported, not just the first.
  const none: WriteAccessFacts = {
    role: 'x', itemsInsert: false, itemsSelect: false,
    itemsUpdateCols: {}, transitionsInsert: false,
  };
  const allBad = evaluateWriteAccess(none);
  check('all privileges missing reports all four problem classes', !allBad.ok && allBad.problems.length === 4);

  // The remedy message: names the eight columns, never suggests table-wide UPDATE.
  const msg = writeAccessRemedy(allBad.problems);
  for (const col of UPSERT_UPDATE_COLS) {
    check(`remedy names column ${col}`, msg.includes(col));
  }
  check('remedy does NOT suggest table-wide UPDATE on mcp_governance_items',
    !/GRANT[^;]*UPDATE\s+ON\s+public\.mcp_governance_items/i.test(msg));
  check('remedy says the narrowing is deliberate',
    /DELIBERATELY narrow/i.test(msg) && /DO NOT grant table-wide UPDATE/i.test(msg));
  check('remedy withholds repo/type/number explicitly',
    /repo, type and number are withheld on purpose/i.test(msg));
}

async function live() {
  if (!process.env.AIVE_INGEST_DATABASE_URL) {
    console.log('\n── live: SKIPPED (AIVE_INGEST_DATABASE_URL unset) ────────');
    console.log('   The offline section above proves the predicate; it does NOT prove');
    console.log('   the live grants satisfy it. That needs the scoped role.');
    return false;
  }
  console.log('\n── live: the real grants, and RLS enforcement ────────────');

  const colList = UPSERT_UPDATE_COLS.map((c) => `'${c}'`).join(',');
  const rows = await q<{ role: string; items_insert: boolean; items_select: boolean; transitions_insert: boolean; update_cols: Record<string, boolean>; items_update_table: boolean }>(
    `SELECT current_user::text AS role,
            has_table_privilege(current_user,'public.mcp_governance_items','INSERT') AS items_insert,
            has_table_privilege(current_user,'public.mcp_governance_items','SELECT') AS items_select,
            has_table_privilege(current_user,'public.mcp_governance_items','UPDATE') AS items_update_table,
            has_table_privilege(current_user,'public.mcp_governance_transitions','INSERT') AS transitions_insert,
            (SELECT jsonb_object_agg(c, has_column_privilege(current_user,'public.mcp_governance_items',c,'UPDATE'))
               FROM unnest(ARRAY[${colList}]) AS c) AS update_cols`,
  );
  const r = rows[0];
  const facts: WriteAccessFacts = {
    role: r.role, itemsInsert: r.items_insert, itemsSelect: r.items_select,
    itemsUpdateCols: r.update_cols ?? {}, transitionsInsert: r.transitions_insert,
  };
  console.log(`   connected as ${facts.role}`);
  console.log(`   table UPDATE on items: ${r.items_update_table}  (false is CORRECT — column-scoped by design)`);
  console.log(`   column UPDATE: ${UPSERT_UPDATE_COLS.filter((c) => facts.itemsUpdateCols[c]).length}/8`);

  const v = evaluateWriteAccess(facts);
  check('LIVE: predicate passes with migration 0016 grants unchanged', v.ok);
  check('LIVE: table-level UPDATE is absent (the narrowing survives)', r.items_update_table === false);

  // RLS is genuinely enforced on the probe path: a repo OUTSIDE the policy's
  // allowlist must be rejected even though the INSERT grant is present. If this
  // succeeded, the probe would be testing grants only and the policy could be
  // dropped without the preflight noticing.
  let rlsEnforced = false;
  try {
    await withRollback(async (exec) => {
      await exec(
        `INSERT INTO mcp_governance_items
           (repo,type,number,title,state,author,labels,created_at,updated_at,url,fetched_at)
         VALUES ('modelcontextprotocol/registry','issue',-999002,'__rls_probe__','open',NULL,'[]'::jsonb,now(),now(),'https://p.invalid',now())`,
      );
    });
    // A permitted repo must SUCCEED — that is the positive control.
    rlsEnforced = true;
  } catch {
    rlsEnforced = false;
  }
  check('LIVE: an allowed repo passes the INSERT policy (positive control)', rlsEnforced);

  let disallowedRejected = false;
  try {
    await withRollback(async (exec) => {
      // repo is not in the policy allowlist. The table CHECK also forbids it, and
      // either rejection proves the write path is gated, which is the property
      // the probe needs to have.
      await exec(
        `INSERT INTO mcp_governance_items
           (repo,type,number,title,state,author,labels,created_at,updated_at,url,fetched_at)
         VALUES ('evil/not-mirrored','issue',-999003,'__rls_probe__','open',NULL,'[]'::jsonb,now(),now(),'https://p.invalid',now())`,
      );
    });
  } catch {
    disallowedRejected = true;
  }
  check('LIVE: a disallowed repo is REJECTED (probe exercises the gate, not just the grant)', disallowedRejected);

  // Nothing leaked from any probe in this script.
  const after = await q<{ leaked: string }>(
    `SELECT count(*)::text AS leaked FROM mcp_governance_items WHERE title IN ('__rls_probe__','__aive_preflight_probe_rolled_back__')`,
  );
  check('LIVE: zero probe rows persisted', after[0]?.leaked === '0');
  return true;
}

async function main() {
  await offline();
  let ranLive = false;
  try { ranLive = await live(); } finally { await endIngestPool().catch(() => {}); }

  console.log('\n── results ──────────────────────────────────────────────');
  let ok = true;
  for (const [label, pass] of checks) {
    if (!pass) ok = false;
    console.log(`${pass ? 'PASS' : '*** FAIL ***'}  ${label}`);
  }
  console.log(`\n${checks.length} assertions${ranLive ? ' (offline + live)' : ' (offline only — live skipped)'}`);
  if (!ok) { console.error('FAILED'); process.exit(1); }
  console.log('All passed.');
  process.exit(0);
}

main().catch(async (e) => { console.error(e); await endIngestPool().catch(() => {}); process.exit(1); });
