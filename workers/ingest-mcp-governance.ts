/**
 * ingest-mcp-governance.ts — mirror upstream MCP repo governance activity.
 *
 * MOVED from the private repo (eXaive/aive-platform workers/) into this public
 * repo. It reads only public GitHub data and writes only public-facing tables,
 * so it belongs with the other public-data workers. Two things changed in the
 * move and nothing else did:
 *
 *  1. NO SERVICE ROLE KEY. The private-repo version built a supabase-js client
 *     from SUPABASE_SERVICE_ROLE_KEY — god-mode over every table in the
 *     database — to write three. This version uses lib/ingest/db.ts: direct
 *     Postgres as the scoped `aive_ingest` role, whose entire write surface is
 *     the tables it needs and which holds no DELETE anywhere. A leaked
 *     credential here vandalizes re-ingestable public data; it cannot read PII.
 *     The import tree of this file contains zero references to supabase-js.
 *
 *  2. ONE DEFINITION, NOT TWO. It is removed from the JOBS array in the private
 *     repo's workers/scheduler.ts in the same change. That array is driven by
 *     scheduler.yml, whose schedule has been PAUSED since 2026-08-01 for
 *     unrelated reasons (prediction resolution semantics), which means this
 *     mirror has not run on a cadence since then. Its own daily workflow here
 *     restores it — the move is a fix, not just a relocation.
 *
 * Fetches ISSUES and DISCUSSIONS from modelcontextprotocol/registry and
 * modelcontextprotocol/modelcontextprotocol into mcp_governance_items.
 * TITLES AND METADATA ONLY: bodies are never requested beyond what the list
 * endpoints return, and the body field is never read, mapped, or stored.
 *
 * Incremental by updated_at:
 *   - issues: the REST `since` param (filters on updated_at) from the stored
 *     per-repo watermark; PRs share the issues endpoint and are dropped via
 *     their `pull_request` key.
 *   - discussions: the REST list has no `since`; the most recent page (100)
 *     is upserted idempotently — new/changed rows update, unchanged rows no-op.
 *
 * FAILURE CONTRACT: each of the four segments (2 repos × issues/discussions)
 * is independent. A failed segment is recorded in ingestion_log (items_failed +
 * error_message naming the segment) and its rows are simply not updated this
 * run — never interpolated, never re-stamped. All four failing throws.
 *
 * NO HOLLOW SUCCESS: a run that mirrors zero rows exits non-zero, even if no
 * segment threw. The upstream repos hold hundreds of open items; a clean run
 * that writes nothing means the write path is broken, not that MCP governance
 * went quiet. Same rule as discover-mcp-servers.ts.
 *
 * AUTH: uses GITHUB_TOKEN when present (GitHub Actions provides one
 * automatically; 1,000 req/hr) and falls back to unauthenticated (60 req/hr/IP)
 * otherwise — this worker makes ~6 requests per run, so both postures are far
 * under limit. No PAT secret is required.
 *
 * Env: AIVE_INGEST_DATABASE_URL (scoped aive_ingest role — pooler host is
 * aws-1-us-east-1, NOT aws-0). Optional: GITHUB_TOKEN.
 *
 * CLI: npx tsx workers/ingest-mcp-governance.ts
 */

import { q, upsertRows, insertRows, withRollback, endIngestPool, logIngestionPg } from '../lib/ingest/db';

const REPOS = ['modelcontextprotocol/registry', 'modelcontextprotocol/modelcontextprotocol'] as const;
const SOURCE_SLUG = 'mcp-governance';
const PER_PAGE = 100;
const TIMEOUT_MS = 15_000;

interface GovRow {
  repo: string;
  type: 'issue' | 'discussion';
  number: number;
  title: string;
  state: string;
  author: string | null;
  labels: string[];
  created_at: string;
  updated_at: string;
  url: string;
}

function ghHeaders(): Record<string, string> {
  const h: Record<string, string> = {
    'User-Agent': 'AIVE-MCP-GovernanceMirror/1.0 (+https://github.com/eXaive/aive-ingest; titles+metadata only)',
    Accept: 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
  };
  if (process.env.GITHUB_TOKEN) h.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
  return h;
}

async function ghGet(url: string): Promise<unknown[]> {
  const res = await fetch(url, { headers: ghHeaders(), signal: AbortSignal.timeout(TIMEOUT_MS) });
  if (!res.ok) throw new Error(`GitHub ${res.status} for ${url}`);
  const data = await res.json();
  if (!Array.isArray(data)) throw new Error(`unexpected non-array payload for ${url}`);
  return data;
}

/** Stored watermark: newest updated_at we hold for a repo+type, or null. */
async function watermark(repo: string, type: GovRow['type']): Promise<string | null> {
  const rows = await q<{ newest: string | null }>(
    'SELECT max(updated_at)::text AS newest FROM mcp_governance_items WHERE repo = $1 AND type = $2',
    [repo, type],
  );
  return rows[0]?.newest ?? null;
}

/* Map WITHOUT touching body fields — the type below is the complete set of
   fields this worker is allowed to read. */
type GhItem = {
  number?: number; title?: string; state?: string;
  user?: { login?: string } | null;
  labels?: ({ name?: string } | string)[];
  created_at?: string; updated_at?: string; html_url?: string;
  pull_request?: unknown;
};

function toRow(repo: string, type: GovRow['type'], it: GhItem): GovRow | null {
  if (typeof it.number !== 'number' || !it.title || !it.created_at || !it.updated_at || !it.html_url) return null;
  return {
    repo, type,
    number: it.number,
    title: it.title.slice(0, 500),
    state: it.state ?? 'open',
    author: it.user?.login ?? null,
    labels: (it.labels ?? [])
      .map((l) => (typeof l === 'string' ? l : l.name ?? ''))
      .filter(Boolean)
      .slice(0, 20),
    created_at: it.created_at,
    updated_at: it.updated_at,
    url: it.html_url,
  };
}

/**
 * State-transition detection (migration 20260809000011): BEFORE upserting,
 * compare each incoming state to the stored one and append a transition row
 * for every real flip. First sight writes NO transition — an item appearing
 * for the first time has no prior state. observed_at is the table default
 * now(): the moment THIS worker saw the change, never inferred from GitHub's
 * updated_at (which moves for comments/labels/edits too).
 *
 * mcp_governance_transitions is append-only, trigger-enforced — this is its
 * only writer and it only ever INSERTs. Note the vocabulary: these are
 * GitHub ITEM-STATE transitions (open ↔ closed ↔ locked). They are a different
 * object from the registry SERVER-STATUS transitions surfaced elsewhere, and
 * the two must not be conflated in any label.
 */
async function recordTransitions(rows: GovRow[]): Promise<number> {
  if (rows.length === 0) return 0;
  const { repo, type } = rows[0];
  const stored = await q<{ number: number; state: string }>(
    'SELECT number, state FROM mcp_governance_items WHERE repo = $1 AND type = $2 AND number = ANY($3::int[])',
    [repo, type, rows.map((r) => r.number)],
  );

  const prior = new Map(stored.map((s) => [Number(s.number), s.state]));
  const transitions = rows.filter((r) => prior.has(r.number) && prior.get(r.number) !== r.state);
  if (transitions.length === 0) return 0;

  await insertRows(
    'mcp_governance_transitions',
    ['repo', 'type', 'number', 'from_state', 'to_state', 'title'],
    transitions.map((r) => [r.repo, r.type, r.number, prior.get(r.number)!, r.state, r.title]),
  );
  return transitions.length;
}

async function upsert(rows: GovRow[]): Promise<number> {
  if (rows.length === 0) return 0;
  const flips = await recordTransitions(rows);
  if (flips > 0) console.log(`[mcp-governance] ${rows[0].repo} ${rows[0].type}: ${flips} item-state transition(s) recorded`);

  await upsertRows({
    table: 'mcp_governance_items',
    cols: ['repo', 'type', 'number', 'title', 'state', 'author', 'labels', 'created_at', 'updated_at', 'url', 'fetched_at'],
    // labels is jsonb and MUST be pre-serialized per the lib/ingest/db
    // contract — node-pg would otherwise write a Postgres array literal into a
    // jsonb column. fetched_at is stamped here, not defaulted, so a re-run
    // records when we last SAW the row even when nothing about it changed.
    rows: rows.map((r) => [
      r.repo, r.type, r.number, r.title, r.state, r.author,
      JSON.stringify(r.labels), r.created_at, r.updated_at, r.url,
      new Date().toISOString(),
    ]),
    conflictCols: ['repo', 'type', 'number'],
    // The SAME constant the preflight asserts column-level UPDATE on. Sharing it
    // is what stops the grant, the assertion and the statement drifting apart —
    // drift between them is exactly what broke the first scheduled run.
    updateCols: [...UPSERT_UPDATE_COLS],
  });
  return rows.length;
}

/**
 * The eight columns the upsert's DO UPDATE arm actually SETs. Migration
 * 20260809000016 grants UPDATE on EXACTLY these and deliberately withholds
 * repo/type/number, so the role can refresh a row's contents but cannot re-key
 * one onto a different repo or issue number. This list and the upsert's
 * updateCols must stay in step — it is asserted below, not assumed.
 */
export const UPSERT_UPDATE_COLS = [
  'title', 'state', 'author', 'labels', 'created_at', 'updated_at', 'url', 'fetched_at',
] as const;

/** The facts the preflight predicate is evaluated over. Gathered from the DB in
 *  one query; kept separate from the predicate so the predicate is testable. */
export interface WriteAccessFacts {
  role: string;
  itemsInsert: boolean;
  itemsSelect: boolean;
  /** column name -> has_column_privilege(..., 'UPDATE') */
  itemsUpdateCols: Record<string, boolean>;
  transitionsInsert: boolean;
}

/**
 * The PREDICATE, pure and exported so it can be tested as ONE EXPRESSION.
 *
 * WHY THAT MATTERS (2026-08-10). The first scheduled run failed with
 * "items=false, transitions=true" because the old preflight asserted
 * has_table_privilege(..., 'UPDATE') — a TABLE-level check — while migration
 * …0016 grants UPDATE at COLUMN level. Postgres reports column grants only
 * through has_column_privilege, so the table-level check is false by design and
 * the preflight blocked a run that would have worked: ON CONFLICT DO UPDATE
 * needs UPDATE only on the columns it SETs, which were granted.
 *
 * The post-apply verification tested INSERT alone (true) and the column list
 * (true x8) and never evaluated the conjunction the worker runs. Testing the
 * ingredients while never testing the expression is what let two files I wrote
 * contradict each other all the way into a scheduled run. Hence: pure function,
 * exported, exercised by scripts/verify-write-access.ts across every failure
 * combination.
 */
export function evaluateWriteAccess(f: WriteAccessFacts): { ok: boolean; problems: string[] } {
  const problems: string[] = [];
  if (!f.itemsInsert) problems.push('missing INSERT on public.mcp_governance_items');
  if (!f.itemsSelect) problems.push('missing SELECT on public.mcp_governance_items');
  const missingCols = UPSERT_UPDATE_COLS.filter((c) => f.itemsUpdateCols[c] !== true);
  if (missingCols.length > 0) {
    problems.push(`missing column-level UPDATE on public.mcp_governance_items (${missingCols.join(', ')})`);
  }
  if (!f.transitionsInsert) problems.push('missing INSERT on public.mcp_governance_transitions');
  return { ok: problems.length === 0, problems };
}

/** The remedy text. Names the EIGHT columns and never suggests table-wide
 *  UPDATE — a table-wide grant would undo migration …0016's deliberate
 *  narrowing, which is the protection that stops this role re-keying rows. */
export function writeAccessRemedy(problems: string[]): string {
  return (
    `mcp-governance: the scoped role cannot write what it needs — ${problems.join('; ')}. ` +
    'Required in eXaive/aive-platform (migration 20260809000016): ' +
    'GRANT SELECT, INSERT ON public.mcp_governance_items TO aive_ingest; ' +
    `GRANT UPDATE (${UPSERT_UPDATE_COLS.join(', ')}) ON public.mcp_governance_items TO aive_ingest; ` +
    'GRANT INSERT ON public.mcp_governance_transitions TO aive_ingest; ' +
    'plus the four RLS policies scoped to the two mirrored repos. ' +
    'DO NOT grant table-wide UPDATE on mcp_governance_items: the column list is ' +
    'DELIBERATELY narrow so this role cannot re-key a row onto a different repo ' +
    'or issue number (repo, type and number are withheld on purpose). ' +
    'Refusing to run rather than reporting a permissions wall as an upstream outage.'
  );
}

/**
 * PREFLIGHT. Two stages, because grants and policies are different things and
 * the old version tested only the first:
 *
 *   1. GRANTS — table-level INSERT/SELECT on items, column-level UPDATE on the
 *      eight columns the upsert SETs, table-level INSERT on transitions.
 *   2. RLS — a real INSERT, a real ON CONFLICT DO UPDATE and a real transitions
 *      INSERT, inside a transaction that is ALWAYS rolled back. A correct grant
 *      set with a missing or hostile policy passes stage 1 and fails here,
 *      which is exactly the gap the grant-only check left open.
 *
 * The probe is self-contained: it inserts its own sentinel row and then upserts
 * that same key, so the DO UPDATE arm is exercised without depending on the
 * table already holding data (it also works on an empty table). Nothing
 * persists — verified after the rollback by row count and sentinel search, not
 * assumed from the fact that ROLLBACK was issued.
 */
const PROBE_SENTINEL = '__aive_preflight_probe_rolled_back__';
const PROBE_NUMBER = -999_001; // negative: cannot collide with a real issue number

async function assertWriteAccess(): Promise<void> {
  // ── stage 1: grants ──
  const colList = UPSERT_UPDATE_COLS.map((c) => `'${c}'`).join(',');
  const rows = await q<{ role: string; items_insert: boolean; items_select: boolean; transitions_insert: boolean; update_cols: Record<string, boolean> }>(
    `SELECT current_user::text AS role,
            has_table_privilege(current_user, 'public.mcp_governance_items', 'INSERT') AS items_insert,
            has_table_privilege(current_user, 'public.mcp_governance_items', 'SELECT') AS items_select,
            has_table_privilege(current_user, 'public.mcp_governance_transitions', 'INSERT') AS transitions_insert,
            (SELECT jsonb_object_agg(c, has_column_privilege(current_user, 'public.mcp_governance_items', c, 'UPDATE'))
               FROM unnest(ARRAY[${colList}]) AS c) AS update_cols`,
  );
  const r = rows[0];
  if (!r) throw new Error('mcp-governance: preflight privilege query returned no rows');

  const facts: WriteAccessFacts = {
    role: r.role,
    itemsInsert: r.items_insert,
    itemsSelect: r.items_select,
    itemsUpdateCols: r.update_cols ?? {},
    transitionsInsert: r.transitions_insert,
  };
  const verdict = evaluateWriteAccess(facts);
  if (!verdict.ok) throw new Error(`${writeAccessRemedy(verdict.problems)} (connected as ${facts.role})`);

  // ── stage 2: RLS, via a rolled-back probe ──
  //
  // Counting mcp_governance_items ONLY. This role has INSERT on
  // mcp_governance_transitions and deliberately NO SELECT (migration …0016), so
  // counting that table would itself be permission-denied — which is how the
  // first version of this probe failed. That is not a coverage gap: both probe
  // writes happen in ONE transaction on ONE pinned connection, so if the items
  // sentinel is absent afterwards the transitions insert was rolled back too.
  // The readable table verifies the rollback for both.
  const before = await q<{ items: string }>(
    'SELECT count(*)::text AS items FROM mcp_governance_items',
  );
  const repo = REPOS[0];
  try {
    await withRollback(async (exec) => {
      // INSERT arm — exercises the INSERT policy's WITH CHECK.
      await exec(
        `INSERT INTO mcp_governance_items
           (repo,type,number,title,state,author,labels,created_at,updated_at,url,fetched_at)
         VALUES ($1,'issue',$2,$3,'open',NULL,'[]'::jsonb,now(),now(),'https://probe.invalid',now())`,
        [repo, PROBE_NUMBER, PROBE_SENTINEL],
      );
      // DO UPDATE arm on the same key — exercises the column UPDATE grants AND
      // the UPDATE policy's USING and WITH CHECK together.
      await exec(
        `INSERT INTO mcp_governance_items
           (repo,type,number,title,state,author,labels,created_at,updated_at,url,fetched_at)
         VALUES ($1,'issue',$2,$3,'closed',NULL,'[]'::jsonb,now(),now(),'https://probe.invalid',now())
         ON CONFLICT (repo,type,number) DO UPDATE SET
           ${UPSERT_UPDATE_COLS.map((c) => `${c} = EXCLUDED.${c}`).join(', ')}`,
        [repo, PROBE_NUMBER, PROBE_SENTINEL],
      );
      // transitions INSERT — from_state must differ from to_state per its policy.
      await exec(
        `INSERT INTO mcp_governance_transitions (repo,type,number,from_state,to_state,title)
         VALUES ($1,'issue',$2,'open','closed',$3)`,
        [repo, PROBE_NUMBER, PROBE_SENTINEL],
      );
    });
  } catch (err) {
    throw new Error(
      'mcp-governance: grants are present but a WRITE PROBE FAILED, which points at RLS rather than privileges — ' +
      `${err instanceof Error ? err.message : String(err)}. ` +
      'Check the four policies from migration 20260809000016 still exist and still admit ' +
      `repo IN (${REPOS.map((x) => `'${x}'`).join(', ')}) and type IN ('issue','discussion'). ` +
      `Connected as ${facts.role}. The probe writes inside a transaction that is always rolled back.`,
    );
  }

  // The rollback was ISSUED; confirm it TOOK. Different claims.
  const after = await q<{ items: string; leaked: string }>(
    `SELECT (SELECT count(*) FROM mcp_governance_items)::text AS items,
            (SELECT count(*) FROM mcp_governance_items WHERE title = $1)::text AS leaked`,
    [PROBE_SENTINEL],
  );
  const b = before[0], a = after[0];
  if (!a || !b || a.items !== b.items || a.leaked !== '0') {
    throw new Error(
      'mcp-governance: the preflight PROBE LEAKED — rows persisted after ROLLBACK ' +
      `(items ${b?.items}→${a?.items}, sentinel rows ${a?.leaked}). Both probe writes share ` +
      'one transaction, so a leak here means the transitions insert leaked too — and this role ' +
      'cannot read or delete that table. Refusing to continue; investigate withRollback() and ' +
      `clear rows with title='${PROBE_SENTINEL}' using an elevated role before running again.`,
    );
  }
  console.log(`[mcp-governance] preflight ok as ${facts.role} — grants + RLS verified, probe rolled back cleanly`);
}

export async function ingestMcpGovernance(): Promise<{ fetched: number; upserted: number; failures: string[] }> {
  const startedAt = new Date();
  let fetched = 0;
  let upserted = 0;
  const failures: string[] = [];

  await assertWriteAccess();

  for (const repo of REPOS) {
    // ── issues (incremental via `since`, PRs dropped) ──
    try {
      const since = await watermark(repo, 'issue');
      const url =
        `https://api.github.com/repos/${repo}/issues?state=all&sort=updated&direction=desc&per_page=${PER_PAGE}` +
        (since ? `&since=${encodeURIComponent(since)}` : '');
      const items = (await ghGet(url)) as GhItem[];
      const rows = items
        .filter((it) => !('pull_request' in it))
        .map((it) => toRow(repo, 'issue', it))
        .filter((r): r is GovRow => r !== null);
      fetched += items.length;
      upserted += await upsert(rows);
    } catch (err) {
      failures.push(`${repo} issues: ${err instanceof Error ? err.message : String(err)}`);
    }

    // ── discussions (no `since` on REST; idempotent upsert of latest page) ──
    try {
      const url = `https://api.github.com/repos/${repo}/discussions?per_page=${PER_PAGE}`;
      const items = (await ghGet(url)) as GhItem[];
      const rows = items
        .map((it) => toRow(repo, 'discussion', it))
        .filter((r): r is GovRow => r !== null);
      fetched += items.length;
      upserted += await upsert(rows);
    } catch (err) {
      failures.push(`${repo} discussions: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  await logIngestionPg({
    sourceSlug: SOURCE_SLUG,
    startedAt,
    itemsFetched: fetched,
    itemsNew: upserted,
    itemsFailed: failures.length,
    errorMessage: failures.length ? failures.join(' | ').slice(0, 500) : undefined,
    metadata: { auth: process.env.GITHUB_TOKEN ? 'github_token' : 'unauthenticated', repo_count: REPOS.length },
  });

  console.log(`[mcp-governance] fetched=${fetched} upserted=${upserted} failures=${failures.length}`);
  if (failures.length) console.error(`[mcp-governance] failed segments:\n  ${failures.join('\n  ')}`);

  // All four segments failing = nothing mirrored this run — red-run it.
  if (failures.length === REPOS.length * 2) {
    throw new Error(`mcp-governance: every segment failed — ${failures.join(' | ')}`);
  }
  return { fetched, upserted, failures };
}

// CLI entry / GHA workflow entrypoint
if (require.main === module) {
  ingestMcpGovernance()
    .then(async (r) => {
      await endIngestPool();
      // NO HOLLOW SUCCESS — see the header. Zero rows mirrored from two active
      // upstream repos means the write path failed, not that governance stopped.
      if (r.upserted === 0) {
        console.error(
          `[mcp-governance] FAILED: run completed having mirrored ZERO rows ` +
          `(fetched=${r.fetched}, failed segments=${r.failures.length}). Exiting non-zero.`,
        );
        process.exit(1);
      }
      process.exit(r.failures.length > 0 ? 1 : 0);
    })
    .catch(async (err) => {
      console.error('[mcp-governance] run failed:', err);
      await endIngestPool();
      process.exit(1);
    });
}
