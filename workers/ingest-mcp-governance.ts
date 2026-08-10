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

import { q, upsertRows, insertRows, endIngestPool, logIngestionPg } from '../lib/ingest/db';

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
    updateCols: ['title', 'state', 'author', 'labels', 'created_at', 'updated_at', 'url', 'fetched_at'],
  });
  return rows.length;
}

/**
 * PREFLIGHT: the scoped role must actually be able to write these two tables.
 * They were created service_role-only (migrations 20260809000009 / …0011), so
 * moving this worker onto `aive_ingest` needs a GRANT + RLS policy pass in
 * eXaive/aive-platform. Without it every segment fails identically on
 * permissions, which would read as "GitHub is down". Fail fast, name the fix.
 */
async function assertWriteAccess(): Promise<void> {
  const rows = await q<{ has_items: boolean; has_transitions: boolean }>(
    `SELECT
       has_table_privilege(current_user, 'public.mcp_governance_items', 'INSERT')
         AND has_table_privilege(current_user, 'public.mcp_governance_items', 'UPDATE')
         AND has_table_privilege(current_user, 'public.mcp_governance_items', 'SELECT') AS has_items,
       has_table_privilege(current_user, 'public.mcp_governance_transitions', 'INSERT') AS has_transitions`,
  );
  const r = rows[0];
  if (!r?.has_items || !r?.has_transitions) {
    throw new Error(
      'mcp-governance: the scoped role lacks write access ' +
      `(items=${r?.has_items ?? 'unknown'}, transitions=${r?.has_transitions ?? 'unknown'}). ` +
      'Required in eXaive/aive-platform: GRANT SELECT, INSERT, UPDATE ON public.mcp_governance_items ' +
      'AND GRANT INSERT ON public.mcp_governance_transitions TO aive_ingest, plus matching RLS ' +
      'policies (both tables have RLS enabled and no aive_ingest policy). ' +
      'Refusing to run rather than reporting a permissions wall as an upstream outage.',
    );
  }
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
