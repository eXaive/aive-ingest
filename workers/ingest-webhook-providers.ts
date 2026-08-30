// ================================================================
// Webhook-provider ingest — registry_artifacts kind='webhook_provider'
//
// THE DATASET, precisely: webhook providers SURFACED BY INTEGRATION
// PLATFORMS — apps for which Pipedream ships a source component, or n8n
// ships a *Trigger node. NOT "companies with webhooks": a vendor can run
// webhooks no platform has wrapped (absent here), and a platform can wrap
// a poll-only surface (present here as mode:"poll"). The corpus measures
// platform coverage, not vendor capability.
//
// Sources — both read-only public git, no API keys required:
//   1. PipedreamHQ/pipedream  components/<app>/sources/*   (per-app row)
//   2. n8n-io/n8n             packages/nodes-base/nodes/**/*Trigger.node.ts
//
// Pattern: fetch → classify → upsert → snapshot-on-change → scan_runs,
// mirroring workers/ingest-mcp-registry.ts. definition_hash uses the SAME
// canonical hasher (lib/mcp/canonicalize.ts hashCanonical); a
// registry_artifact_snapshots row is written ONLY for new artifacts or
// changed hashes, so an unchanged upstream writes zero snapshots.
//
// Fetch budget: 3–4 GitHub tree/metadata API calls per source (uses
// GITHUB_TOKEN when present; fits unauthenticated limits otherwise). File
// CONTENTS come from raw.githubusercontent.com, which is not API-metered.
// Content fetches are CAPPED with the drop count logged loudly — a silent
// cap would read as full coverage (no-silent-caps rule).
//
// Run: npx tsx workers/ingest-webhook-providers.ts   (npm run ingest:webhook-providers)
// Env: AIVE_INGEST_DATABASE_URL (scoped aive_ingest role), optional GITHUB_TOKEN
// ================================================================

import { hashCanonical } from '../lib/mcp/canonicalize';
// Direct Postgres via the scoped aive_ingest role — no supabase-js, no
// service key. See lib/ingest/db.ts for the role surface and TLS notes.
import { q, upsertRows, insertRows, endIngestPool } from '../lib/ingest/db';

const KIND = 'webhook_provider';
const GITHUB_API = 'https://api.github.com';
const RAW_BASE = 'https://raw.githubusercontent.com';
const FETCH_TIMEOUT_MS = 20_000;
const RAW_CONCURRENCY = 8;
/** Cap on raw-content fetches per source repo; dropped counts are logged. */
const CONTENT_FETCH_CAP = 400;
export const WEBHOOK_PROVIDER_REQUEST_BOUNDS = Object.freeze({
  githubApiCalls: 8,
  rawFetchesPerSource: CONTENT_FETCH_CAP,
  sources: 2,
  rawConcurrency: RAW_CONCURRENCY,
});

/** Freshness tripwire threshold (hours). Same posture as the MCP worker's
 *  48h snapshot tripwire, but keyed on registry_artifacts.last_seen instead
 *  of snapshots: this corpus is small and stable enough that an unchanged
 *  day legitimately writes ZERO snapshots (write-on-change), so a
 *  snapshot-age tripwire would false-alarm. last_seen is stamped on every
 *  row every successful run, so its age measures run liveness exactly. */
const FRESHNESS_HOURS = 48;

interface ArtifactRow {
  source: 'pipedream' | 'n8n';
  external_id: string;
  name: string;
  homepage_url: string;
  manifest: Record<string, unknown>;
}

// ── provider_type classification ────────────────────────────────────────────
// Distinguishes real VENDOR providers from the platforms' own machinery.
// Derived HERE, in the worker, from the upstream path segment (the app slug /
// node directory) — the page carries no name list and renders whatever the
// manifest says. Stored in manifest jsonb, not a typed column.
//   vendor            — an external company/product (Stripe, Shopify, GitHub…)
//   platform_internal — the platform's own workflow machinery (n8n's
//                       ErrorTrigger/ManualTrigger/Schedule…, Pipedream's
//                       schedule app): triggers ABOUT the platform, not about
//                       any provider
//   protocol          — generic protocols and datastores (Postgres, Kafka,
//                       MQTT, AMQP, RSS, IMAP…): a technology surface, not a
//                       vendor's event feed
// Sets are grounded in the live corpus (enumerated 2026-08-01); a new
// unmatched path segment defaults to vendor, which the next corpus review can
// reclassify — defaulting the other way would silently shrink the headline.

const N8N_PLATFORM_INTERNAL = new Set([
  'errortrigger', 'executeworkflow', 'manualtrigger', 'n8ntrigger',
  'workflowtrigger', 'simulate', 'e2etest', 'schedule', 'localfiletrigger',
  'ssetrigger',
  // n8n's own Forms feature (FormIo/Formstack remain vendors)
  'form',
]);
const N8N_PROTOCOL = new Set([
  'postgres', 'redis', 'kafka', 'mqtt', 'amqp',
  // same class: message broker + feed-protocol readers
  'rabbitmq', 'rssfeedread',
]);
const PIPEDREAM_PLATFORM_INTERNAL = new Set([
  'schedule', // Pipedream's own scheduler app
]);
const PIPEDREAM_PROTOCOL = new Set([
  'amqp', 'email', 'http', 'imap', 'mongodb', 'mysql', 'postgresql', 'rss', 'sftp',
]);

function classifyProviderType(source: 'pipedream' | 'n8n', pathSegment: string): 'vendor' | 'platform_internal' | 'protocol' {
  const key = pathSegment.toLowerCase();
  if (source === 'n8n') {
    if (N8N_PLATFORM_INTERNAL.has(key)) return 'platform_internal';
    if (N8N_PROTOCOL.has(key)) return 'protocol';
    return 'vendor';
  }
  if (PIPEDREAM_PLATFORM_INTERNAL.has(key)) return 'platform_internal';
  if (PIPEDREAM_PROTOCOL.has(key)) return 'protocol';
  return 'vendor';
}

let apiCalls = 0;
let rawFetches = 0;

export function ghHeaders(token = process.env.GITHUB_TOKEN): Record<string, string> {
  const h: Record<string, string> = {
    'Accept': 'application/vnd.github+json',
    'User-Agent': 'aive-webhook-provider-ingest',
  };
  if (token) h['Authorization'] = `Bearer ${token}`;
  return h;
}

type GitHubFailureClass =
  | 'AUTHENTICATION_REJECTED'
  | 'RATE_LIMITED'
  | 'ACCESS_FORBIDDEN'
  | 'NOT_FOUND'
  | 'UPSTREAM_FAILURE'
  | 'REQUEST_REJECTED';

function sanitizedHeader(headers: Headers, name: string): string {
  const value = headers.get(name);
  return value && /^\d{1,20}$/.test(value) ? value : 'unavailable';
}

export function classifyGitHubFailure(status: number, headers: Headers): GitHubFailureClass {
  if (status === 401) return 'AUTHENTICATION_REJECTED';
  if (status === 429 || (status === 403 && headers.get('x-ratelimit-remaining') === '0')) return 'RATE_LIMITED';
  if (status === 403) return 'ACCESS_FORBIDDEN';
  if (status === 404) return 'NOT_FOUND';
  if (status >= 500) return 'UPSTREAM_FAILURE';
  return 'REQUEST_REJECTED';
}

export function sanitizedGitHubFailure(path: string, status: number, headers: Headers): string {
  return [
    `GitHub API ${path}: HTTP ${status}`,
    `classification=${classifyGitHubFailure(status, headers)}`,
    `x-ratelimit-limit=${sanitizedHeader(headers, 'x-ratelimit-limit')}`,
    `x-ratelimit-remaining=${sanitizedHeader(headers, 'x-ratelimit-remaining')}`,
    `x-ratelimit-reset=${sanitizedHeader(headers, 'x-ratelimit-reset')}`,
    `retry-after=${sanitizedHeader(headers, 'retry-after')}`,
  ].join(' ');
}

export function githubApiRequest(path: string, token = process.env.GITHUB_TOKEN): [string, RequestInit] {
  if (!path.startsWith('/') || path.startsWith('//')) throw new Error('GitHub API path rejected');
  const url = new URL(path, GITHUB_API);
  if (url.origin !== GITHUB_API) throw new Error('GitHub API origin rejected');
  return [url.toString(), {
    headers: ghHeaders(token),
    redirect: 'error',
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  }];
}

async function ghJson<T>(path: string): Promise<T> {
  apiCalls++;
  const [url, init] = githubApiRequest(path);
  const res = await fetch(url, init);
  if (!res.ok) throw new Error(sanitizedGitHubFailure(path, res.status, res.headers));
  return (await res.json()) as T;
}

async function rawText(repo: string, branch: string, path: string): Promise<string | null> {
  rawFetches++;
  try {
    const res = await fetch(`${RAW_BASE}/${repo}/${branch}/${path}`, {
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    });
    if (!res.ok) return null;
    return await res.text();
  } catch {
    return null;
  }
}

/** Small concurrency pool for raw content fetches. */
async function mapPool<T, R>(items: T[], limit: number, fn: (t: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let next = 0;
  async function lane() {
    while (next < items.length) {
      const i = next++;
      out[i] = await fn(items[i]);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, lane));
  return out;
}

interface TreeEntry { path: string; type: 'blob' | 'tree'; sha: string }
interface TreeResponse { sha: string; tree: TreeEntry[]; truncated: boolean }

/** Resolve a subdirectory's tree sha by walking one path segment at a time
 *  (1 API call per segment), then fetch that subtree recursively (1 call).
 *  Throws on truncation — a silently partial corpus must not look complete. */
async function fetchSubtreeRecursive(repo: string, branch: string, dirPath: string): Promise<TreeEntry[]> {
  let sha = branch;
  for (const segment of dirPath.split('/')) {
    const tree = await ghJson<TreeResponse>(`/repos/${repo}/git/trees/${sha}`);
    const entry = tree.tree.find((e) => e.type === 'tree' && e.path === segment);
    if (!entry) throw new Error(`${repo}: directory segment '${segment}' not found under '${dirPath}'`);
    sha = entry.sha;
  }
  const subtree = await ghJson<TreeResponse>(`/repos/${repo}/git/trees/${sha}?recursive=1`);
  if (subtree.truncated) {
    throw new Error(`${repo}/${dirPath}: recursive tree TRUNCATED by GitHub — refusing to build a silently partial corpus`);
  }
  return subtree.tree;
}

// ── Source 1: Pipedream components ──────────────────────────────────────────
// components/<app>/sources/<source>/… — one row per app that ships ≥1 source.
// Classification heuristics (per the audit brief):
//   - a source is "instant" when its directory name contains "instant"
//     (Pipedream's own naming convention for webhook-backed sources), and
//   - an app is webhook-backed when its tree contains common-webhook.mjs
//     (the shared webhook base module). NOTE: this is FILE PRESENCE in the
//     app's subtree, not a per-file import parse — parsing imports for every
//     source would cost thousands of content fetches per run.
// Event types come from <app>/{common,sources/common}/events.mjs where
// present, extracted as the `value:` string literals (capped, drops logged).

async function collectPipedream(): Promise<{ rows: ArtifactRow[]; notes: string[] }> {
  const repo = 'PipedreamHQ/pipedream';
  const meta = await ghJson<{ default_branch: string }>(`/repos/${repo}`);
  const branch = meta.default_branch;
  const entries = await fetchSubtreeRecursive(repo, branch, 'components');
  console.log(`[webhook-providers] pipedream: components subtree = ${entries.length} entries (branch ${branch})`);

  // Group paths by app slug (first segment under components/).
  const byApp = new Map<string, string[]>();
  for (const e of entries) {
    if (e.type !== 'blob') continue;
    const slash = e.path.indexOf('/');
    if (slash < 0) continue;
    const app = e.path.slice(0, slash);
    const rest = e.path.slice(slash + 1);
    const list = byApp.get(app);
    if (list) list.push(rest);
    else byApp.set(app, [rest]);
  }

  // Identify apps with sources and their events.mjs paths.
  const rows: ArtifactRow[] = [];
  const eventsToFetch: { app: string; path: string }[] = [];
  for (const [app, paths] of byApp) {
    const sourceDirs = new Set<string>();
    let hasCommonWebhook = false;
    let eventsPath: string | null = null;
    for (const p of paths) {
      const m = p.match(/^sources\/([^/]+)\//);
      if (m && m[1] !== 'common') sourceDirs.add(m[1]);
      if (/(^|\/)common-webhook\.mjs$/.test(p)) hasCommonWebhook = true;
      if (p === 'common/events.mjs' || p === 'sources/common/events.mjs') eventsPath = p;
    }
    if (sourceDirs.size === 0) continue; // app ships no source components

    const sources = [...sourceDirs].sort().map((key) => ({
      key,
      mode: key.includes('instant') ? 'instant' : 'poll',
    }));
    const mode = hasCommonWebhook || sources.some((s) => s.mode === 'instant') ? 'instant' : 'poll';

    rows.push({
      source: 'pipedream',
      external_id: app,
      name: app.replace(/[_-]+/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase()),
      homepage_url: `https://github.com/${repo}/tree/${branch}/components/${app}`,
      manifest: {
        platform: 'pipedream',
        mode,
        provider_type: classifyProviderType('pipedream', app),
        has_common_webhook: hasCommonWebhook,
        sources,
      },
    });
    if (eventsPath) eventsToFetch.push({ app, path: `components/${app}/${eventsPath}` });
  }

  const notes: string[] = [];
  // Event-type extraction — capped, drop count logged (no silent caps).
  const capped = eventsToFetch.slice(0, CONTENT_FETCH_CAP);
  const dropped = eventsToFetch.length - capped.length;
  if (dropped > 0) {
    const msg = `pipedream: events.mjs fetch cap hit — ${dropped}/${eventsToFetch.length} apps' event types NOT extracted this run`;
    console.warn(`[webhook-providers] ${msg}`);
    notes.push(msg);
  }
  const rowByApp = new Map(rows.map((r) => [r.external_id, r]));
  await mapPool(capped, RAW_CONCURRENCY, async ({ app, path }) => {
    const text = await rawText(repo, branch, path);
    if (!text) return null;
    // Heuristic: events.mjs files carry { label, value } entries; collect the
    // value: literals. Imperfect by design and bounded; recorded as such.
    const values = [...text.matchAll(/value:\s*["'`]([^"'`\n]{1,80})["'`]/g)].map((m) => m[1]);
    const unique = [...new Set(values)].slice(0, 100);
    const row = rowByApp.get(app);
    if (row && unique.length) {
      row.manifest.event_types = unique;
      row.manifest.event_types_source = path;
    }
    return null;
  });

  return { rows, notes };
}

// ── Source 2: n8n trigger nodes ─────────────────────────────────────────────
// packages/nodes-base/nodes/**/*Trigger.node.ts — one row per node directory
// that ships ≥1 trigger file. Classification is content-based (the file set
// is small enough to read every file):
//   webhooks: [        → instant (webhook-backed trigger)
//   polling: true      → poll
//   neither            → 'unclassified' (cron/websocket/other trigger styles)

async function collectN8n(): Promise<{ rows: ArtifactRow[]; notes: string[] }> {
  const repo = 'n8n-io/n8n';
  const meta = await ghJson<{ default_branch: string }>(`/repos/${repo}`);
  const branch = meta.default_branch;
  const entries = await fetchSubtreeRecursive(repo, branch, 'packages/nodes-base/nodes');
  console.log(`[webhook-providers] n8n: nodes subtree = ${entries.length} entries (branch ${branch})`);

  const triggerFiles = entries.filter((e) => e.type === 'blob' && /Trigger\.node\.ts$/.test(e.path));
  const notes: string[] = [];
  const capped = triggerFiles.slice(0, CONTENT_FETCH_CAP);
  const dropped = triggerFiles.length - capped.length;
  if (dropped > 0) {
    const msg = `n8n: trigger-file fetch cap hit — ${dropped}/${triggerFiles.length} trigger files NOT classified this run`;
    console.warn(`[webhook-providers] ${msg}`);
    notes.push(msg);
  }

  const classified = await mapPool(capped, RAW_CONCURRENCY, async (e) => {
    const text = await rawText(repo, branch, `packages/nodes-base/nodes/${e.path}`);
    let mode: 'instant' | 'poll' | 'unclassified' = 'unclassified';
    if (text) {
      const hasWebhooks = /webhooks\s*[:=]\s*\[/.test(text);
      const hasPolling = /polling:\s*true/.test(text);
      mode = hasWebhooks ? 'instant' : hasPolling ? 'poll' : 'unclassified';
    }
    return { path: e.path, mode, fetched: text !== null };
  });

  // Group by top-level node directory (e.g. Github/GithubTrigger.node.ts → github).
  const byDir = new Map<string, { path: string; mode: string }[]>();
  for (const c of classified) {
    if (!c.fetched) {
      notes.push(`n8n: content fetch failed for ${c.path} — left unclassified`);
    }
    const dir = c.path.split('/')[0];
    const list = byDir.get(dir);
    const item = { path: c.path, mode: c.mode };
    if (list) list.push(item);
    else byDir.set(dir, [item]);
  }

  const rows: ArtifactRow[] = [];
  for (const [dir, files] of byDir) {
    const mode = files.some((f) => f.mode === 'instant')
      ? 'instant'
      : files.some((f) => f.mode === 'poll')
        ? 'poll'
        : 'unclassified';
    rows.push({
      source: 'n8n',
      external_id: dir.toLowerCase(),
      name: dir,
      homepage_url: `https://github.com/${repo}/tree/${branch}/packages/nodes-base/nodes/${dir}`,
      manifest: {
        platform: 'n8n',
        mode,
        provider_type: classifyProviderType('n8n', dir),
        trigger_files: files.sort((a, b) => a.path.localeCompare(b.path)),
      },
    });
  }

  return { rows, notes };
}

// ── scan_runs bookkeeping (mirrors ingest-mcp-registry.ts) ──────────────────

async function openScanRun(startedAt: Date): Promise<string | null> {
  try {
    const rows = await q<{ id: string }>(
      "INSERT INTO scan_runs (started_at, status) VALUES ($1, 'running') RETURNING id",
      [startedAt.toISOString()],
    );
    return rows[0]?.id ?? null;
  } catch (err: any) {
    console.error('[webhook-providers] scan_runs insert failed:', err?.message ?? err);
    return null;
  }
}

async function closeScanRun(
  id: string | null,
  fields: { pages_fetched: number; servers_returned: number | null; status: string },
): Promise<void> {
  if (!id) return;
  try {
    await q(
      'UPDATE scan_runs SET finished_at = now(), pages_fetched = $2, servers_returned = $3, status = $4 WHERE id = $1',
      [id, fields.pages_fetched, fields.servers_returned, fields.status],
    );
  } catch (err: any) {
    console.error('[webhook-providers] scan_runs update failed:', err?.message ?? err);
  }
}

// ── Main ────────────────────────────────────────────────────────────────────

export async function ingestWebhookProviders(): Promise<number> {
  const startedAt = new Date();
  const scanRunId = await openScanRun(startedAt);

  try {
    const [pd, n8n] = [await collectPipedream(), await collectN8n()];
    const rows = [...pd.rows, ...n8n.rows];
    console.log(`[webhook-providers] collected: pipedream=${pd.rows.length} n8n=${n8n.rows.length} total=${rows.length}`);
    if (pd.rows.length === 0 || n8n.rows.length === 0) {
      throw new Error(`a source returned zero rows (pipedream=${pd.rows.length}, n8n=${n8n.rows.length}) — refusing a half-empty corpus`);
    }

    // Existing hashes → snapshot only what is new or changed.
    // PAGINATED: PostgREST caps un-ranged selects at 1000 rows, and a
    // truncated read here silently reclassifies every truncated-away artifact
    // as 'new' and re-snapshots it (caught live 2026-08-01: run 2 wrote
    // exactly corpus-1000 = 287 phantom snapshots).
    // Pagination KEPT under direct pg even though the PostgREST 1000-row cap
    // no longer applies: it bounds per-query memory on a growing corpus and
    // preserves the audited access pattern (sweep doc 2026-08-01) — dropping
    // it would re-introduce the un-bounded-read shape the sweep hunts for.
    const existing: { id: string; source: string; external_id: string; definition_hash: string }[] = [];
    for (let from = 0; ; from += 1000) {
      const page = await q<{ id: string; source: string; external_id: string; definition_hash: string }>(
        'SELECT id, source, external_id, definition_hash FROM registry_artifacts WHERE kind = $1 ORDER BY id LIMIT 1000 OFFSET $2',
        [KIND, from],
      );
      existing.push(...page);
      if (page.length < 1000) break;
    }
    const existingByKey = new Map(
      (existing ?? []).map((r) => [`${r.source} ${r.external_id}`, r]),
    );

    const nowIso = new Date().toISOString();
    const upserts = rows.map((r) => ({
      kind: KIND,
      source: r.source,
      external_id: r.external_id,
      name: r.name,
      homepage_url: r.homepage_url,
      manifest: r.manifest,
      definition_hash: hashCanonical(r.manifest),
      status: 'active',
      last_seen: nowIso,
      updated_at: nowIso,
    }));

    // Chunked upsert, returning ids so snapshots can reference them.
    const UPSERT_COLS = ['kind', 'source', 'external_id', 'name', 'homepage_url', 'manifest', 'definition_hash', 'status', 'last_seen', 'updated_at'];
    const idByKey = new Map<string, string>();
    for (let i = 0; i < upserts.length; i += 500) {
      const chunk = upserts.slice(i, i + 500);
      const returned = await upsertRows<{ id: string; source: string; external_id: string }>({
        table: 'registry_artifacts',
        cols: UPSERT_COLS,
        rows: chunk.map((u) => [u.kind, u.source, u.external_id, u.name, u.homepage_url, JSON.stringify(u.manifest), u.definition_hash, u.status, u.last_seen, u.updated_at]),
        conflictCols: ['kind', 'source', 'external_id'],
        updateCols: ['name', 'homepage_url', 'manifest', 'definition_hash', 'status', 'last_seen', 'updated_at'],
        returning: ['id', 'source', 'external_id'],
      });
      for (const r of returned) idByKey.set(`${r.source} ${r.external_id}`, r.id);
    }

    // Snapshots: write-on-change only.
    const snapshotRows = upserts
      .filter((u) => {
        const prev = existingByKey.get(`${u.source} ${u.external_id}`);
        return !prev || prev.definition_hash !== u.definition_hash;
      })
      .map((u) => ({
        artifact_id: idByKey.get(`${u.source} ${u.external_id}`)!,
        captured_at: nowIso,
        definition_hash: u.definition_hash,
        manifest: u.manifest,
      }))
      .filter((s) => s.artifact_id);

    for (let i = 0; i < snapshotRows.length; i += 500) {
      await insertRows(
        'registry_artifact_snapshots',
        ['artifact_id', 'captured_at', 'definition_hash', 'manifest'],
        snapshotRows.slice(i, i + 500).map((r) => [r.artifact_id, r.captured_at, r.definition_hash, JSON.stringify(r.manifest)]),
      );
    }

    const notes = [...pd.notes, ...n8n.notes];
    console.log(
      `[webhook-providers] done — upserted=${upserts.length} snapshots=${snapshotRows.length} ` +
      `apiCalls=${apiCalls} rawFetches=${rawFetches}` +
      (notes.length ? ` notes=${notes.length}` : ''),
    );

    await closeScanRun(scanRunId, {
      pages_fetched: apiCalls + rawFetches,
      servers_returned: upserts.length,
      status: 'ok',
    });

    // ── Freshness tripwire (see FRESHNESS_HOURS comment) ──
    const fresh = await q<{ last_seen: string }>(
      'SELECT last_seen FROM registry_artifacts WHERE kind = $1 ORDER BY last_seen DESC LIMIT 1',
      [KIND],
    );
    const newest = fresh[0]?.last_seen ? Date.parse(String(fresh[0].last_seen)) : 0;
    const ageHours = (Date.now() - newest) / 3_600_000;
    if (!newest || ageHours > FRESHNESS_HOURS) {
      console.error(`[webhook-providers] FRESHNESS TRIPWIRE: newest last_seen is ${ageHours.toFixed(1)}h old (limit ${FRESHNESS_HOURS}h)`);
      return 1;
    }

    return 0;
  } catch (err: any) {
    console.error('[webhook-providers] Fatal:', err?.message ?? err);
    await closeScanRun(scanRunId, {
      pages_fetched: apiCalls + rawFetches,
      servers_returned: null,
      status: `error: ${String(err?.message ?? err).slice(0, 200)}`,
    });
    return 1;
  }
}

const isMain = process.argv[1]?.includes('ingest-webhook-providers');
if (isMain) {
  // process.exitCode, not process.exit(): a hard exit races supabase-js's
  // open handles on Windows (see scripts/check-scheduler-dark.ts).
  ingestWebhookProviders()
    .then(async (code) => { await endIngestPool(); process.exitCode = code; })
    .catch(async (err) => {
      console.error('[webhook-providers] Fatal:', err?.message ?? err);
      await endIngestPool();
      process.exitCode = 1;
    });
}
