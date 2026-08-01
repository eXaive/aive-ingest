// ================================================================
// npm keyword-tagged SDK ingest — registry_artifacts kind='sdk'
//
// THE DATASET, precisely (load-bearing, do not soften): packages whose
// publishers SELF-TAGGED them with the npm keywords sdk / client /
// api-client. A FLOOR, not a census — the keyword field is optional and
// many widely-used SDKs carry no keyword. The panel renders a live
// absence example for exactly this reason.
//
// Sources, no API key:
//   https://registry.npmjs.org/-/v1/search?text=keywords:<kw>  (inventory)
//   https://api.npmjs.org/downloads/point/last-week/...        (downloads)
//
// SEARCH PAGINATION CAP: the search endpoint is Elasticsearch-backed and
// refuses from+size > 10,000 per query. Each keyword corpus larger than
// 10,000 is therefore TRUNCATED AT THE ENDPOINT'S CAP — this is logged
// per query and surfaced in scan_runs; pretending otherwise would be the
// silent-cap bug again.
//
// DOWNLOADS: the bulk endpoint accepts up to 128 UNSCOPED packages per
// request; scoped (@org/name) packages must be fetched one by one, so
// scoped fetches are rate-limited and CAPPED with the drop count logged
// loudly. A package with no download data (dropped, unknown, or new) gets
// metrics.weekly_downloads = null — unmeasured, never zero.
//
// HASH ISOLATION, same rule as the Homebrew ingester: definition_hash =
// hashCanonical(manifest) ONLY; volatile downloads live in metrics and a
// run where every download count moved writes ZERO snapshots.
//
// Run: npx tsx workers/ingest-npm-sdk-packages.ts   (npm run ingest:npm-sdk)
// Env: AIVE_INGEST_DATABASE_URL (scoped aive_ingest role)
// ================================================================

import { hashCanonical } from '../lib/mcp/canonicalize';
// Direct Postgres via the scoped aive_ingest role — no supabase-js, no
// service key. See lib/ingest/db.ts for the role surface and TLS notes.
import { q, upsertRows, insertRows, endIngestPool } from '../lib/ingest/db';

const KIND = 'sdk';
const SOURCE = 'npm';
const SEARCH_BASE = 'https://registry.npmjs.org/-/v1/search';
const DOWNLOADS_BASE = 'https://api.npmjs.org/downloads/point/last-week';
const KEYWORD_QUERIES = ['sdk', 'client', 'api-client'] as const;
const PAGE_SIZE = 250;
/** Elasticsearch window: the endpoint refuses from+size beyond this. */
const SEARCH_WINDOW_CAP = 10_000;
const BULK_DOWNLOADS_BATCH = 128;
/** Scoped packages need one request each — capped, drops logged loudly. */
const SCOPED_DOWNLOADS_CAP = 4_000;
const DOWNLOAD_CONCURRENCY = 6;
const FETCH_TIMEOUT_MS = 30_000;
const FRESHNESS_HOURS = 48;

interface SearchResult {
  total?: number;
  objects?: {
    package?: {
      name?: string;
      scope?: string;
      version?: string;
      description?: string;
      keywords?: string[];
      license?: string;
      publisher?: { username?: string };
      links?: { npm?: string; homepage?: string; repository?: string };
    };
  }[];
}

let httpCalls = 0;

async function fetchJson<T>(url: string): Promise<T | null> {
  httpCalls++;
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch {
    return null;
  }
}

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

async function openScanRun(startedAt: Date): Promise<string | null> {
  try {
    const rows = await q<{ id: string }>(
      "INSERT INTO scan_runs (started_at, status) VALUES ($1, 'running') RETURNING id",
      [startedAt.toISOString()],
    );
    return rows[0]?.id ?? null;
  } catch (err) {
    console.error('[npm-sdk] scan_runs insert failed:', err instanceof Error ? err.message : err);
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
  } catch (err) {
    console.error('[npm-sdk] scan_runs update failed:', err instanceof Error ? err.message : err);
  }
}

interface PkgRecord {
  name: string;
  scope: string | null;
  version: string | null;
  description: string | null;
  keywords: string[];
  license: string | null;
  publisher: string | null;
  homepage: string | null;
  repository: string | null;
  matched_queries: string[];
}

export async function ingestNpmSdkPackages(): Promise<number> {
  const startedAt = new Date();
  const scanRunId = await openScanRun(startedAt);

  try {
    /* ── 1. Search all three keyword corpora, paginating to the cap ─────── */
    const byName = new Map<string, PkgRecord>();
    const perQuery: Record<string, { fetched: number; upstreamTotal: number; truncated: boolean }> = {};

    for (const kw of KEYWORD_QUERIES) {
      let fetched = 0;
      let upstreamTotal = 0;
      for (let from = 0; from < SEARCH_WINDOW_CAP; from += PAGE_SIZE) {
        const size = Math.min(PAGE_SIZE, SEARCH_WINDOW_CAP - from);
        // The search endpoint throttles rapid sequential paging (observed
        // live: hard failure at from=2750 on the first run). Retry with
        // backoff, and pace pages politely below.
        let page: SearchResult | null = null;
        for (let attempt = 0; attempt < 4 && !page; attempt++) {
          if (attempt > 0) await new Promise((r) => setTimeout(r, 1500 * 2 ** attempt));
          page = await fetchJson<SearchResult>(
            `${SEARCH_BASE}?text=${encodeURIComponent(`keywords:${kw}`)}&size=${size}&from=${from}`,
          );
        }
        if (!page) throw new Error(`search keywords:${kw} from=${from} failed after 4 attempts`);
        await new Promise((r) => setTimeout(r, 300));
        upstreamTotal = page.total ?? upstreamTotal;
        const objects = page.objects ?? [];
        for (const o of objects) {
          const p = o.package;
          if (!p?.name) continue;
          fetched++;
          const prev = byName.get(p.name);
          if (prev) {
            if (!prev.matched_queries.includes(kw)) prev.matched_queries.push(kw);
            continue;
          }
          byName.set(p.name, {
            name: p.name,
            scope: p.scope && p.scope !== 'unscoped' ? p.scope : (p.name.startsWith('@') ? p.name.split('/')[0].slice(1) : null),
            version: p.version ?? null,
            description: p.description ?? null,
            keywords: [...new Set(p.keywords ?? [])].sort(),
            license: p.license ?? null,
            publisher: p.publisher?.username ?? (p.name.startsWith('@') ? p.name.split('/')[0].slice(1) : null),
            homepage: p.links?.homepage ?? p.links?.npm ?? null,
            repository: p.links?.repository ?? null,
            matched_queries: [kw],
          });
        }
        if (objects.length < size) break; // corpus exhausted before the cap
      }
      const truncated = upstreamTotal > SEARCH_WINDOW_CAP;
      perQuery[kw] = { fetched, upstreamTotal, truncated };
      console.log(
        `[npm-sdk] keywords:${kw} — fetched=${fetched} upstreamTotal=${upstreamTotal}` +
        (truncated ? ` TRUNCATED at the endpoint's ${SEARCH_WINDOW_CAP} search window` : ' (complete)'),
      );
    }

    const pkgs = [...byName.values()];
    if (pkgs.length === 0) throw new Error('search returned zero packages — refusing an empty corpus');
    console.log(`[npm-sdk] unique packages across queries: ${pkgs.length}`);

    /* ── 2. Weekly downloads: bulk for unscoped, capped singles for scoped ─ */
    const downloads = new Map<string, { downloads: number; start: string; end: string }>();
    const unscoped = pkgs.filter((p) => !p.name.startsWith('@'));
    const scoped = pkgs.filter((p) => p.name.startsWith('@'));

    for (let i = 0; i < unscoped.length; i += BULK_DOWNLOADS_BATCH) {
      const batch = unscoped.slice(i, i + BULK_DOWNLOADS_BATCH);
      const res = await fetchJson<Record<string, { downloads?: number; start?: string; end?: string } | null>>(
        `${DOWNLOADS_BASE}/${batch.map((p) => p.name).join(',')}`,
      );
      if (!res) {
        console.warn(`[npm-sdk] bulk downloads batch at ${i} failed — ${batch.length} packages left unmeasured`);
        continue;
      }
      // Single-package batches return the object directly, not keyed by name.
      const entries = batch.length === 1 ? { [batch[0].name]: res as any } : res;
      for (const [name, d] of Object.entries(entries)) {
        if (d && typeof d.downloads === 'number' && d.start && d.end) {
          downloads.set(name, { downloads: d.downloads, start: d.start, end: d.end });
        }
      }
    }

    const scopedToFetch = scoped.slice(0, SCOPED_DOWNLOADS_CAP);
    const scopedDropped = scoped.length - scopedToFetch.length;
    if (scopedDropped > 0) {
      console.warn(
        `[npm-sdk] scoped-downloads cap hit — ${scopedDropped}/${scoped.length} scoped packages left ` +
        `unmeasured this run (weekly_downloads=null). Not silent: this line and scan_runs record it.`,
      );
    }
    await mapPool(scopedToFetch, DOWNLOAD_CONCURRENCY, async (p) => {
      const d = await fetchJson<{ downloads?: number; start?: string; end?: string }>(
        `${DOWNLOADS_BASE}/${encodeURIComponent(p.name)}`,
      );
      if (d && typeof d.downloads === 'number' && d.start && d.end) {
        downloads.set(p.name, { downloads: d.downloads, start: d.start, end: d.end });
      }
      return null;
    });

    /* ── 3. Build rows — hash isolation: manifest hashed, metrics never ──── */
    const nowIso = new Date().toISOString();
    let unmeasured = 0;
    const upserts = pkgs.map((p) => {
      // manifest = identity + packaging facts only (the publisher's own
      // assertions). NOTE: the search API does not expose npm deprecation, so
      // `deprecated` is null-unknown here rather than a fabricated false —
      // resolving it would need one packument fetch per package.
      const manifest = {
        name: p.name,
        scope: p.scope,
        license: p.license,
        keywords: p.keywords,
        deprecated: null as boolean | null,
        repository: p.repository,
        matched_queries: [...p.matched_queries].sort(),
      };
      const dl = downloads.get(p.name) ?? null;
      if (!dl) unmeasured++;
      const metrics = {
        weekly_downloads: dl?.downloads ?? null,
        window_start: dl?.start ?? null,
        window_end: dl?.end ?? null,
      };
      return {
        kind: KIND,
        source: SOURCE,
        external_id: p.name,
        name: p.name,
        publisher: p.publisher,
        version: p.version,
        description: p.description,
        homepage_url: p.homepage,
        manifest,
        metrics,
        definition_hash: hashCanonical(manifest),
        status: 'active',
        last_seen: nowIso,
        updated_at: nowIso,
      };
    });

    /* ── 4. Existing hashes — PAGINATED (PostgREST 1000-row cap) ─────────── */
    // Pagination KEPT under direct pg even though the PostgREST 1000-row cap
    // no longer applies: it bounds per-query memory on a growing corpus and
    // preserves the audited access pattern (sweep doc 2026-08-01).
    const existing: { id: string; external_id: string; definition_hash: string }[] = [];
    for (let from = 0; ; from += 1000) {
      const page = await q<{ id: string; external_id: string; definition_hash: string }>(
        'SELECT id, external_id, definition_hash FROM registry_artifacts WHERE kind = $1 AND source = $2 ORDER BY id LIMIT 1000 OFFSET $3',
        [KIND, SOURCE, from],
      );
      existing.push(...page);
      if (page.length < 1000) break;
    }
    const existingByKey = new Map(existing.map((r) => [r.external_id, r]));

    const UPSERT_COLS = ['kind', 'source', 'external_id', 'name', 'publisher', 'version', 'description', 'homepage_url', 'manifest', 'metrics', 'definition_hash', 'status', 'last_seen', 'updated_at'];
    const idByKey = new Map<string, string>();
    for (let i = 0; i < upserts.length; i += 500) {
      const chunk = upserts.slice(i, i + 500);
      const returned = await upsertRows<{ id: string; external_id: string }>({
        table: 'registry_artifacts',
        cols: UPSERT_COLS,
        rows: chunk.map((u) => [u.kind, u.source, u.external_id, u.name, u.publisher, u.version, u.description, u.homepage_url, JSON.stringify(u.manifest), JSON.stringify(u.metrics), u.definition_hash, u.status, u.last_seen, u.updated_at]),
        conflictCols: ['kind', 'source', 'external_id'],
        updateCols: ['name', 'publisher', 'version', 'description', 'homepage_url', 'manifest', 'metrics', 'definition_hash', 'status', 'last_seen', 'updated_at'],
        returning: ['id', 'external_id'],
      });
      for (const r of returned) idByKey.set(r.external_id, r.id);
    }

    /* ── 5. Snapshots: write-on-change of the manifest hash only ─────────── */
    const snapshotRows = upserts
      .filter((u) => existingByKey.get(u.external_id)?.definition_hash !== u.definition_hash)
      .map((u) => ({
        artifact_id: idByKey.get(u.external_id)!,
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

    console.log(
      `[npm-sdk] done — upserted=${upserts.length} snapshots=${snapshotRows.length} ` +
      `measured=${upserts.length - unmeasured} unmeasured(weekly_downloads=null)=${unmeasured} ` +
      `httpCalls=${httpCalls} scopedDropped=${scopedDropped}`,
    );

    await closeScanRun(scanRunId, {
      pages_fetched: httpCalls,
      servers_returned: upserts.length,
      status:
        `ok${Object.values(perQuery).some((q) => q.truncated) ? ' (search truncated at 10k window per query)' : ''}` +
        (scopedDropped > 0 ? ` (${scopedDropped} scoped downloads dropped)` : ''),
    });

    // Freshness tripwire (same shape as the sibling ingesters).
    const fresh = await q<{ last_seen: string }>(
      'SELECT last_seen FROM registry_artifacts WHERE kind = $1 ORDER BY last_seen DESC LIMIT 1',
      [KIND],
    );
    const newest = fresh[0]?.last_seen ? Date.parse(String(fresh[0].last_seen)) : 0;
    const ageHours = (Date.now() - newest) / 3_600_000;
    if (!newest || ageHours > FRESHNESS_HOURS) {
      console.error(`[npm-sdk] FRESHNESS TRIPWIRE: newest last_seen is ${ageHours.toFixed(1)}h old (limit ${FRESHNESS_HOURS}h)`);
      return 1;
    }
    return 0;
  } catch (err: any) {
    console.error('[npm-sdk] Fatal:', err?.message ?? err);
    await closeScanRun(scanRunId, {
      pages_fetched: httpCalls,
      servers_returned: null,
      status: `error: ${String(err?.message ?? err).slice(0, 200)}`,
    });
    return 1;
  }
}

const isMain = process.argv[1]?.includes('ingest-npm-sdk-packages');
if (isMain) {
  // process.exitCode, not process.exit(): hard exits race supabase-js handles
  // on Windows (see scripts/check-scheduler-dark.ts).
  ingestNpmSdkPackages()
    .then(async (code) => { await endIngestPool(); process.exitCode = code; })
    .catch(async (err) => {
      console.error('[npm-sdk] Fatal:', err?.message ?? err);
      await endIngestPool();
      process.exitCode = 1;
    });
}
