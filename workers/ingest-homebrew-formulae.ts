// ================================================================
// Homebrew formulae ingest — registry_artifacts kind='cli'
//
// Inventory + drift for CLI tools as packaged by homebrew-core. Two public
// endpoints, no API key:
//   https://formulae.brew.sh/api/formula.json            — the inventory
//   https://formulae.brew.sh/api/analytics/install/30d.json — install counts
//
// HASH ISOLATION — the load-bearing rule of this worker: install counts are a
// rolling 30-day window that moves daily even when nothing about a tool
// changes. They live in the `metrics` jsonb ONLY, and definition_hash is
// computed with hashCanonical over `manifest` ONLY — never over metrics. A
// daily run against an unchanged upstream therefore writes ZERO snapshots
// even though every install count moved; drift history measures the tools,
// not the calendar.
//
// JOIN GAP, handled honestly: the analytics endpoint does not cover every
// formula. A formula with no analytics row gets metrics.install_30d = null
// (unmeasured) — never 0, which would claim "nobody installed it".
//
// Pattern per workers/ingest-webhook-providers.ts: canonical hash →
// snapshot-on-change → scan_runs → PAGINATED existing-hash reads (PostgREST
// caps un-ranged selects at 1000; this corpus is thousands of rows, the
// un-ranged form silently truncates — the exact bug swept on 2026-08-01).
//
// Run: npx tsx workers/ingest-homebrew-formulae.ts   (npm run ingest:homebrew-formulae)
// Env: AIVE_INGEST_DATABASE_URL (scoped aive_ingest role)
// ================================================================

import { hashCanonical } from '../lib/mcp/canonicalize';
// Direct Postgres via the scoped aive_ingest role — no supabase-js, no
// service key. See lib/ingest/db.ts for the role surface and TLS notes.
import { q, upsertRows, insertRows, endIngestPool } from '../lib/ingest/db';

const KIND = 'cli';
const SOURCE = 'homebrew';
const FORMULA_URL = 'https://formulae.brew.sh/api/formula.json';
const ANALYTICS_URL = 'https://formulae.brew.sh/api/analytics/install/30d.json';
const ON_REQUEST_URL = 'https://formulae.brew.sh/api/analytics/install-on-request/30d.json';
const FETCH_TIMEOUT_MS = 120_000;

// ── artifact_type classification (tool vs library) ──────────────────────────
// Derived HERE, in the worker; the page renders whatever manifest says.
//
// FIELD: formula.json's `executables` (present on 8,331/8,528 bulk entries,
// 2026-08-01). Reliability, verified against the live payload: it is
// authoritative for "ships binaries" but OVER-INCLUSIVE for toolhood —
// libraries bundle helper utilities (glib ships gdbus, fontconfig ships
// fc-*, harfbuzz ships hb-*), so "has executables" alone would re-admit the
// exact dependency-pressure rows this classification exists to exclude.
// Therefore:
//   1. executables empty or key missing → 'library' (definitive: nothing to
//      run ⇒ not a CLI tool; ca-certificates, gmp, icu4c land here)
//   2. executables non-empty → discriminate with Homebrew's OWN
//      request-vs-dependency signal: install-on-request/install ratio over
//      the same 30d window. ratio ≥ 0.5 (with install ≥ 100 for signal
//      floor) → 'tool'; below → 'library'. Verified: jq .989→tool,
//      ripgrep .677→tool, cmake .713→tool; glib .128, ca-certs .030,
//      gmp .029 → library.
//   3. No/insufficient analytics → 'library' (the required default:
//      defaulting toward tool would inflate the headline this panel is
//      named for).
// The ratio is volatile; a borderline formula flipping type across runs
// re-snapshots that row — accepted as a genuine change in what the row
// claims to be, not hash noise.
const RATIO_THRESHOLD = 0.5;
const INSTALL_FLOOR = 100;

function classifyArtifactType(
  executables: string[] | undefined,
  install: number | null,
  onRequest: number | null,
): { artifact_type: 'tool' | 'library'; classification_basis: string } {
  if (!executables || executables.length === 0) {
    return { artifact_type: 'library', classification_basis: 'no-executables' };
  }
  if (install !== null && install >= INSTALL_FLOOR && onRequest !== null) {
    return {
      artifact_type: onRequest / install >= RATIO_THRESHOLD ? 'tool' : 'library',
      classification_basis: 'on-request-ratio',
    };
  }
  return { artifact_type: 'library', classification_basis: 'unmatched-default' };
}

/** Same posture as the webhook worker: last_seen is stamped on every row every
 *  successful run, so its age measures run liveness; snapshots are
 *  write-on-change and would false-alarm. */
const FRESHNESS_HOURS = 48;

interface Formula {
  name: string;
  full_name?: string;
  tap?: string;
  desc?: string | null;
  license?: string | null;
  homepage?: string | null;
  versions?: { stable?: string | null };
  deprecated?: boolean;
  disabled?: boolean;
  dependencies?: string[];
  caveats?: string | null;
  executables?: string[];
}

interface AnalyticsPayload {
  start_date?: string;
  end_date?: string;
  items?: { formula?: string; count?: string | number }[];
}

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(url, { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
  if (!res.ok) throw new Error(`${url}: HTTP ${res.status}`);
  return (await res.json()) as T;
}

/** Analytics counts arrive as strings with thousands separators ("1,234,567"). */
function parseCount(v: string | number | undefined): number | null {
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  if (typeof v === 'string') {
    const n = Number(v.replace(/,/g, ''));
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

async function openScanRun(startedAt: Date): Promise<string | null> {
  try {
    const rows = await q<{ id: string }>(
      "INSERT INTO scan_runs (started_at, status) VALUES ($1, 'running') RETURNING id",
      [startedAt.toISOString()],
    );
    return rows[0]?.id ?? null;
  } catch (err) {
    console.error('[homebrew] scan_runs insert failed:', err instanceof Error ? err.message : err);
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
    console.error('[homebrew] scan_runs update failed:', err instanceof Error ? err.message : err);
  }
}

export async function ingestHomebrewFormulae(): Promise<number> {
  const startedAt = new Date();
  const scanRunId = await openScanRun(startedAt);
  let fetches = 0;

  try {
    fetches++;
    const formulae = await fetchJson<Formula[]>(FORMULA_URL);
    fetches++;
    const analytics = await fetchJson<AnalyticsPayload>(ANALYTICS_URL);
    fetches++;
    const onRequestPayload = await fetchJson<AnalyticsPayload>(ON_REQUEST_URL);
    if (!Array.isArray(formulae) || formulae.length === 0) {
      throw new Error('formula.json returned zero formulae — refusing an empty corpus');
    }

    const installByFormula = new Map<string, number>();
    for (const item of analytics.items ?? []) {
      const n = parseCount(item.count);
      if (item.formula && n !== null) installByFormula.set(item.formula, n);
    }
    const onRequestByFormula = new Map<string, number>();
    for (const item of onRequestPayload.items ?? []) {
      const n = parseCount(item.count);
      if (item.formula && n !== null) onRequestByFormula.set(item.formula, n);
    }
    console.log(`[homebrew] formulae=${formulae.length} analyticsItems=${installByFormula.size} onRequestItems=${onRequestByFormula.size} window=${analytics.start_date}..${analytics.end_date}`);

    const nowIso = new Date().toISOString();
    let unmeasured = 0;

    const upserts = formulae.map((f) => {
      const install = installByFormula.get(f.name) ?? null;
      const onRequest = onRequestByFormula.get(f.name) ?? null;
      const cls = classifyArtifactType(f.executables, install, onRequest);
      // manifest = identity + packaging facts ONLY. definition_hash covers
      // exactly this object; anything volatile belongs in metrics below.
      // artifact_type is derived partly from volatile analytics but is itself
      // a claim about what the row IS — a borderline flip legitimately
      // re-snapshots that row (see classifyArtifactType header).
      const manifest = {
        name: f.name,
        full_name: f.full_name ?? f.name,
        tap: f.tap ?? null,
        license: f.license ?? null,
        deprecated: f.deprecated ?? false,
        disabled: f.disabled ?? false,
        dependencies: [...(f.dependencies ?? [])].sort(),
        has_caveats: f.caveats != null && String(f.caveats).trim() !== '',
        executables: [...(f.executables ?? [])].sort(),
        artifact_type: cls.artifact_type,
        classification_basis: cls.classification_basis,
      };
      // metrics = volatile popularity, NEVER hashed. install_30d is null (not
      // 0) when the analytics join has no row — unmeasured ≠ uninstalled. The
      // window dates travel with the number so it is never shown windowless.
      if (install === null) unmeasured++;
      const metrics = {
        install_30d: install,
        install_on_request_30d: onRequest,
        window_start: analytics.start_date ?? null,
        window_end: analytics.end_date ?? null,
      };
      return {
        kind: KIND,
        source: SOURCE,
        external_id: f.name,
        name: f.name,
        // Homebrew has no publisher field; null beats inventing one.
        publisher: null as string | null,
        version: f.versions?.stable ?? null,
        description: f.desc ?? null,
        homepage_url: f.homepage ?? null,
        manifest,
        metrics,
        definition_hash: hashCanonical(manifest),
        status: f.disabled ? 'disabled' : f.deprecated ? 'deprecated' : 'active',
        last_seen: nowIso,
        updated_at: nowIso,
      };
    });

    // Existing hashes — PAGINATED (PostgREST 1000-row cap; corpus is ~7-13k).
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

    // Snapshots: write-on-change of the MANIFEST hash only.
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

    const tools = upserts.filter((u) => (u.manifest as { artifact_type?: string }).artifact_type === 'tool').length;
    console.log(
      `[homebrew] done — upserted=${upserts.length} snapshots=${snapshotRows.length} ` +
      `measured=${upserts.length - unmeasured} unmeasured(install_30d=null)=${unmeasured} ` +
      `tools=${tools} libraries=${upserts.length - tools}`,
    );

    await closeScanRun(scanRunId, {
      pages_fetched: fetches,
      servers_returned: upserts.length,
      status: 'ok',
    });

    // Freshness tripwire (same shape as the webhook worker).
    const fresh = await q<{ last_seen: string }>(
      'SELECT last_seen FROM registry_artifacts WHERE kind = $1 ORDER BY last_seen DESC LIMIT 1',
      [KIND],
    );
    const newest = fresh[0]?.last_seen ? Date.parse(String(fresh[0].last_seen)) : 0;
    const ageHours = (Date.now() - newest) / 3_600_000;
    if (!newest || ageHours > FRESHNESS_HOURS) {
      console.error(`[homebrew] FRESHNESS TRIPWIRE: newest last_seen is ${ageHours.toFixed(1)}h old (limit ${FRESHNESS_HOURS}h)`);
      return 1;
    }
    return 0;
  } catch (err: any) {
    console.error('[homebrew] Fatal:', err?.message ?? err);
    await closeScanRun(scanRunId, {
      pages_fetched: fetches,
      servers_returned: null,
      status: `error: ${String(err?.message ?? err).slice(0, 200)}`,
    });
    return 1;
  }
}

const isMain = process.argv[1]?.includes('ingest-homebrew-formulae');
if (isMain) {
  // process.exitCode, not process.exit(): hard exits race supabase-js handles
  // on Windows (see scripts/check-scheduler-dark.ts).
  ingestHomebrewFormulae()
    .then(async (code) => { await endIngestPool(); process.exitCode = code; })
    .catch(async (err) => {
      console.error('[homebrew] Fatal:', err?.message ?? err);
      await endIngestPool();
      process.exitCode = 1;
    });
}
