/**
 * lib/ingest/db.ts — the ONE connection point for the public-data ingest
 * workers (ingest-mcp-registry, ingest-homebrew-formulae,
 * ingest-npm-sdk-packages, ingest-webhook-providers).
 *
 * Direct Postgres via the scoped `aive_ingest` role — NOT supabase-js, NOT
 * PostgREST, NOT the service-role key. The role's entire surface is six
 * tables (registry_artifacts, registry_artifact_snapshots, scan_runs,
 * mcp_servers, mcp_server_snapshots, ingestion_log) with no DELETE anywhere;
 * a leaked AIVE_INGEST_DATABASE_URL can vandalize re-ingestable public data,
 * not read PII or destroy anything.
 *
 * Env: AIVE_INGEST_DATABASE_URL (fail-closed — no fallback to the service
 * key; these workers must never silently re-acquire god-mode).
 *
 * TLS note: pg treats `sslmode=require` in the URL as verify-full, which
 * rejects Supabase's chain. The URL's query string is stripped and TLS is
 * configured explicitly instead.
 *
 * Host note (corrected 2026-08-01): the direct host (db.<ref>.supabase.co)
 * is IPv6-ONLY — fine from IPv6 networks, unreachable from GitHub Actions
 * runners (ENETUNREACH). CI must use the Supavisor pooler (IPv4) with the
 * tenant-suffixed username (role.<project-ref>) at the project's ACTUAL
 * pooler host — read it from the dashboard/Management API, never guess:
 * this project is aws-1-us-east-1, and a wrong host (aws-0) fails with
 * "Tenant or user not found", which was earlier misread as the pooler
 * rejecting custom roles. Custom roles work through the pooler (verified
 * 2026-08-01, session and transaction mode).
 *
 * Relative-import discipline: these modules are
 * reached from `npx tsx workers/...` where "@/" aliases don't apply.
 */

import { Pool } from 'pg';

let pool: Pool | null = null;

export function ingestPool(): Pool {
  if (!pool) {
    const raw = process.env.AIVE_INGEST_DATABASE_URL;
    if (!raw) {
      throw new Error('AIVE_INGEST_DATABASE_URL not set — ingest workers refuse to run without the scoped role (no service-key fallback by design)');
    }
    const u = new URL(raw);
    u.search = ''; // TLS handled explicitly; see header
    pool = new Pool({
      connectionString: u.toString(),
      max: 4, // matches the role's CONNECTION LIMIT
      ssl: { rejectUnauthorized: false },
      connectionTimeoutMillis: 20_000,
    });
  }
  return pool;
}

/** Parameterized query returning rows. */
export async function q<T = Record<string, unknown>>(text: string, params?: unknown[]): Promise<T[]> {
  const res = await ingestPool().query(text, params);
  return res.rows as T[];
}

/**
 * Multi-row INSERT ... ON CONFLICT DO UPDATE. `rows` is an array of value
 * arrays aligned with `cols`. jsonb values must be pre-serialized with
 * JSON.stringify by the caller — node-pg would otherwise format JS arrays as
 * Postgres array literals and corrupt jsonb columns.
 */
export async function upsertRows<T = Record<string, unknown>>(opts: {
  table: string;
  cols: string[];
  rows: unknown[][];
  conflictCols: string[];
  updateCols: string[];
  returning?: string[];
}): Promise<T[]> {
  if (opts.rows.length === 0) return [];
  const width = opts.cols.length;
  const values: unknown[] = [];
  const tuples = opts.rows.map((row, r) => {
    if (row.length !== width) throw new Error(`upsertRows: row ${r} has ${row.length} values for ${width} cols`);
    values.push(...row);
    return `(${row.map((_, c) => `$${r * width + c + 1}`).join(',')})`;
  });
  const sql =
    `INSERT INTO ${opts.table} (${opts.cols.join(',')}) VALUES ${tuples.join(',')} ` +
    `ON CONFLICT (${opts.conflictCols.join(',')}) DO UPDATE SET ` +
    opts.updateCols.map((c) => `${c} = EXCLUDED.${c}`).join(', ') +
    (opts.returning?.length ? ` RETURNING ${opts.returning.join(',')}` : '');
  const res = await ingestPool().query(sql, values);
  return res.rows as T[];
}

/**
 * Multi-row INSERT. No RETURNING.
 *
 * @param conflictTarget  When set, appends ON CONFLICT (<target>) DO NOTHING.
 *   OPT-IN, never the default: every other caller treats a duplicate as a real
 *   error and must keep doing so. Returns the number of rows ACTUALLY inserted,
 *   which is not rows.length when a conflict is skipped -- callers that report
 *   persistence counts must use the return value, or a silently dropped row
 *   reads as a written one.
 */
export async function insertRows(
  table: string, cols: string[], rows: unknown[][],
  conflictTarget?: string, conflictWhere?: string,
): Promise<number> {
  if (rows.length === 0) return 0;
  const width = cols.length;
  const values: unknown[] = [];
  const tuples = rows.map((row, r) => {
    values.push(...row);
    return `(${row.map((_, c) => `$${r * width + c + 1}`).join(',')})`;
  });
  const onConflict = conflictTarget
    ? ` ON CONFLICT (${conflictTarget})${conflictWhere ? ` WHERE ${conflictWhere}` : ''} DO NOTHING`
    : '';
  const res = await ingestPool().query(
    `INSERT INTO ${table} (${cols.join(',')}) VALUES ${tuples.join(',')}${onConflict}`, values);
  return res.rowCount ?? 0;
}

/**
 * Run `fn` inside a transaction that is ALWAYS rolled back, on ONE pinned
 * connection.
 *
 * WHY THIS EXISTS AND WHY IT IS NOT JUST q('BEGIN'). q() calls
 * ingestPool().query(), which checks a connection out of the pool PER CALL. A
 * BEGIN issued that way can land on a different connection from the INSERT that
 * follows and the ROLLBACK after it — which would leave the write COMMITTED
 * (each statement autocommitting on its own connection) and a transaction
 * dangling open on a third. For a probe whose whole purpose is to write and then
 * not persist, that is the one failure mode that must be impossible.
 *
 * So: one client checked out explicitly, BEGIN, callback, ROLLBACK in a
 * `finally`, release in an outer `finally`. The rollback runs whether the
 * callback returns or throws, and a failed ROLLBACK throws rather than being
 * swallowed — a probe must never report success while its writes are still live.
 *
 * This guarantees the ROLLBACK was ISSUED. It does not by itself prove nothing
 * persisted; callers verify that separately by row count and sentinel search,
 * because those are different claims.
 */
export async function withRollback<T>(
  fn: (exec: (text: string, params?: unknown[]) => Promise<unknown[]>) => Promise<T>,
): Promise<T> {
  const client = await ingestPool().connect();
  try {
    await client.query('BEGIN');
    try {
      const exec = async (text: string, params?: unknown[]) =>
        (await client.query(text, params)).rows as unknown[];
      return await fn(exec);
    } finally {
      await client.query('ROLLBACK');
    }
  } finally {
    client.release();
  }
}

/** Drain the pool so the process can exit naturally (process.exitCode style). */
export async function endIngestPool(): Promise<void> {
  const p = pool;
  pool = null;
  await p?.end();
}

// ── ingest-local ingestion_log writer ───────────────────────────────────────
// A VARIANT of workers/logIngestion.ts, deliberately not a refactor of it:
// the shared helper constructs a supabase-js service-key client at module
// load, and its private-repo callers (scheduler tick, cron routes, other
// workers) keep that behavior untouched. The public-repo workers need an
// import tree with ZERO references to supabase-js or the service key, which
// an injectable-client refactor of the shared module would not achieve.
// Same row shape, same never-throws contract.

export interface IngestionResult {
  sourceSlug: string;
  startedAt: Date;
  itemsFetched?: number;
  itemsNew?: number;
  itemsDuplicate?: number;
  itemsFailed?: number;
  errorMessage?: string;
  metadata?: Record<string, unknown>;
}

export async function logIngestionPg(result: IngestionResult): Promise<void> {
  const completedAt = new Date();
  try {
    await q(
      `INSERT INTO ingestion_log
         (source_slug, started_at, completed_at, items_fetched, items_new,
          items_duplicate, items_failed, duration_ms, error_message, metadata)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [
        result.sourceSlug,
        result.startedAt.toISOString(),
        completedAt.toISOString(),
        result.itemsFetched ?? 0,
        result.itemsNew ?? 0,
        result.itemsDuplicate ?? 0,
        result.itemsFailed ?? 0,
        completedAt.getTime() - result.startedAt.getTime(),
        result.errorMessage ?? null,
        result.metadata ? JSON.stringify(result.metadata) : null,
      ],
    );
  } catch (err: any) {
    console.error(`[logIngestionPg] Failed to log ${result.sourceSlug}:`, err?.message ?? err);
  }
}
