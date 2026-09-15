/**
 * Proves the checkpointed daily-full-walk design (2026-09-15, revised
 * 2026-09-16) end to end against a REAL loopback registry and an in-memory
 * store implementing the worker's RegistryStore contract. The same reasoning
 * as verify-registry-retry.ts: a real HTTP server exercises the actual
 * Response, headers and control flow.
 *
 * WHAT IS PROVEN
 *   1. A backfill run stops at its budget, exits 0, persists the cursor, and
 *      writes ingestion_log rows with no error (the watchdog's gap resets).
 *   2. The next run resumes from the persisted cursor, not the first page.
 *   3. Reaching nextCursor=null RE-ARMS for another full walk: phase stays
 *      'backfill' (does NOT flip to delta), cursor clears, last_full_pass_at
 *      stamps, the watermark refreshes to this run's own anchor, and every
 *      server lands once at its isLatest version.
 *   4. last_seen keeps advancing on a SECOND consecutive full pass — the
 *      "confirmed alive today" guarantee this design exists to protect —
 *      and the phase is still 'backfill' going in and coming out of it.
 *   5. Delta stays a working code path when a checkpoint is manually put into
 *      it (an operator override): updated_since=<watermark> and
 *      include_deleted=true are sent, only changed servers are upserted,
 *      active->deleted is recorded, and a completed delta run stays in delta
 *      (nothing auto-reverts it — only an operator, or the oversized-budget
 *      fallback in #12, ever changes it).
 *   6. Re-running the same watermark writes zero new rows (idempotent upsert).
 *   7. Retry-After is honoured and the sleep source is logged.
 *   8. A mid-walk 500 that outlasts the ladder commits and checkpoints, exits
 *      0, and the next run resumes to completion (still re-arming to
 *      'backfill', not flipping to delta).
 *   9. A Retry-After longer than the remaining budget is not cut short: the
 *      run checkpoints instead of retrying early.
 *  10. A registry that answers no page exits 1 and logs an error (the
 *      watchdog's consecutive-failure signal).
 *  11. A server whose versions straddle a batch boundary is written once, at
 *      its isLatest version — no interim version, no spurious snapshot.
 *  12. A delta too large for the budget falls back to a fresh backfill.
 *  13. A write failure never advances the cursor past the rows that failed.
 *  14. The duration-warning predicate fires at/above 60% of the job's
 *      30-minute budget and not below it, and is wired into both RunResult
 *      and the ingestion_log row for the run.
 *
 * RUNTIME ~16 s. No database, no network, no credentials.
 * Run: npx tsx scripts/verify-registry-checkpoint.ts
 */

import assert from 'node:assert/strict';
import http from 'node:http';
import { AddressInfo } from 'node:net';

import {
  ingestMCPRegistry, exitCodeForRun, planFlush, durationWarningFires, DURATION_WARNING_MS,
  type RegistryStore, type Checkpoint, type ExistingServer, type SnapshotRow, type ServerRow,
  type RunOptions, type RunResult, type CacheRefreshBundle, type MCPRegistryItem, type ScanRunClose,
} from '../workers/ingest-mcp-registry';
import type { IngestionResult } from '../lib/ingest/db';

const LOOKBACK_MS = 5 * 60_000;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ── Loopback registry ────────────────────────────────────────────────────────

interface Entry { name: string; version: string; status: string; updatedAt: string; isLatest: boolean; description: string }
type Hook = (requestNo: number, url: URL) => { status: number; retryAfter?: string } | null;

const keyOf = (e: Entry) => `${e.name}\u0000${e.version.split('.').map((n) => n.padStart(6, '0')).join('.')}`;
const toItem = (e: Entry): MCPRegistryItem => ({
  server: { name: e.name, version: e.version, description: e.description },
  _meta: { 'io.modelcontextprotocol.registry/official': { status: e.status, updatedAt: e.updatedAt, isLatest: e.isLatest } },
});

/** Pages entries in (name, version) order with name-based cursors, like the real registry. */
class FakeRegistry {
  entries: Entry[];
  requests: URL[] = [];
  pageSize = 10;
  delayMs = 0;
  hook: Hook | null = null;
  private server: http.Server;
  constructor(entries: Entry[]) {
    this.entries = entries;
    this.server = http.createServer(async (req, res) => {
      const url = new URL(req.url ?? '/', 'http://loopback');
      this.requests.push(url);
      const h = this.hook?.(this.requests.length, url);
      if (h) {
        if (h.retryAfter !== undefined) res.setHeader('retry-after', h.retryAfter);
        res.writeHead(h.status, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ title: 'Internal Server Error', status: h.status }));
        return;
      }
      if (this.delayMs) await sleep(this.delayMs);
      const since = url.searchParams.get('updated_since');
      const includeDeleted = url.searchParams.get('include_deleted') === 'true';
      const after = url.searchParams.get('cursor');
      const pool = [...this.entries]
        .sort((a, b) => keyOf(a).localeCompare(keyOf(b)))
        .filter((e) => includeDeleted || e.status !== 'deleted')
        .filter((e) => !since || e.updatedAt >= since)
        .filter((e) => !after || keyOf(e) > after);
      const page = pool.slice(0, this.pageSize);
      const next = pool.length > this.pageSize ? keyOf(page[page.length - 1]) : null;
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ servers: page.map(toItem), metadata: { nextCursor: next, count: page.length } }));
    });
  }
  async start(): Promise<string> {
    await new Promise<void>((r) => this.server.listen(0, '127.0.0.1', () => r()));
    return `http://127.0.0.1:${(this.server.address() as AddressInfo).port}`;
  }
  stop(): Promise<void> { return new Promise((r) => this.server.close(() => r())); }
  /** Requests issued from index `from` on. */
  since(from: number): URL[] { return this.requests.slice(from); }
}

const OLD = '2026-09-01T00:00:00.000Z';
/** `n` servers; every 7th has three versions (the last flagged isLatest). */
function corpus(n: number): Entry[] {
  const out: Entry[] = [];
  for (let i = 0; i < n; i++) {
    const name = `io.example/s${String(i).padStart(4, '0')}`;
    const versions = i % 7 === 0 ? ['1.0.0', '1.1.0', '2.0.0'] : ['1.0.0'];
    versions.forEach((v, j) => out.push({
      name, version: v, status: 'active', updatedAt: OLD, isLatest: j === versions.length - 1, description: `server ${i}`,
    }));
  }
  return out;
}
const latestOf = (entries: Entry[]) => new Map(entries.filter((e) => e.isLatest).map((e) => [e.name, e]));

// ── In-memory store (the RegistryStore contract, with the table's CHECKs) ────

class MemoryStore implements RegistryStore {
  checkpoint: Checkpoint | null = null;
  servers = new Map<string, ExistingServer & { last_seen: string }>();
  snapshots: SnapshotRow[] = [];
  logs: (IngestionResult & { completedAt: number })[] = [];
  scanRuns: { id: string; status: string | null }[] = [];
  upserted: string[] = [];
  failUpsertAfter: number | null = null; // fail every upsert call after this many
  private upsertCalls = 0;

  async loadCheckpoint(): Promise<Checkpoint | null> { return this.checkpoint ? { ...this.checkpoint } : null; }
  async saveCheckpoint(cp: Checkpoint): Promise<void> {
    assert.ok(cp.phase === 'backfill' || cp.phase === 'delta', 'ingest_checkpoints_phase_check');
    if (cp.phase === 'delta') {
      assert.ok(cp.updated_since_watermark && cp.last_full_pass_at && cp.cursor === null, 'ingest_checkpoints_delta_anchored');
    }
    this.checkpoint = { ...cp };
  }
  async existingServers(names: string[]): Promise<ExistingServer[]> {
    return names.flatMap((n) => { const s = this.servers.get(n); return s ? [{ ...s }] : []; });
  }
  async upsertServers(rows: ServerRow[]): Promise<{ name: string; id: string }[]> {
    this.upsertCalls++;
    if (this.failUpsertAfter !== null && this.upsertCalls > this.failUpsertAfter) throw new Error('simulated write failure');
    return rows.map((r) => {
      const id = this.servers.get(r.name)?.id ?? `id-${this.servers.size + 1}`;
      this.servers.set(r.name, {
        name: r.name, id, version: r.version, status: r.status, definition_hash: r.definition_hash,
        status_hash: r.status_hash, status_message_hash: r.status_message_hash, last_seen: r.last_seen,
      });
      this.upserted.push(r.name);
      return { name: r.name, id };
    });
  }
  async insertSnapshots(rows: SnapshotRow[]): Promise<number> { this.snapshots.push(...rows); return rows.length; }
  async logIngestion(r: IngestionResult): Promise<void> { this.logs.push({ ...r, completedAt: Date.now() }); }
  async openScanRun(): Promise<string | null> {
    const id = `scan-${this.scanRuns.length + 1}`;
    this.scanRuns.push({ id, status: null });
    return id;
  }
  async closeScanRun(id: string | null, f: ScanRunClose): Promise<void> {
    const s = this.scanRuns.find((x) => x.id === id);
    if (s) s.status = f.status;
  }
  /** The watchdog's gap: newest started_at over every row. */
  newestLogAt(): number { return Math.max(...this.logs.map((l) => l.startedAt.getTime())); }
  /** The watchdog's failure streak: newest-first rows carrying an error. */
  failureStreak(): number {
    const rows = [...this.logs].sort((a, b) => b.startedAt.getTime() - a.startedAt.getTime() || b.completedAt - a.completedAt);
    let n = 0;
    for (const r of rows) { if (!r.errorMessage) break; n++; }
    return n;
  }
}

const stubCaches = async (): Promise<CacheRefreshBundle> => {
  const ok = { status: 'SUCCEEDED' as const, error: null, errorClass: null, durationMs: 1, startedAt: new Date().toISOString(), finishedAt: new Date().toISOString() };
  return { dashboard: ok, reachability: ok, health: null };
};

function run(store: MemoryStore, baseUrl: string, over: Partial<RunOptions> = {}): Promise<RunResult> {
  return ingestMCPRegistry({
    store, baseUrl, timeoutMs: 2_000, budgetMs: 60_000, segmentPages: 3,
    interPageDelayMs: 0, lookbackMs: LOOKBACK_MS, refreshCaches: stubCaches, ...over,
  });
}

/** Capture worker log lines for one call; the worker is chatty and the checks read its sleep source. */
async function captured<T>(fn: () => Promise<T>): Promise<{ value: T; lines: string[] }> {
  const lines: string[] = [];
  const orig = { log: console.log, warn: console.warn, error: console.error };
  const grab = (...args: unknown[]) => { lines.push(args.map(String).join(' ')); };
  console.log = grab; console.warn = grab; console.error = grab;
  try { return { value: await fn(), lines }; } finally { Object.assign(console, orig); }
}

async function main(): Promise<void> {
  // ── 0. planFlush in isolation ─────────────────────────────────────────────
  {
    const item = (name: string, version: string) => toItem({ name, version, status: 'active', updatedAt: OLD, isLatest: false, description: '' });
    const pending = [
      { requestCursor: null, items: [item('a', '1.0.0'), item('b', '1.0.0')] },
      { requestCursor: 'k1', items: [item('b', '2.0.0'), item('c', '1.0.0'), item('c', '1.1.0')] },
      { requestCursor: 'k2', items: [item('c', '2.0.0')] },
    ];
    const seg = planFlush(pending, 'k3', 'segment');
    assert.deepEqual(seg.commit.map((i) => i.server.name), ['a', 'b', 'b'], 'the trailing name is held back');
    assert.equal(seg.carry[0].items.length, 3, 'all three versions of the trailing name are carried');
    assert.equal(seg.resumeCursor, 'k1', 'the resume point rewinds to the first page holding it');
    const stop = planFlush(pending, 'k3', 'stop');
    assert.equal(stop.carry.length, 0, 'a stop leaves the trailing name for the next run');
    assert.equal(stop.resumeCursor, 'k1');
    const done = planFlush(pending, null, 'complete');
    assert.equal(done.commit.length, 6); assert.equal(done.resumeCursor, null);
    const one = planFlush([{ requestCursor: 'k0', items: [item('z', '1.0.0')] }], 'k1', 'stop');
    assert.equal(one.commit.length, 1, 'one name filling the batch is committed, never held forever');
    assert.equal(one.resumeCursor, 'k1');
    console.log('  [0/14] planFlush holds the trailing name back and rewinds the resume point');
  }

  // ── 1-6: one corpus through backfill → re-armed backfill → manual delta ────
  const entries = corpus(200);
  const reg = new FakeRegistry(entries);
  const base = await reg.start();
  const store = new MemoryStore();
  const totalPages = Math.ceil(entries.length / reg.pageSize);

  // 1. Backfill stops at its budget, exits 0, persists the cursor, logs without error.
  reg.delayMs = 40;
  const t1 = Date.now();
  const r1 = await captured(() => run(store, base, { budgetMs: 400 })).then((c) => c.value);
  assert.equal(r1.phase_before, 'backfill');
  assert.equal(r1.stop, 'budget', 'the walk stopped at its budget');
  assert.equal(exitCodeForRun(r1), 0, 'a budget stop is a committed checkpoint: exit 0');
  assert.ok(r1.pages > 0 && r1.pages < totalPages, `stopped part-way (${r1.pages}/${totalPages} pages)`);
  assert.equal(r1.phase_after, 'backfill');
  assert.ok(store.checkpoint?.cursor, 'the cursor is persisted');
  assert.ok(store.servers.size > 0, 'fetched servers are committed, not held in memory');
  assert.ok(store.logs.length >= 2, 'a checkpoint row per committed batch plus the run row');
  assert.ok(store.logs.every((l) => !l.errorMessage), 'a budget stop logs no error');
  assert.ok(store.newestLogAt() >= t1, 'ingestion_log advances during the run — the watchdog gap resets');
  assert.equal(r1.ingest_status, 'PARTIAL', 'a checkpoint is a slice, not a census');
  assert.equal(r1.dashboard_refresh_status, 'SKIPPED', 'a slice is never published as the census');
  assert.match(store.scanRuns[0].status ?? '', /^checkpoint phase=backfill->backfill stop=budget/);
  console.log(`  [1/14] backfill stopped at budget after ${r1.pages}/${totalPages} pages, exit 0, cursor persisted, ${store.logs.length} log rows, no error`);

  // 2. The next run resumes from the persisted cursor.
  const cursor1 = store.checkpoint!.cursor;
  const before2 = reg.requests.length;
  const r2 = await captured(() => run(store, base, { budgetMs: 400 })).then((c) => c.value);
  const first2 = reg.since(before2)[0];
  assert.equal(first2.searchParams.get('cursor'), cursor1, 'the first request carries the persisted cursor');
  assert.equal(r2.phase_before, 'backfill');
  console.log(`  [2/14] second run resumed from cursor ${cursor1?.replace('\u0000', '@')} (not the first page)`);

  // 3. Run to the end: phase RE-ARMS to 'backfill' (does not flip to delta),
  //    every server lands once at its isLatest version.
  reg.delayMs = 0;
  let r3: RunResult;
  let guard = 0;
  do { r3 = await captured(() => run(store, base)).then((c) => c.value); } while (r3.stop !== 'complete' && ++guard < 5);
  assert.equal(r3.stop, 'complete');
  assert.equal(r3.phase_after, 'backfill', 'reaching the end of the corpus re-arms for another full walk — it does not flip to delta');
  assert.ok(store.checkpoint!.last_full_pass_at, 'last_full_pass_at stamped');
  assert.equal(store.checkpoint!.cursor, null);
  assert.equal(store.checkpoint!.phase, 'backfill');
  assert.equal(store.checkpoint!.updated_since_watermark, new Date(Date.parse(r3.run_started_at) - LOOKBACK_MS).toISOString(),
    'the watermark refreshes to THIS pass\'s own anchor, not a stale one from wherever the row started');
  const latest = latestOf(entries);
  assert.equal(store.servers.size, latest.size, 'every server landed');
  for (const [name, e] of latest) assert.equal(store.servers.get(name)?.version, e.version, `${name} stored at its isLatest version`);
  assert.equal(store.snapshots.length, latest.size, 'exactly one snapshot per server — no interim versions from a split name');
  assert.equal(r3.dashboard_refresh_status, 'SUCCEEDED', 'caches refresh after a complete pass');
  console.log(`  [3/14] backfill completed: phase stays backfill (re-armed), ${store.servers.size} servers at isLatest, ${store.snapshots.length} snapshots`);

  // 4. last_seen keeps advancing on a SECOND consecutive full pass — the
  //    "confirmed alive today" guarantee this design exists to protect.
  const watchedServer = 'io.example/s0001';
  const lastSeenAfterPass1 = store.servers.get(watchedServer)!.last_seen;
  await sleep(5); // guarantee a distinct timestamp from the next pass
  const r4 = await captured(() => run(store, base)).then((c) => c.value);
  assert.equal(r4.phase_before, 'backfill', 'still backfill going in — the steady state held after pass 1');
  assert.equal(r4.stop, 'complete');
  assert.equal(r4.phase_after, 'backfill', 'and still backfill coming out — a second consecutive full pass, not a one-time reversion');
  const lastSeenAfterPass2 = store.servers.get(watchedServer)!.last_seen;
  assert.notEqual(lastSeenAfterPass2, lastSeenAfterPass1, `${watchedServer}.last_seen must advance even though nothing about it changed`);
  assert.ok(Date.parse(lastSeenAfterPass2) > Date.parse(lastSeenAfterPass1), 'and move strictly forward');
  console.log(`  [4/14] last_seen advanced on a second, unchanged full pass (confirmed-alive-today holds under repeated full walks)`);

  // 5. Delta stays a working code path when a checkpoint is manually put into
  //    it (an operator override) — proves it stays reachable even though
  //    nothing in normal daily operation enters it automatically.
  const watermark5 = new Date().toISOString();
  store.checkpoint = { source: 'mcp-registry', phase: 'delta', cursor: null, updated_since_watermark: watermark5, last_full_pass_at: store.checkpoint!.last_full_pass_at };
  const now = new Date().toISOString();
  const bump = entries.find((e) => e.name === 'io.example/s0003')!;          // new version published
  bump.isLatest = false; bump.updatedAt = now;
  entries.push({ ...bump, version: '1.1.0', isLatest: true, updatedAt: now });
  const del = entries.find((e) => e.name === 'io.example/s0010')!;           // deleted
  del.status = 'deleted'; del.updatedAt = now;
  const desc = entries.find((e) => e.name === 'io.example/s0020')!;          // description only
  desc.description = 'edited'; desc.updatedAt = now;
  const upBefore = store.upserted.length, snapBefore = store.snapshots.length, before5 = reg.requests.length;
  const t5 = Date.now();
  const r5 = await captured(() => run(store, base)).then((c) => c.value);
  const elapsed5 = Date.now() - t5;
  const first5 = reg.since(before5)[0];
  assert.equal(r5.phase_before, 'delta');
  assert.equal(first5.searchParams.get('updated_since'), watermark5, 'delta reads since the watermark');
  assert.equal(first5.searchParams.get('include_deleted'), 'true');
  assert.equal(r5.stop, 'complete');
  assert.equal(r5.phase_after, 'delta', 'a completed delta run stays in delta — only an operator (or the oversized-budget fallback) ever leaves it, and normal operation never enters it to begin with');
  assert.deepEqual(new Set(store.upserted.slice(upBefore)), new Set(['io.example/s0003', 'io.example/s0010', 'io.example/s0020']),
    'only the changed servers are upserted');
  assert.equal(store.servers.get('io.example/s0003')?.version, '1.1.0');
  assert.equal(store.servers.get('io.example/s0010')?.status, 'deleted', 'the deletion is stored');
  assert.equal(r5.status_transitions['active->deleted'], 1, 'and counted');
  const newSnaps = store.snapshots.slice(snapBefore);
  assert.equal(newSnaps.length, 2, 'snapshots for the version bump and the deletion; a description edit is not a definition change');
  assert.ok(newSnaps.some((s) => s.status === 'deleted'));
  assert.equal(r5.watermark_after, new Date(Date.parse(r5.run_started_at) - LOOKBACK_MS).toISOString(), 'watermark = run start - lookback');
  assert.ok(elapsed5 < 60_000, `delta completed in ${elapsed5}ms`);
  console.log(`  [5/14] delta stays reachable via a manually-entered checkpoint: updated_since honoured, 3 changed servers upserted, active->deleted recorded, watermark advanced (${elapsed5}ms)`);

  // 6. Re-running the same watermark writes zero new rows (idempotent upsert).
  store.checkpoint = { ...store.checkpoint!, updated_since_watermark: watermark5 };
  const sizeBefore = store.servers.size, snaps6 = store.snapshots.length;
  const r6 = await captured(() => run(store, base)).then((c) => c.value);
  assert.equal(store.servers.size, sizeBefore, 'no new server rows');
  assert.equal(store.snapshots.length, snaps6, 'no new snapshots');
  assert.equal(r6.snapshots, 0);
  assert.equal(r6.upserted, 3, 'the same three rows were re-upserted in place');
  console.log('  [6/14] same watermark re-run: 0 new server rows, 0 new snapshots (idempotent)');
  await reg.stop();

  // 7. Retry-After honoured, sleep source logged.
  {
    const r = new FakeRegistry(corpus(30)); const url = await r.start(); const s = new MemoryStore();
    r.hook = (n) => (n === 2 ? { status: 503, retryAfter: '1' } : null);
    const t = Date.now();
    const { value, lines } = await captured(() => run(s, url));
    const ms = Date.now() - t;
    await r.stop();
    assert.equal(value.stop, 'complete');
    assert.equal(value.retries_5xx, 1);
    assert.ok(ms >= 900 && ms < 4_500, `Retry-After: 1 honoured over the 5s schedule (${ms}ms)`);
    assert.ok(lines.some((l) => /sleep_source=retry-after \(Retry-After honoured\)/.test(l)), 'the sleep source is logged');
    console.log(`  [7/14] 503 Retry-After: 1 honoured (${ms}ms), logged sleep_source=retry-after`);
  }

  // 8. Mid-walk 500 past the ladder: commit, checkpoint, exit 0, resume to
  //    completion — still re-arming to 'backfill', not flipping to delta.
  {
    const e = corpus(100); const r = new FakeRegistry(e); const url = await r.start(); const s = new MemoryStore();
    r.hook = (n) => (n >= 6 ? { status: 500, retryAfter: '0' } : null);
    const { value } = await captured(() => run(s, url, { segmentPages: 2 }));
    assert.equal(value.stop, 'registry');
    assert.equal(exitCodeForRun(value), 0, 'a checkpointed registry stop does not fail the run');
    assert.equal(value.retries_5xx, 3, 'the full ladder was walked (Retry-After: 0 honoured)');
    assert.equal(value.pages, 5);
    assert.ok(s.servers.size > 0, 'pages fetched before the 500 are committed, not discarded');
    assert.ok(s.checkpoint?.cursor, 'resume point persisted');
    assert.ok(s.logs.every((l) => !l.errorMessage), 'progress was made: not a watchdog failure');
    r.hook = null;
    const before = r.requests.length;
    const { value: next } = await captured(() => run(s, url, { segmentPages: 2 }));
    assert.equal(r.since(before)[0].searchParams.get('cursor'), value.cursor_after, 'the resumed run\'s first request carries the checkpoint');
    assert.equal(next.stop, 'complete', 'the resume completes the corpus in one call');
    assert.equal(next.phase_after, 'backfill', 'completing after a mid-walk failure still re-arms to backfill, not delta');
    assert.equal(s.servers.size, latestOf(e).size, 'and completes the corpus');
    await r.stop();
    console.log(`  [8/14] mid-walk 500 after ${value.pages} pages: committed ${value.fetched} items, checkpointed, exit 0; resumed run completed`);
  }

  // 9. Retry-After past the budget: checkpoint instead of retrying early.
  {
    const r = new FakeRegistry(corpus(50)); const url = await r.start(); const s = new MemoryStore();
    r.hook = (n) => (n === 2 ? { status: 503, retryAfter: '30' } : null);
    const t = Date.now();
    const { value, lines } = await captured(() => run(s, url, { budgetMs: 2_000 }));
    const ms = Date.now() - t;
    await r.stop();
    assert.equal(value.stop, 'registry');
    assert.ok(ms < 5_000, `did not sleep 30s (${ms}ms)`);
    assert.equal(r.requests.length, 2, 'the page was NOT retried early');
    assert.ok(lines.some((l) => /wait_ms=30000 sleep_source=retry-after would pass the run budget/.test(l)));
    assert.equal(exitCodeForRun(value), 0, 'page 1 committed: exit 0');
    console.log(`  [9/14] Retry-After: 30 > budget: no early retry, checkpointed in ${ms}ms`);
  }

  // 10. Registry answers no page: exit 1, logged as a failure.
  {
    const r = new FakeRegistry(corpus(20)); const url = await r.start(); const s = new MemoryStore();
    r.hook = () => ({ status: 500, retryAfter: '0' });
    const { value } = await captured(() => run(s, url));
    await r.stop();
    assert.equal(value.stop, 'registry');
    assert.equal(value.pages, 0);
    assert.equal(exitCodeForRun(value), 1, 'nothing landed: exit 1');
    assert.match(s.logs.at(-1)?.errorMessage ?? '', /no page read this run/);
    assert.equal(s.failureStreak(), 1, 'the watchdog counts it');
    console.log('  [10/14] registry unavailable from page 1: exit 1, error logged for the watchdog');
  }

  // 11. A name straddling a batch boundary is written once at isLatest.
  {
    const e: Entry[] = [];
    for (let i = 0; i < 18; i++) e.push({ name: `io.example/a${String(i).padStart(2, '0')}`, version: '1.0.0', status: 'active', updatedAt: OLD, isLatest: true, description: '' });
    ['1.0.0', '2.0.0', '3.0.0', '4.0.0'].forEach((v, j) => e.push({ name: 'io.example/m-split', version: v, status: 'active', updatedAt: OLD, isLatest: j === 3, description: '' }));
    for (let i = 0; i < 7; i++) e.push({ name: `io.example/z${i}`, version: '1.0.0', status: 'active', updatedAt: OLD, isLatest: true, description: '' });
    const r = new FakeRegistry(e); const url = await r.start(); const s = new MemoryStore();
    const { value } = await captured(() => run(s, url, { segmentPages: 2 }));
    await r.stop();
    assert.equal(value.phase_after, 'backfill');
    assert.equal(s.servers.get('io.example/m-split')?.version, '4.0.0');
    assert.equal(s.snapshots.filter((x) => x.version !== null && s.servers.get('io.example/m-split')?.id === x.server_id).length, 1,
      'one snapshot for the split server — no interim 2.0.0');
    assert.equal(s.upserted.filter((n) => n === 'io.example/m-split').length, 1, 'written once');
    console.log('  [11/14] versions straddling a batch boundary: written once at 4.0.0, one snapshot');
  }

  // 12. A delta too large for its budget falls back to a fresh backfill.
  {
    const e = corpus(80); const r = new FakeRegistry(e); const url = await r.start(); const s = new MemoryStore();
    s.checkpoint = { source: 'mcp-registry', cursor: null, phase: 'delta', updated_since_watermark: '2026-01-01T00:00:00.000Z', last_full_pass_at: OLD };
    r.delayMs = 40;
    const { value } = await captured(() => run(s, url, { budgetMs: 150 }));
    await r.stop();
    assert.equal(value.stop, 'budget');
    assert.equal(value.phase_after, 'backfill');
    assert.equal(s.checkpoint?.cursor, null);
    assert.equal(s.checkpoint?.updated_since_watermark, new Date(Date.parse(value.run_started_at) - LOOKBACK_MS).toISOString());
    console.log('  [12/14] oversized delta: fell back to a fresh backfill anchored at this run');
  }

  // 13. A write failure never advances the cursor past the failed rows.
  {
    const r = new FakeRegistry(corpus(60)); const url = await r.start(); const s = new MemoryStore();
    s.failUpsertAfter = 1; // the first batch lands, every later write fails
    const { value } = await captured(() => run(s, url, { segmentPages: 2 }));
    assert.equal(value.stop, 'store');
    assert.equal(exitCodeForRun(value), 1);
    const cursorAfterFirst = s.checkpoint?.cursor;
    assert.ok(cursorAfterFirst, 'the first batch checkpointed');
    s.failUpsertAfter = null;
    const before = r.requests.length;
    await captured(() => run(s, url, { segmentPages: 2 }));
    await r.stop();
    assert.equal(r.since(before)[0].searchParams.get('cursor'), cursorAfterFirst, 'the failed batch is re-read, not skipped');
    console.log('  [13/14] write failure: exit 1, cursor held at the last committed batch, re-read next run');
  }

  // 14. Duration-warning predicate: fires at/above 60% of the job's 30-minute
  //     budget (18 min), not below it — and is wired into RunResult and the
  //     ingestion_log row. Tested directly against the pure predicate for the
  //     boundary (an 18-minute real run is not a reasonable price for this
  //     check), and end to end via a test-only threshold override for wiring.
  {
    assert.equal(DURATION_WARNING_MS, 18 * 60_000, 'the exported constant matches 60% of the 30-minute job budget');
    assert.equal(durationWarningFires(18 * 60_000), true, 'fires exactly at the threshold');
    assert.equal(durationWarningFires(18 * 60_000 + 1), true, 'fires above it');
    assert.equal(durationWarningFires(18 * 60_000 - 1), false, 'does not fire just under it');
    assert.equal(durationWarningFires(5 * 60_000), false, 'nowhere near it');

    const r = new FakeRegistry(corpus(10)); const url = await r.start(); const s = new MemoryStore();
    const { value } = await captured(() => run(s, url, { durationWarningThresholdMs: 0 }));
    await r.stop();
    assert.equal(value.duration_warning, true, 'wired into RunResult when the run exceeds the (test-lowered) threshold');
    const runRow = s.logs.find((l) => (l.metadata as Record<string, unknown> | undefined)?.kind === 'run');
    assert.equal((runRow?.metadata as Record<string, unknown> | undefined)?.duration_warning, true, 'and into the ingestion_log row');

    const r2 = new FakeRegistry(corpus(10)); const url2 = await r2.start(); const s2 = new MemoryStore();
    const { value: value2 } = await captured(() => run(s2, url2, { durationWarningThresholdMs: 60 * 60_000 }));
    await r2.stop();
    assert.equal(value2.duration_warning, false, 'does not fire well under threshold');

    console.log('  [14/14] duration warning fires at/above 60% of the 30-minute job budget (pure predicate + wired into RunResult and ingestion_log), not below it');
  }

  console.log('MCP registry checkpoint contract: passed');
}

main().catch((e) => { console.error(e); process.exit(1); });
