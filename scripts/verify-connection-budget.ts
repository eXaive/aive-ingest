/**
 * Connection budget contract (lib/ingest/db.ts, 2026-11-06; MCP audit 2026-11-05).
 *
 * SYNTHETIC AND ISOLATED: real pg Pools, but their Client is a fake that enforces a shared
 * ROLE CONNECTION LIMIT of 4 exactly like Postgres does (SQLSTATE 53300 at connect). No database,
 * no network, no credentials.
 *
 * PROVES
 *   1. poolMaxFrom: default 2; overrides 1-4 honoured; anything else falls back to 2.
 *   2. Two workers at the default pool size never exceed the role limit under heavy load
 *      (the overlap that cost 12 tool captures on 2026-11-05 cannot happen).
 *   3. The OLD configuration (max 4 per worker) does hit 53300 under the same load: the
 *      synthetic harness reproduces the production failure, so (2) is not vacuous.
 *   4. withConnectionLimitRetry retries ONLY connection-limit refusals, on the documented
 *      schedule, then rethrows; other errors are never retried.
 *   5. A retried write executes exactly once: 53300 happens before any statement, so retries
 *      cannot duplicate evidence.
 *
 * Run: npx tsx scripts/verify-connection-budget.ts
 */
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { Pool } from 'pg';
import {
  poolMaxFrom, DEFAULT_POOL_MAX, ROLE_CONNECTION_LIMIT, CONNECTION_LIMIT_RETRY_MS,
  isConnectionLimitError, withConnectionLimitRetry,
} from '../lib/ingest/db';

// A fake "server" shared by every pool: at most ROLE_CONNECTION_LIMIT open connections.
const server = { open: 0, peak: 0, refused: 0, executed: new Map<string, number>() };
class FakeClient extends EventEmitter {
  private connected = false;
  connect(cb?: (err?: Error) => void) {
    const p = new Promise<void>((resolve, reject) => setImmediate(() => {
      if (server.open >= ROLE_CONNECTION_LIMIT) {
        server.refused++;
        return reject(Object.assign(new Error('too many connections for role "aive_ingest"'), { code: '53300' }));
      }
      server.open++; server.peak = Math.max(server.peak, server.open); this.connected = true; resolve();
    }));
    if (cb) { p.then(() => cb(), cb); return undefined; }
    return p;
  }
  query(text: string | { text: string }, values?: unknown, cb?: (err: Error | null, res?: unknown) => void) {
    const sql = typeof text === 'string' ? text : text.text;
    const callback = typeof values === 'function' ? values as typeof cb : cb;
    const p = new Promise((resolve) => setTimeout(() => {
      server.executed.set(sql, (server.executed.get(sql) ?? 0) + 1);
      resolve({ rows: [{ ok: 1 }], rowCount: 1 });
    }, 2));
    if (callback) { p.then((r) => callback(null, r)); return undefined; }
    return p;
  }
  end(cb?: () => void) { if (this.connected) { server.open--; this.connected = false; } const p = Promise.resolve(); if (cb) { p.then(cb); return undefined; } return p; }
  release() { /* pool-managed */ }
}
const makePool = (max: number) => new Pool({ max, Client: FakeClient as never, idleTimeoutMillis: 1, connectionTimeoutMillis: 60_000 });
const reset = () => { server.open = 0; server.peak = 0; server.refused = 0; server.executed.clear(); };
const load = (pool: Pool, tag: string, n: number) =>
  Array.from({ length: n }, (_, i) => pool.query(`INSERT ${tag}-${i}`).then(() => 'ok', (e) => (isConnectionLimitError(e) ? '53300' : 'other')));

async function main() {
  // 1. Pool size parsing.
  assert.equal(DEFAULT_POOL_MAX, 2); assert.equal(ROLE_CONNECTION_LIMIT, 4);
  assert.equal(poolMaxFrom(undefined), 2); assert.equal(poolMaxFrom(''), 2);
  for (const ok of ['1', '2', '3', '4']) assert.equal(poolMaxFrom(ok), Number(ok));
  const warn = console.warn; console.warn = () => {};
  for (const bad of ['0', '5', '2.5', 'four', '-1']) assert.equal(poolMaxFrom(bad), 2, `fallback for ${bad}`);
  console.warn = warn;
  assert.ok(2 * DEFAULT_POOL_MAX <= ROLE_CONNECTION_LIMIT, 'two concurrent workers fit inside the role limit');
  console.log('  [1] pool size: default 2, overrides 1-4, invalid values fall back; 2 workers x 2 <= role limit 4');

  // 2. New default: two workers, 200 concurrent writes each, never exceed the limit.
  reset();
  {
    const a = makePool(poolMaxFrom(undefined)), b = makePool(poolMaxFrom(undefined));
    const results = await Promise.all([...load(a, 'probe', 200), ...load(b, 'tools', 200)]);
    await a.end(); await b.end();
    assert.equal(results.filter((r) => r !== 'ok').length, 0, 'every write succeeded');
    assert.equal(server.refused, 0, 'no connection was refused');
    assert.ok(server.peak <= ROLE_CONNECTION_LIMIT, `peak ${server.peak} <= ${ROLE_CONNECTION_LIMIT}`);
    console.log(`  [2] default budget: 400 concurrent writes from 2 workers, 0 refusals, peak ${server.peak} connections`);
  }

  // 3. Old configuration reproduces the 2026-11-05 failure.
  reset();
  {
    const a = makePool(4), b = makePool(4);
    const results = await Promise.all([...load(a, 'probe', 200), ...load(b, 'tools', 200)]);
    await a.end(); await b.end();
    const refused = results.filter((r) => r === '53300').length;
    assert.ok(refused > 0, 'max:4 per worker hits the role limit under overlap (the production failure)');
    console.log(`  [3] old budget (max 4 per worker): ${refused} of 400 writes refused with 53300, reproducing 2026-11-05`);
  }

  // 4. Retry policy.
  {
    const sleeps: number[] = [];
    const sleep = async (ms: number) => { sleeps.push(ms); };
    const limit = () => Object.assign(new Error('too many connections for role "aive_ingest"'), { code: '53300' });
    let calls = 0;
    const v = await withConnectionLimitRetry(async () => { if (++calls < 3) throw limit(); return 'done'; }, 't', sleep);
    assert.equal(v, 'done'); assert.equal(calls, 3); assert.deepEqual(sleeps, [5_000, 15_000]);
    sleeps.length = 0; calls = 0;
    const w = console.warn; console.warn = () => {};
    await assert.rejects(withConnectionLimitRetry(async () => { calls++; throw limit(); }, 't', sleep), /too many connections/);
    console.warn = w;
    assert.equal(calls, CONNECTION_LIMIT_RETRY_MS.length + 1); assert.deepEqual(sleeps, [...CONNECTION_LIMIT_RETRY_MS]);
    sleeps.length = 0; calls = 0;
    await assert.rejects(withConnectionLimitRetry(async () => { calls++; throw Object.assign(new Error('duplicate key'), { code: '23505' }); }, 't', sleep), /duplicate key/);
    assert.equal(calls, 1, 'non-connection errors are never retried'); assert.equal(sleeps.length, 0);
    assert.equal(isConnectionLimitError({ message: 'FATAL: remaining connection slots are reserved for roles with the SUPERUSER attribute' }), true);
    assert.equal(isConnectionLimitError({ code: '57014', message: 'canceling statement due to statement timeout' }), false);
    console.log('  [4] retry: only 53300, schedule 5/15/45/120 s, then rethrow; other errors once');
  }

  // 5. Retries never duplicate evidence. Old config (8 contenders for 4 slots) so refusals really happen:
  //    a write that eventually succeeds executed exactly once; a write that exhausted its retries executed zero times.
  reset();
  {
    const a = makePool(4), b = makePool(4);
    const fast = async () => { await new Promise((r) => setTimeout(r, 5)); };
    const w = console.warn; console.warn = () => {};
    const wrap = (pool: Pool, tag: string, n: number) => Array.from({ length: n }, (_, i) =>
      withConnectionLimitRetry(() => pool.query(`INSERT ${tag}-${i}`), 'w', fast).then(() => [`INSERT ${tag}-${i}`, 'ok'] as const, () => [`INSERT ${tag}-${i}`, 'failed'] as const));
    const results = await Promise.all([...wrap(a, 'probe', 150), ...wrap(b, 'tools', 150)]);
    console.warn = w;
    await a.end(); await b.end();
    assert.ok(server.refused > 0, 'refusals happened and were retried');
    for (const [sql, outcome] of results) {
      assert.equal(server.executed.get(sql) ?? 0, outcome === 'ok' ? 1 : 0, `${sql}: executed ${outcome === 'ok' ? 'once' : 'never'}`);
    }
    const failed = results.filter(([, o]) => o === 'failed').length;
    console.log(`  [5] ${server.refused} refusals retried; ${300 - failed} writes executed exactly once, ${failed} exhausted retries and executed zero times (no duplicates either way)`);
  }

  // 6. KNOWN LIMIT, measured rather than hidden: THREE overlapping jobs at the new default want 6 of 4 slots. Pools
  //    keep busy connections, so under SUSTAINED load the third job can starve past its retry window. Its writes then
  //    fail visibly (the worker counts them and ends PARTIAL; watchdog check 7 reports it). They are never duplicated
  //    and never silently lost. Closing this needs scheduling coordination or the transaction-mode pooler (P1, owner).
  reset();
  {
    const pools = [makePool(DEFAULT_POOL_MAX), makePool(DEFAULT_POOL_MAX), makePool(DEFAULT_POOL_MAX)];
    const backoff = async () => { await new Promise((r) => setTimeout(r, 25)); };
    const w = console.warn; console.warn = () => {};
    const results = await Promise.all(pools.flatMap((p, k) => Array.from({ length: 60 }, (_, i) =>
      withConnectionLimitRetry(() => p.query(`INSERT job${k}-${i}`), 'w', backoff).then(() => [`INSERT job${k}-${i}`, 'ok'] as const, () => [`INSERT job${k}-${i}`, 'failed'] as const))));
    console.warn = w;
    for (const p of pools) await p.end();
    for (const [sql, outcome] of results) assert.equal(server.executed.get(sql) ?? 0, outcome === 'ok' ? 1 : 0, `${sql}: once or never`);
    assert.ok(server.peak <= ROLE_CONNECTION_LIMIT, 'the role limit itself is never exceeded');
    const failed = results.filter(([, o]) => o === 'failed').length;
    console.log(`  [6] KNOWN LIMIT: 3 overlapping jobs under sustained load: ${180 - failed}/180 landed once, ${failed} failed visibly after retries, 0 duplicates, peak ${server.peak}`);
  }

  console.log('Connection budget contract: passed');
}

main().catch((e) => { console.error(e); process.exit(1); });
