/**
 * Protocol watch contract (Protocol Intelligence, Phase 2). Offline: a fake fetch, a loopback HTTP fixture and the
 * in-memory store. No database, no internet. Run: npx tsx scripts/verify-protocol-watch.ts
 */
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { join } from 'node:path';
import { automatedSources, catalogueVersionOf, loadCatalogue, parseCatalogue, type WatchCatalogue } from '../lib/protocolWatch/catalogue';
import { WatchHttp, WatchHttpError, GITHUB_RATE_FLOOR, RETRY_AFTER_CAP_MS } from '../lib/protocolWatch/http';
import { manualRunKey, runWatch, scheduleSlotKey } from '../lib/protocolWatch/engine';
import { MemoryWatchStore } from '../lib/protocolWatch/store';
import { setSsrfTestPolicy } from '../lib/net/ssrfGuard';

const root = join(__dirname, '..');
let passed = 0;
const ok = (name: string) => { passed++; console.log(`  ✓ ${name}`); };
// Public addresses for the two allowlisted hosts; no real DNS is used.
setSsrfTestPolicy({ lookup: async (h) => (h === 'api.github.com' ? ['140.82.112.6'] : h === 'datatracker.ietf.org' ? ['104.16.44.99'] : ['93.184.216.34']) });

type Fx = { status?: number; body?: unknown; headers?: Record<string, string> };
function fakeFetch(route: (url: URL) => Fx | Fx[] | undefined) {
  const calls: string[] = [];
  const seen = new Map<string, number>();
  const impl = (async (input: string) => {
    const url = new URL(input); calls.push(url.href);
    let fx = route(url);
    if (Array.isArray(fx)) { const n = seen.get(url.href) ?? 0; seen.set(url.href, n + 1); fx = fx[Math.min(n, fx.length - 1)]; }
    if (!fx) return new Response('not found', { status: 404 });
    return new Response(fx.body === undefined ? '' : JSON.stringify(fx.body), { status: fx.status ?? 200, headers: { 'content-type': 'application/json', 'x-ratelimit-remaining': '4000', ...(fx.headers ?? {}) } });
  }) as unknown as typeof fetch;
  return { impl, calls };
}
const noSleep = { sleeps: [] as number[], sleep: async (ms: number) => { noSleep.sleeps.push(ms); } };

// A small catalogue: two repos, one draft, plus blocked entries that must never be fetched.
function miniCatalogue(): WatchCatalogue {
  const sources = [
    { sourceId: 'github:acme/spec', protocolId: 'x402', method: 'GITHUB_REPOSITORY', target: 'acme/spec', gate: 'ALLOWED', freshnessHours: 48 },
    { sourceId: 'github:acme/other', protocolId: 'ucp', method: 'GITHUB_REPOSITORY', target: 'acme/other', gate: 'ALLOWED', freshnessHours: 48 },
    { sourceId: 'ietf:draft-acme-thing', protocolId: 'slim', method: 'IETF_DRAFT', target: 'draft-acme-thing', gate: 'ALLOWED', freshnessHours: 168 },
    { sourceId: 'registry:blocked', protocolId: 'a2a', method: 'REGISTRY_API', target: 'https://registry.example/api', gate: 'BLOCKED_TERMS_NOT_REVIEWED', freshnessHours: 48 },
    { sourceId: 'registry:phase3', protocolId: 'x402', method: 'REGISTRY_API', target: 'https://bazaar.example/discovery', gate: 'BLOCKED_PHASE3', freshnessHours: 48 },
  ] as WatchCatalogue['sources'];
  const body = { schema: 'aive.protocol-watch-sources.v1' as const, reviewedOn: '2026-11-06', sources };
  return parseCatalogue({ ...body, version: catalogueVersionOf(body) });
}

let world = {
  spec: { archived: false, fullName: 'acme/spec', releases: [{ id: 2, tag_name: 'v1.1.0', name: 'v1.1.0', published_at: '2026-10-01T00:00:00Z', html_url: 'https://github.com/acme/spec/releases/tag/v1.1.0' }, { id: 1, tag_name: 'v1.0.0', name: 'v1.0.0', published_at: '2026-09-01T00:00:00Z', html_url: 'https://github.com/acme/spec/releases/tag/v1.0.0' }], tags: ['v1.1.0', 'v1.0.0'] },
  other: { status: 200 },
  draftRev: '03',
};
function route(url: URL): Fx | Fx[] | undefined {
  const p = url.pathname;
  if (url.hostname === 'api.github.com') {
    if (p === '/repos/acme/spec') return { body: { full_name: world.spec.fullName, archived: world.spec.archived, disabled: false, default_branch: 'main', license: { spdx_id: 'Apache-2.0' }, stargazers_count: Math.random() } };
    if (p === '/repos/acme/spec/releases') return { body: world.spec.releases };
    if (p === '/repos/acme/spec/tags') return { body: world.spec.tags.map((name) => ({ name })) };
    if (p === '/repos/acme/other') return world.other.status === 200 ? { body: { full_name: 'acme/other', archived: false, default_branch: 'main', license: null } } : { status: world.other.status, body: { message: 'x' } };
    if (p === '/repos/acme/other/releases' || p === '/repos/acme/other/tags') return { body: [] };
  }
  if (url.hostname === 'datatracker.ietf.org' && p === '/api/v1/doc/document/draft-acme-thing/') return { body: { name: 'draft-acme-thing', rev: world.draftRev, title: 'Acme Thing', time: '2026-11-02T10:00:00', expires: '2027-05-06T10:00:00' } };
  return undefined;
}
const http = (impl: typeof fetch, budget = 150) => new WatchHttp(budget, { fetchImpl: impl, sleep: noSleep.sleep, githubRateFloor: GITHUB_RATE_FLOOR });

async function main() {
  console.log('protocol watch contract');

  // 1. The committed catalogue.
  const committed = loadCatalogue();
  assert.equal(committed.version, catalogueVersionOf(committed));
  const auto = automatedSources(committed);
  assert.ok(auto.length >= 20 && auto.every((s) => s.method === 'GITHUB_REPOSITORY' || s.method === 'IETF_DRAFT'));
  assert.ok(committed.sources.some((s) => s.gate !== 'ALLOWED'), 'blocked sources are catalogued, not dropped');
  assert.ok(auto.some((s) => s.target === 'x402-foundation/x402') && !committed.sources.some((s) => s.target === 'coinbase/x402'), 'official x402 repo');
  ok(`committed catalogue hashes to its version; ${auto.length} automated of ${committed.sources.length}; blocked never automated`);

  // 2. Tampering / unsafe catalogues are refused.
  const raw = JSON.parse(readFileSync(join(root, 'config', 'protocol-watch-sources.v1.json'), 'utf8'));
  assert.throws(() => parseCatalogue({ ...raw, sources: raw.sources.slice(1) }), /does not match its version/);
  const sneaky = { ...raw, sources: raw.sources.map((s: { gate: string; method: string }) => (s.method === 'REGISTRY_API' ? { ...s, gate: 'ALLOWED' } : s)) };
  assert.throws(() => parseCatalogue({ ...sneaky, version: catalogueVersionOf(sneaky) }), /cannot be automated in Phase 2/);
  ok('hand-edited or registry-automating catalogue refused');

  // 3. Host allowlist: nothing outside api.github.com / datatracker.ietf.org is ever requested.
  const f0 = fakeFetch(() => ({ body: {} }));
  await assert.rejects(http(f0.impl).getJson('https://registry.example/api'), (e: WatchHttpError) => e.code === 'HOST_NOT_ALLOWED');
  await assert.rejects(http(f0.impl).getJson('http://api.github.com/repos/a/b'), (e: WatchHttpError) => e.code === 'HOST_NOT_ALLOWED');
  assert.equal(f0.calls.length, 0);
  ok('non-allowlisted host and plain http refused before any request');

  // 4. First run: baseline only.
  const store = new MemoryWatchStore();
  const cat = miniCatalogue();
  const f1 = fakeFetch(route);
  const r1 = await runWatch({ catalogue: cat, store, http: http(f1.impl), runKey: 'schedule:2026-11-06t00', trigger: 'schedule' });
  assert.equal(r1.status, 'SUCCEEDED'); assert.equal(r1.planned, 3); assert.equal(r1.blocked, 2);
  assert.ok(f1.calls.every((c) => /^https:\/\/(api\.github\.com|datatracker\.ietf\.org)\//.test(c)), 'only allowlisted hosts');
  assert.ok(!f1.calls.some((c) => c.includes('registry.example') || c.includes('bazaar.example')), 'blocked sources never fetched');
  assert.equal(store.snapshots.length, 3);
  const obs1 = [...store.observations.values()];
  assert.deepEqual(obs1.map((o) => o.kind).sort(), ['DRAFT_REVISION', 'RELEASE_PUBLISHED']);
  assert.equal(obs1.find((o) => o.kind === 'RELEASE_PUBLISHED')!.title, 'v1.1.0', 'baseline is the newest release only');
  assert.equal(obs1.find((o) => o.kind === 'RELEASE_PUBLISHED')!.sourcePublishedAt, '2026-10-01T00:00:00.000Z');
  assert.equal(obs1.find((o) => o.kind === 'DRAFT_REVISION')!.sourcePublishedAt, null, 'a timestamp without a zone is unknown, never guessed');
  ok('first run: 3 snapshots, baseline = newest release + current draft revision, no tag flood, blocked sources untouched');

  // 5. Same slot again: idempotent no-op.
  const f2 = fakeFetch(route);
  const r2 = await runWatch({ catalogue: cat, store, http: http(f2.impl), runKey: 'schedule:2026-11-06t00', trigger: 'schedule' });
  assert.equal(r2.status, 'NOOP_ALREADY_FINISHED'); assert.equal(f2.calls.length, 0);
  ok('re-running a finished slot makes zero requests and writes nothing');

  // 6. Next slot, nothing changed (stars are volatile and excluded): write-on-change.
  const r3 = await runWatch({ catalogue: cat, store, http: http(fakeFetch(route).impl), runKey: 'schedule:2026-11-06t12', trigger: 'schedule' });
  assert.equal(r3.status, 'SUCCEEDED'); assert.equal(store.snapshots.length, 3); assert.equal(store.observations.size, 2);
  ok('unchanged sources write no snapshot and no observation (volatile counters excluded)');

  // 7. A new release, a tag with a release and a bare tag; draft revision; archive and relocation.
  world.spec.releases = [{ id: 3, tag_name: 'v1.2.0', name: 'Version 1.2', published_at: '2026-11-06T09:00:00Z', html_url: 'https://github.com/acme/spec/releases/tag/v1.2.0' }, ...world.spec.releases];
  world.spec.tags = ['v1.2.0', 'v2.0.0-rc.1', ...world.spec.tags];
  world.draftRev = '04';
  await runWatch({ catalogue: cat, store, http: http(fakeFetch(route).impl), runKey: 'schedule:2026-11-07t00', trigger: 'schedule' });
  const added = [...store.observations.values()].slice(2);
  assert.deepEqual(added.map((o) => `${o.kind}:${o.title}`).sort(), ['DRAFT_REVISION:draft-acme-thing-04', 'RELEASE_PUBLISHED:Version 1.2', 'TAG_CREATED:v2.0.0-rc.1']);
  assert.match(String(added.find((o) => o.kind === 'TAG_CREATED')!.detail.note), /not necessarily an official release/);
  world.spec.archived = true; world.spec.fullName = 'acme-foundation/spec';
  await runWatch({ catalogue: cat, store, http: http(fakeFetch(route).impl), runKey: 'schedule:2026-11-07t12', trigger: 'schedule' });
  const kinds = [...store.observations.values()].map((o) => o.kind);
  assert.ok(kinds.includes('REPOSITORY_ARCHIVED') && kinds.includes('REPOSITORY_RELOCATED'));
  ok('release vs bare tag distinguished; draft revision, archive and relocation observed');

  // 8. 404 is UNAVAILABLE, repeated, and never deletes anything.
  const snapsBefore = store.snapshots.length, obsBefore = store.observations.size;
  world.other.status = 404;
  for (const k of ['schedule:2026-11-08t00', 'schedule:2026-11-08t12', 'schedule:2026-11-09t00']) {
    const r = await runWatch({ catalogue: cat, store, http: http(fakeFetch(route).impl), runKey: k, trigger: 'schedule' });
    assert.equal(r.status, 'PARTIAL');
  }
  const other = store.subjects.get('github:acme/other')!;
  assert.equal(other.lastOutcome, 'UNAVAILABLE'); assert.equal(other.consecutiveFailures, 3); assert.equal(other.lastHttpStatus, 404);
  assert.ok(other.lastSuccessAt !== null, 'earlier success kept');
  assert.equal(store.snapshots.length, snapsBefore); assert.equal(store.observations.size, obsBefore);
  assert.ok(store.snapshots.some((s) => s.subjectId === 'github:acme/other'), 'history of the unavailable source intact');
  world.other.status = 200;
  ok('404 x3 → UNAVAILABLE with history intact; nothing removed');

  // 9. Retry policy: 5xx retried then recovers; persistent 5xx fails; Retry-After beyond cap is not waited out.
  noSleep.sleeps.length = 0;
  const f9 = fakeFetch((u) => (u.pathname === '/repos/acme/spec' ? [{ status: 502 }, { body: { full_name: 'acme/spec' } }] : route(u)));
  const h9 = http(f9.impl);
  assert.equal((await h9.getJson<{ full_name: string }>('https://api.github.com/repos/acme/spec')).body!.full_name, 'acme/spec');
  assert.ok(noSleep.sleeps.includes(2_000));
  await assert.rejects(http(fakeFetch(() => ({ status: 503 })).impl).getJson('https://datatracker.ietf.org/api/v1/x'), (e: WatchHttpError) => e.code === 'HTTP_503');
  noSleep.sleeps.length = 0;
  await assert.rejects(http(fakeFetch(() => ({ status: 429, headers: { 'retry-after': '3600' } })).impl).getJson('https://datatracker.ietf.org/api/v1/x'), (e: WatchHttpError) => e.code === 'HTTP_429');
  assert.ok(noSleep.sleeps.every((ms) => ms <= RETRY_AFTER_CAP_MS) && !noSleep.sleeps.includes(3_600_000));
  noSleep.sleeps.length = 0;
  const f9b = fakeFetch(() => [{ status: 429, headers: { 'retry-after': '3' } }, { body: { ok: true } }]);
  await http(f9b.impl).getJson('https://datatracker.ietf.org/api/v1/y');
  assert.ok(noSleep.sleeps.includes(3_000));
  ok('5xx retried with backoff; persistent 5xx fails; Retry-After honoured up to the cap, never waited beyond it');

  // 10. GitHub rate floor: GitHub stops, IETF continues, run is PARTIAL, nothing marked failed.
  const s10 = new MemoryWatchStore();
  const f10 = fakeFetch((u) => { const fx = route(u); return u.hostname === 'api.github.com' && fx && !Array.isArray(fx) ? { ...fx, headers: { 'x-ratelimit-remaining': String(GITHUB_RATE_FLOOR - 1) } } : fx; });
  const r10 = await runWatch({ catalogue: cat, store: s10, http: http(f10.impl), runKey: 'manual:gh-1', trigger: 'manual', concurrency: 1 });
  assert.equal(r10.githubRateFloorHit, true); assert.equal(r10.status, 'PARTIAL');
  assert.ok(r10.outcomes.some((o) => o.outcome === 'SKIPPED' && o.detail === 'RATE_LIMIT_FLOOR'));
  assert.ok(r10.outcomes.some((o) => o.sourceId === 'ietf:draft-acme-thing' && o.outcome === 'SUCCESS'));
  assert.ok(!r10.outcomes.some((o) => o.outcome === 'FAILED'));
  ok('GitHub quota floor stops GitHub requests for the run (SKIPPED, not FAILED); IETF unaffected');

  // 11. Request budget: exhausted → SKIPPED.
  const r11 = await runWatch({ catalogue: cat, store: new MemoryWatchStore(), http: http(fakeFetch(route).impl, 2), runKey: 'manual:gh-2', trigger: 'manual', concurrency: 1 });
  assert.ok(r11.requests <= 2); assert.ok(r11.outcomes.some((o) => o.outcome === 'SKIPPED' && o.detail === 'BUDGET_EXHAUSTED'));
  ok('per-run request budget enforced (remaining sources SKIPPED)');

  // 12. Checkpoint/restart: an interrupted run resumes without repeating finished sources.
  const s12 = new MemoryWatchStore();
  const began = await s12.beginRun({ runKey: 'schedule:2026-11-10t00', catalogueVersion: cat.version, trigger: 'schedule', planned: 3 });
  await s12.recordSuccess({ runId: began.runId, source: cat.sources[0], result: { outcome: 'SUCCESS', state: { kind: 'x' }, stateHash: 'a'.repeat(64), observations: [], httpStatus: 200 } });
  const f12 = fakeFetch(route);
  const r12 = await runWatch({ catalogue: cat, store: s12, http: http(f12.impl), runKey: 'schedule:2026-11-10t00', trigger: 'schedule' });
  assert.equal(r12.resumed, true);
  assert.ok(r12.outcomes.some((o) => o.sourceId === 'github:acme/spec' && o.outcome === 'ALREADY_DONE'));
  assert.ok(!f12.calls.some((c) => c.includes('/repos/acme/spec')), 'finished source not re-fetched');
  ok('interrupted run resumes from its checkpoint; finished sources are not re-fetched');

  // 13. Abandoned receipts are closed, never left RUNNING.
  let t = Date.parse('2026-11-10T00:00:00Z');
  const s13 = new MemoryWatchStore(() => t);
  await s13.beginRun({ runKey: 'schedule:2026-11-10t00', catalogueVersion: cat.version, trigger: 'schedule', planned: 3 });
  t += 2 * 3_600_000;
  await s13.beginRun({ runKey: 'schedule:2026-11-10t12', catalogueVersion: cat.version, trigger: 'schedule', planned: 3 });
  assert.equal(s13.runs[0].status, 'ABANDONED');
  ok('stale RUNNING receipt marked ABANDONED by the next run');

  // 14. SSRF guard: an allowlisted name resolving to a private address is refused and recorded as FAILED.
  setSsrfTestPolicy({ lookup: async (h) => (h === 'api.github.com' ? ['127.0.0.1'] : ['104.16.44.99']) });
  const s14 = new MemoryWatchStore();
  const f14 = fakeFetch(route);
  const r14 = await runWatch({ catalogue: cat, store: s14, http: http(f14.impl), runKey: 'manual:gh-3', trigger: 'manual' });
  assert.ok(r14.outcomes.filter((o) => o.sourceId.startsWith('github:')).every((o) => o.outcome === 'FAILED' && o.detail === 'SSRF_BLOCKED'));
  assert.ok(!f14.calls.some((c) => c.includes('api.github.com')), 'no request reached the private address');
  setSsrfTestPolicy({ lookup: async (h) => (h === 'api.github.com' ? ['140.82.112.6'] : ['104.16.44.99']) });
  ok('DNS answer in private space refused before any request (SSRF)');

  // 15. Real sockets: loopback fixture through real fetch; per-host pacing; timeout is a FAILURE not a deletion.
  setSsrfTestPolicy({ allowLoopback: true, lookup: async () => ['127.0.0.1'] });
  let hits = 0;
  const server = createServer((req, res) => {
    hits++;
    if (req.url?.startsWith('/hang')) return; // never answers
    res.setHeader('content-type', 'application/json'); res.end(JSON.stringify({ name: 'draft-acme-thing', rev: '05', title: 'T', time: '2026-11-06T00:00:00' }));
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', () => r()));
  const port = (server.address() as { port: number }).port;
  const real = new WatchHttp(10, { rewrite: (u) => new URL(`http://127.0.0.1:${port}${u.pathname}${u.search}`), timeoutMs: 400, sleep: async () => undefined });
  const got = await real.getJson<{ rev: string }>('https://datatracker.ietf.org/api/v1/doc/document/draft-acme-thing/?format=json');
  assert.equal(got.body!.rev, '05');
  await assert.rejects(real.getJson('https://datatracker.ietf.org/hang'), (e: WatchHttpError) => e.code === 'TIMEOUT');
  server.closeAllConnections?.(); server.close();
  assert.ok(hits >= 2);
  const paced: number[] = [];
  let clock = 0;
  const p = new WatchHttp(10, { fetchImpl: fakeFetch(() => ({ body: {} })).impl, now: () => clock, sleep: async (ms) => { paced.push(ms); clock += ms; } });
  setSsrfTestPolicy({ lookup: async () => ['104.16.44.99'] });
  await p.getJson('https://datatracker.ietf.org/a'); await p.getJson('https://datatracker.ietf.org/b');
  assert.ok(paced.some((ms) => ms >= 1_900), 'second request to the same host waits the minimum gap');
  ok('real-socket fixture works; request timeout surfaces as TIMEOUT; per-host pacing enforced');

  // 16. Run keys.
  assert.equal(scheduleSlotKey(new Date('2026-11-06T13:15:00Z')), 'schedule:2026-11-06t12');
  assert.equal(scheduleSlotKey(new Date('2026-11-06T01:15:00Z')), 'schedule:2026-11-06t00');
  assert.equal(manualRunKey('123456', new Date()), 'manual:gh-123456');
  ok('12-hour slot keys and manual keys are deterministic');

  // 17. Worker: write mode is refused unless PROTOCOL_WATCH_ENABLED === "true" (before any database connection).
  const env = { ...process.env, PROTOCOL_WATCH_MODE: 'write', PROTOCOL_WATCH_ENABLED: 'false', AIVE_INGEST_DATABASE_URL: 'postgres://nobody:none@127.0.0.1:9/none' };
  const w = spawnSync(process.execPath, [join(root, 'node_modules', 'tsx', 'dist', 'cli.mjs'), join(root, 'workers', 'protocol-watch.ts')], { env, encoding: 'utf8', timeout: 60_000 });
  assert.equal(w.status, 2, w.stderr); assert.match(w.stderr, /write mode refused/);
  ok('worker refuses write mode while monitoring is disabled');

  // 18. Workflow: disabled by default, schedule + dispatch only, shared DB lane, alert coverage.
  const wf = readFileSync(join(root, '.github', 'workflows', 'protocol-watch.yml'), 'utf8').replace(/\r/g, '');
  assert.match(wf, /if: \$\{\{ vars\.PROTOCOL_WATCH_ENABLED == 'true' \}\}/);
  // Amendment 1: single lane (own group, pool 1), never the shared aive-ingest-db lane, where a newer pending job
  // silently cancels an older pending one.
  assert.doesNotMatch(wf, /group: aive-ingest-db/);
  assert.match(wf, /^concurrency:\n  group: protocol-watch\n  cancel-in-progress: false/m);
  assert.match(wf, /AIVE_INGEST_POOL_MAX: '1'/);
  assert.doesNotMatch(wf, /^\s+(push|pull_request|pull_request_target):/m);
  const secrets = [...wf.matchAll(/secrets\.([A-Z_]+)/g)].map((m) => m[1]).sort();
  assert.deepEqual([...new Set(secrets)], ['AIVE_INGEST_DATABASE_URL', 'GITHUB_TOKEN']);
  assert.equal((wf.match(/cron:/g) ?? []).length, 1, 'one schedule, not one per protocol');
  const alert = readFileSync(join(root, '.github', 'workflows', 'workflow-failure-alert.yml'), 'utf8');
  assert.match(alert, /- 'Protocol Watch'/);
  ok('workflow gated by vars.PROTOCOL_WATCH_ENABLED, single cron, single DB lane (pool 1), no PAT, failure alert covers it');

  // 19. The worker's dry-run mode never touches the database module.
  const src = readFileSync(join(root, 'workers', 'protocol-watch.ts'), 'utf8');
  assert.match(src, /if \(mode === 'write'\) \{[\s\S]*await import\('\.\.\/lib\/ingest\/db'\)/);
  assert.doesNotMatch(src, /^import .*lib\/ingest\/db/m);
  ok('database module is imported only in write mode');

  console.log(`protocol watch contract: ${passed} checks passed`);
}

main().then(() => process.exit(0), (err) => { console.error(err); process.exit(1); });
