/**
 * Connection topology contract (2026-11-06). aive_ingest has CONNECTION LIMIT 4 for the whole role.
 *
 * Every workflow that carries AIVE_INGEST_DATABASE_URL must be classified into exactly one lane:
 *   INGEST LANE    its database job is in job-level concurrency group 'aive-ingest-db' (cancel-in-progress: false)
 *                  and does not raise AIVE_INGEST_POOL_MAX above DEFAULT_POOL_MAX: at most ONE such job runs, with
 *                  at most 2 connections.
 *   SINGLE LANE    every AIVE_INGEST_DATABASE_URL entry is paired with AIVE_INGEST_POOL_MAX: '1'.
 * A new workflow that gets the secret without either fails here, so the budget cannot drift silently.
 *
 * It also states the bound honestly: ingest lane 2 + one connection per single-lane workflow running at once.
 * The single-lane workflows keep their own per-workflow concurrency groups (at most one run each); a 14-day replay
 * of real runs (2026-10-22..11-05) never had more than 2 of them at once (peak 3 connections in total). Excess
 * beyond 4 is a refusal at connect (53300), retried by lib/ingest/db.ts before any statement runs.
 *
 * Static: reads .github/workflows only. Run: npx tsx scripts/verify-connection-topology.ts
 */
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { DEFAULT_POOL_MAX, ROLE_CONNECTION_LIMIT } from '../lib/ingest/db';

const dir = join(__dirname, '..', '.github', 'workflows');
type Job = { name: string; lines: string[] };
function jobsOf(text: string): Job[] {
  const lines = text.replace(/\r/g, '').split('\n');
  const out: Job[] = []; let inJobs = false; let cur: Job | null = null;
  for (const l of lines) {
    if (/^jobs:\s*$/.test(l)) { inJobs = true; continue; }
    if (inJobs && /^\S/.test(l)) { inJobs = false; cur = null; }
    if (!inJobs) continue;
    const m = l.match(/^  ([A-Za-z0-9_-]+):\s*$/);
    if (m) { cur = { name: m[1], lines: [] }; out.push(cur); } else if (cur) cur.lines.push(l);
  }
  return out;
}

const ingestLane: string[] = []; const singleLane: string[] = []; const problems: string[] = [];
for (const f of readdirSync(dir).filter((x) => /\.ya?ml$/.test(x)).sort()) {
  const text = readFileSync(join(dir, f), 'utf8');
  if (!text.includes('AIVE_INGEST_DATABASE_URL')) continue;
  for (const job of jobsOf(text).filter((j) => j.lines.some((l) => l.includes('AIVE_INGEST_DATABASE_URL')))) {
    const body = job.lines.join('\n');
    const inGroup = /^    concurrency:\n      group: aive-ingest-db\n      cancel-in-progress: false$/m.test(body);
    const urlLines = job.lines.map((l, i) => [l, i] as const).filter(([l]) => /AIVE_INGEST_DATABASE_URL:/.test(l));
    const allPool1 = urlLines.length > 0 && urlLines.every(([, i]) => /^\s*AIVE_INGEST_POOL_MAX: '1'/.test(job.lines[i + 1] ?? ''));
    const poolOverrides = job.lines.filter((l) => /AIVE_INGEST_POOL_MAX:/.test(l)).map((l) => Number((l.match(/AIVE_INGEST_POOL_MAX:\s*'?(\d+)/) ?? [])[1]));
    const id = `${f}#${job.name}`;
    if (inGroup && allPool1) problems.push(`${id}: classified twice`);
    else if (inGroup) { if (poolOverrides.some((n) => !(n <= DEFAULT_POOL_MAX))) problems.push(`${id}: ingest lane raises the pool`); ingestLane.push(id); }
    else if (allPool1) singleLane.push(id);
    else problems.push(`${id}: carries AIVE_INGEST_DATABASE_URL but is in neither lane`);
  }
}

assert.deepEqual(problems, [], `unclassified or misclassified database jobs:\n  ${problems.join('\n  ')}`);
assert.equal(DEFAULT_POOL_MAX, 2); assert.equal(ROLE_CONNECTION_LIMIT, 4);
assert.ok(ingestLane.length >= 9, `ingest lane holds the ${ingestLane.length} ingest jobs`);
const ingestBound = DEFAULT_POOL_MAX;                 // one job at a time in the group
const headroomForSingleLane = ROLE_CONNECTION_LIMIT - ingestBound;
console.log(`  ingest lane (one at a time, pool ${DEFAULT_POOL_MAX}): ${ingestLane.length} jobs: ${ingestLane.join(', ')}`);
console.log(`  single lane (pool 1, own groups): ${singleLane.length} jobs: ${singleLane.join(', ')}`);
console.log(`  bound: ${ingestBound} + 1 per concurrent single-lane job; ${headroomForSingleLane} single-lane jobs fit inside ${ROLE_CONNECTION_LIMIT} (replay peak: 2)`);
console.log('Connection topology contract: passed');
