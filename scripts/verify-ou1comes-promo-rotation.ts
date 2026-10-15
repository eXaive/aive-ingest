/**
 * OU1COMES promotion in the Daily Post rotation (lib/broadcast/promoRotation.ts).
 *
 * Asserts, against an in-memory broadcast_topic_queue:
 *   - Monday/Wednesday/Friday (UTC) pick promo topics; the other days regular.
 *   - Each pool keeps queue_position order and loops ON ITS OWN: exhausting
 *     the promo pool never revives regular rows, and vice versa.
 *   - 'skipped' rows are never picked or revived.
 *   - A promotion day with no promo rows falls back to the regular rotation.
 *   - The text-card rotation never picks a promo row and never resets one.
 *   - A forced topic bypasses the pools, as before.
 *
 * NO DATABASE. pg.Pool.prototype.query is intercepted and answers the four
 * statement shapes the two nextTopic functions send.
 *
 * Run: npx tsx scripts/verify-ou1comes-promo-rotation.ts
 */

import assert from 'node:assert/strict';
import pg from 'pg';
import { isPromoDay, topicPoolPredicate, PROMO_TOPIC_PREFIX } from '../lib/broadcast/promoRotation';
import { nextTopic as nextVideoTopic } from '../workers/broadcast-daily-post';
import { nextTopic as nextCardTopic } from '../workers/broadcast-text-card';

process.env.AIVE_INGEST_DATABASE_URL ??= 'postgres://verify:verify@127.0.0.1:5432/verify';

interface Row { id: string; topic_name: string; queue_position: number; status: string; used_as_card: boolean }
let table: Row[] = [];
const writes: string[] = [];

const PROMO_SQL = topicPoolPredicate(true);
const REGULAR_SQL = topicPoolPredicate(false);
function inPool(sql: string, r: Row): boolean {
  const promo = r.topic_name.startsWith(PROMO_TOPIC_PREFIX);
  if (sql.includes(REGULAR_SQL)) return !promo;
  if (sql.includes(PROMO_SQL)) return promo;
  return true;
}
const first = (rows: Row[]) => [...rows].sort((a, b) => a.queue_position - b.queue_position).slice(0, 1);

// eslint-disable-next-line @typescript-eslint/no-explicit-any
(pg.Pool.prototype as any).query = async function fake(sql: string, values: unknown[] = []) {
  const s = sql.replace(/\s+/g, ' ');
  let rows: unknown[];
  if (/WHERE topic_name = \$1/.test(s)) rows = table.filter((r) => r.topic_name === values[0]).slice(0, 1);
  else if (/^SELECT .* WHERE status = 'pending'/.test(s)) rows = first(table.filter((r) => r.status === 'pending' && inPool(s, r)));
  else if (/^UPDATE .* SET status = 'pending'/.test(s)) {
    writes.push(s);
    rows = table.filter((r) => r.status === 'used' && inPool(s, r)).map((r) => { r.status = 'pending'; return { id: r.id }; });
  } else if (/^SELECT .* WHERE used_as_card = false/.test(s)) rows = first(table.filter((r) => !r.used_as_card && r.status !== 'skipped' && inPool(s, r)));
  else if (/^UPDATE .* SET used_as_card = false/.test(s)) {
    writes.push(s);
    rows = table.filter((r) => r.used_as_card && r.status !== 'skipped' && inPool(s, r)).map((r) => { r.used_as_card = false; return { id: r.id }; });
  } else throw new Error(`unexpected statement: ${s.slice(0, 120)}`);
  return { rows, rowCount: rows.length };
};

const row = (topic_name: string, queue_position: number, status = 'pending'): Row =>
  ({ id: topic_name, topic_name, queue_position, status, used_as_card: false });
const seed = (withPromo = true) => {
  table = [row('MCP', 1), row('A2A', 2), row('AIVE Independent Intelligence', 3, 'skipped'), row('x402', 4)];
  if (withPromo) table.push(row('OU1COMES: Find MCP servers', 101), row('OU1COMES: Explore capabilities', 102));
};
const day = (iso: string) => new Date(`${iso}T14:00:00Z`);
const status = (name: string) => table.find((r) => r.topic_name === name)!.status;

let checks = 0;
const ok = (label: string) => { checks++; console.log(`  PASS  ${label}`); };

async function main(): Promise<void> {
  // ── Day mapping ─────────────────────────────────────────────────────────────
  // 2026-10-19 is a Monday.
  const week = ['2026-10-19', '2026-10-20', '2026-10-21', '2026-10-22', '2026-10-23', '2026-10-24', '2026-10-25'];
  assert.deepEqual(week.map((d) => isPromoDay(day(d))), [true, false, true, false, true, false, false]);
  // UTC, not local: 23:30 Sunday UTC is still Sunday.
  assert.equal(isPromoDay(new Date('2026-10-25T23:30:00Z')), false);
  assert.equal(isPromoDay(new Date('2026-10-26T00:30:00Z')), true);
  ok('Mon/Wed/Fri are promotion days, Tue/Thu/Sat/Sun are not (UTC)');

  // ── Eight days of the video rotation ────────────────────────────────────────
  seed();
  const picks: string[] = [];
  for (const d of [...week, '2026-10-26']) {
    const t = await nextVideoTopic(null, day(d));
    assert.ok(t, `topic for ${d}`);
    picks.push(t.topic_name);
    table.find((r) => r.id === t.id)!.status = 'used'; // what a confirmed post writes
    if (d === '2026-10-23') {
      // Friday looped the promo pool; the regular rows used Tue/Thu must still be used.
      assert.equal(status('MCP'), 'used'); assert.equal(status('A2A'), 'used');
    }
  }
  assert.deepEqual(picks, [
    'OU1COMES: Find MCP servers',     // Mon
    'MCP',                            // Tue
    'OU1COMES: Explore capabilities', // Wed
    'A2A',                            // Thu
    'OU1COMES: Find MCP servers',     // Fri — promo pool looped on its own
    'x402',                           // Sat — skipped row passed over
    'MCP',                            // Sun — regular pool looped on its own
    'OU1COMES: Explore capabilities', // Mon
  ]);
  assert.equal(status('AIVE Independent Intelligence'), 'skipped', 'skipped row never revived');
  for (const w of writes) assert.ok(w.includes(PROMO_SQL), `every loop-back is pool-scoped: ${w}`);
  ok('promo on Mon/Wed/Fri, regular otherwise; each pool loops independently');

  // ── Fallback when no promo copy exists ─────────────────────────────────────
  seed(false);
  assert.equal((await nextVideoTopic(null, day('2026-10-19')))?.topic_name, 'MCP');
  ok('a promotion day with no promo rows posts the regular rotation');

  // ── Forced topic bypasses the pools ────────────────────────────────────────
  seed();
  assert.equal((await nextVideoTopic('x402', day('2026-10-19')))?.topic_name, 'x402');
  assert.equal((await nextVideoTopic('OU1COMES: Explore capabilities', day('2026-10-20')))?.topic_name, 'OU1COMES: Explore capabilities');
  ok('BROADCAST_FORCE_TOPIC still selects the exact named topic');

  // ── Text cards never use promo rows ────────────────────────────────────────
  seed();
  writes.length = 0;
  const cards: string[] = [];
  for (let i = 0; i < 5; i++) {
    const t = await nextCardTopic(null);
    assert.ok(t);
    cards.push(t.topic_name);
    table.find((r) => r.id === t.id)!.used_as_card = true;
  }
  assert.deepEqual(cards, ['MCP', 'A2A', 'x402', 'MCP', 'A2A']);
  assert.ok(table.filter((r) => r.topic_name.startsWith(PROMO_TOPIC_PREFIX)).every((r) => !r.used_as_card));
  assert.equal(writes.length, 1); assert.ok(writes[0].includes(REGULAR_SQL), 'card loop-back excludes promo rows');
  ok('text-card rotation skips promo rows and its loop-back never touches them');

  console.log(`\nOU1COMES promo rotation: ${checks} checks passed`);
}

main().catch((e) => { console.error(e instanceof Error ? e.stack : e); process.exit(1); });
