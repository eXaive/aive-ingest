/**
 * Watchdog onset contract: a breach is NEW while its onset sits inside
 * NEW_WITHIN_HOURS and ONGOING after, an undatable breach is never ONGOING,
 * and an escalated streak dates from the run that reached the threshold.
 */
import assert from 'node:assert/strict';
import { breachAge, cacheFailureStreak, NEW_WITHIN_HOURS, streakOnsetIndex } from '../workers/ingest-watchdog';

const now = Date.parse('2026-10-07T12:00:00Z');
const hoursAgo = (h: number) => new Date(now - h * 3_600_000).toISOString();

assert.equal(breachAge(hoursAgo(1), now), 'NEW');
assert.equal(breachAge(hoursAgo(NEW_WITHIN_HOURS), now), 'NEW', 'window edge is inclusive');
assert.equal(breachAge(hoursAgo(NEW_WITHIN_HOURS + 0.1), now), 'ONGOING');
assert.equal(breachAge(hoursAgo(24 * 7), now), 'ONGOING', 'a week-old red is ongoing, not news');
assert.equal(breachAge(null, now), 'NEW', 'unknown onset never passes as old news');
assert.equal(breachAge('not-a-date', now), 'NEW');

// Newest-first streak of 5 with threshold 3: escalation began at the 3rd-oldest failure (index 2).
assert.equal(streakOnsetIndex(5, 3), 2);
assert.equal(streakOnsetIndex(3, 3), 0, 'exactly at threshold: onset is the newest run');
assert.equal(streakOnsetIndex(2, 3), null, 'below threshold there is no escalated onset');

// SKIPPED is stepped over, never a reset; SUCCEEDED ends the streak. Hosted shape 2026-10-06.
assert.deepEqual(cacheFailureStreak(['FAILED', 'SKIPPED', 'SKIPPED', 'FAILED', 'FAILED', 'SUCCEEDED', 'FAILED']), [0, 3, 4]);
assert.deepEqual(cacheFailureStreak(['SUCCEEDED', 'FAILED']), []);
assert.deepEqual(cacheFailureStreak(['SKIPPED', null]), [], 'no evidence is not a failure');

console.log('Watchdog onset contract: passed');
