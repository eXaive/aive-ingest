/** Verifies the hourly OU1COMES introduction worker with a fake app API: no network, provider or publication.
 *  Run: npx tsx scripts/verify-intro-distribution-worker.ts */
import assert from 'node:assert/strict';
import { introRunDecision, runIntroDistribution } from '../lib/broadcast/introDistribution';

process.env.BROADCAST_INTRO_DISTRIBUTION = '1';
const now = () => new Date(Date.UTC(2026, 9, 24, 19, 7));
const status = (over: any = {}) => ({
  settings: { state: 'active', post_hour_utc: 19, destinations: ['tiktok', 'instagram', 'twitter', 'linkedin'] },
  standing: { authorized: true }, cycles: [{ cycle: 1, revoked_at: null }], rotation: { last: { cycle: 1, position: 4 }, next: { cycle: 1, position: 5 } },
  deliveries: [], ...over,
});
function harness(s: any, failing: Record<string, string> = {}, exportFails = false) {
  const calls: any[] = [], logs: string[] = [];
  const call = async (body?: any) => {
    calls.push(body ?? { action: 'status' });
    if (!body) return s;
    if (body.action === 'prepare') return { day: '2026-10-24', cycle: 1, position: 5, category: 'cash', status: 'CONCEPT', mediaUrl: 'https://database.blotato.io/storage/v1/object/public/public_media/a.mp4', sha256: 'a'.repeat(64) };
    if (body.action === 'deliver') { if (failing[body.platform]) throw Error(failing[body.platform]); return { jobId: 'job-' + body.platform, state: 'provider_accepted' }; }
    return {};
  };
  const xExport = async () => { if (exportFails) throw Error('ffmpeg missing'); return { url: 'https://database.blotato.io/x.mp4', outputSha256: 'b'.repeat(64) } as any; };
  return { calls, logs, run: () => runIntroDistribution({ call, xExport, now, log: m => logs.push(m) }) };
}
(async () => {
  // Decisions mirror the server gates (the server still decides).
  assert.equal(introRunDecision(status({ settings: { ...status().settings, state: 'paused' } }), now()).reason, 'paused');
  assert.equal(introRunDecision(status({ standing: { authorized: false } }), now()).reason, 'standing_policy_required');
  assert.equal(introRunDecision(status(), new Date(Date.UTC(2026, 9, 24, 18, 7))).reason, 'before_post_time');
  assert.equal(introRunDecision(status({ rotation: { next: { cycle: 2, position: 1 } } }), now()).reason, 'cycle_not_approved');
  console.log('ok - decision gates: paused, standing authorization, posting hour, next cycle approval');

  // Paused: reconciliation still runs for accepted receipts; nothing is prepared or delivered.
  let h = harness(status({ settings: { ...status().settings, state: 'paused' }, deliveries: [{ job_id: 'old', platform: 'tiktok', utc_day: '2026-10-23', attempt: { state: 'provider_accepted' } }] }));
  await h.run();
  assert.deepEqual(h.calls.map(c => c.action), ['status', 'reconcile']);
  console.log('ok - pause: prior receipts reconciled, no new submission');

  // Active: TikTok first, one deliver per destination, X gets the verified derivative, each accepted job reconciled once.
  h = harness(status()); await h.run();
  assert.deepEqual(h.calls.filter(c => c.action === 'deliver').map(c => c.platform), ['tiktok', 'instagram', 'twitter', 'linkedin']);
  assert.equal(h.calls.find(c => c.action === 'deliver' && c.platform === 'twitter').derivative.outputSha256, 'b'.repeat(64));
  assert.equal(h.calls.filter(c => c.action === 'prepare').length, 1);
  assert.equal(h.calls.filter(c => c.action === 'reconcile').length, 4);
  console.log('ok - active day: one prepare, TikTok-first single delivery per destination, X derivative, reconcile per new job');

  // Partial failures: an uncertain/blocked destination is not retried; the others still run.
  h = harness(status(), { instagram: 'intro_capacity_unavailable' }, true); await h.run();
  assert.deepEqual(h.calls.filter(c => c.action === 'deliver').map(c => c.platform), ['tiktok', 'instagram', 'linkedin']);
  assert.equal(h.calls.find(c => c.action === 'block').reason, 'worker_x_export_unavailable');
  assert.ok(h.logs.some(l => l.includes('instagram: intro_capacity_unavailable; no automatic retry')));
  console.log('ok - partial failures: blocked destination logged without retry, X export failure recorded, others continue');

  // Restart later the same day: only destinations without a delivery today are attempted.
  h = harness(status({ deliveries: [{ job_id: 'j1', platform: 'tiktok', utc_day: '2026-10-24', attempt: { state: 'confirmed_published' } }, { job_id: 'j2', platform: 'instagram', utc_day: '2026-10-24', attempt: { state: 'outcome_unknown' } }] }));
  await h.run();
  assert.deepEqual(h.calls.filter(c => c.action === 'deliver').map(c => c.platform), ['twitter', 'linkedin']);
  console.log('ok - restart: delivered and uncertain destinations are never resubmitted');

  // Destination selection and completion.
  h = harness(status({ settings: { ...status().settings, destinations: ['linkedin'] } })); await h.run();
  assert.deepEqual(h.calls.filter(c => c.action === 'deliver').map(c => c.platform), ['linkedin']);
  h = harness(status({ settings: { ...status().settings, destinations: ['linkedin'] }, deliveries: [{ job_id: 'j', platform: 'linkedin', utc_day: '2026-10-24', attempt: { state: 'provider_accepted' } }] }));
  await h.run(); assert.ok(!h.calls.some(c => c.action === 'prepare'));
  console.log('ok - destination selection honoured; completed day makes no further calls beyond reconciliation');

  process.env.BROADCAST_INTRO_DISTRIBUTION = '0';
  h = harness(status()); await h.run(); assert.equal(h.calls.length, 0);
  console.log('ok - inert unless BROADCAST_INTRO_DISTRIBUTION=1');
  console.log('PASS intro distribution worker.');
})().catch(e => { console.error(e); process.exitCode = 1; });
