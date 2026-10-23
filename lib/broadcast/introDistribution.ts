/** OU1COMES daily category-introduction distribution. Runs hourly; acts at most once per destination per UTC day.
 * Disabled unless BROADCAST_INTRO_DISTRIBUTION=1 AND the server reports an active rotation with standing authorization.
 * No generation. Every submission is admitted (or refused) by the server's atomic begin; nothing is retried. */
import { xExport } from './automaticDistribution';
const enabled = () => process.env.BROADCAST_INTRO_DISTRIBUTION === '1';
// TikTok first: it shares the account's daily capacity with the other OU1COMES TikTok posts.
const ORDER = ['tiktok', 'instagram', 'twitter', 'linkedin'] as const;
async function call(body?: unknown): Promise<any> {
  const base = new URL(process.env.AIVE_BASE_URL!);
  if (base.protocol !== 'https:' || base.username || base.password) throw Error('intro_app_origin_invalid');
  const response = await fetch(new URL('/api/broadcast/intro-distribution', base), {
    method: body ? 'POST' : 'GET', redirect: 'error', signal: AbortSignal.timeout(180000),
    headers: { Authorization: `Bearer ${process.env.AIVE_CRON_SECRET}`, 'Content-Type': 'application/json' },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const json = await response.json().catch(() => ({}));
  if (!response.ok) throw Error(typeof json.error === 'string' && /^[a-z_]{1,100}$/.test(json.error) ? json.error : 'intro_request_blocked');
  return json;
}
/** Worker-side mirror of the server rules, used only to skip needless calls. The server decides. */
export function introRunDecision(status: any, now = new Date()) {
  const s = status?.settings, day = now.toISOString().slice(0, 10);
  if (!s || s.state !== 'active') return { run: false, reason: s?.state ?? 'not_installed' };
  if (!status.standing?.authorized) return { run: false, reason: 'standing_policy_required' };
  if (now.getUTCHours() < s.post_hour_utc) return { run: false, reason: 'before_post_time' };
  const next = status.rotation?.next;
  if (next && !(status.cycles ?? []).some((c: any) => c.cycle === next.cycle && !c.revoked_at)) return { run: false, reason: 'cycle_not_approved' };
  const done = new Set((status.deliveries ?? []).filter((d: any) => d.utc_day === day).map((d: any) => d.platform));
  const platforms = ORDER.filter(p => s.destinations.includes(p) && !done.has(p));
  return platforms.length ? { run: true, reason: 'due', platforms } : { run: false, reason: 'complete_for_today' };
}
export async function runIntroDistribution(deps = { call, xExport, now: () => new Date(), log: (m: string) => console.log(m) }) {
  if (!enabled()) return;
  let status: any;
  try { status = await deps.call(); } catch { deps.log('[intro-distribution] status unavailable; nothing submitted'); return; }
  // Reconcile accepted submissions regardless of pause/disable: evidence only, never a resubmission.
  for (const d of (status.deliveries ?? []).slice(0, 60)) {
    if (['provider_accepted', 'confirmation_pending'].includes(d.attempt?.state)) {
      try { await deps.call({ action: 'reconcile', jobId: d.job_id }); } catch { deps.log('[intro-distribution] receipt pending; no resubmission'); }
    }
  }
  const decision = introRunDecision(status, deps.now());
  if (!decision.run) { deps.log(`[intro-distribution] no submission: ${decision.reason}`); return; }
  let media: any;
  try { media = await deps.call({ action: 'prepare' }); }
  catch (e) { deps.log(`[intro-distribution] prepare blocked: ${e instanceof Error ? e.message : 'unknown'}; nothing submitted`); return; }
  deps.log(`[intro-distribution] ${media.day}: cycle ${media.cycle}, category ${media.position}/42 (${media.category}, ${media.status})`);
  for (const platform of decision.platforms ?? []) {
    try {
      let derivative;
      if (platform === 'twitter') {
        try { derivative = await deps.xExport({ mediaUrl: media.mediaUrl, sourceSha256: media.sha256 }); }
        catch { await deps.call({ action: 'block', platform, reason: 'worker_x_export_unavailable' }).catch(() => null); deps.log('[intro-distribution] twitter: X export unavailable; skipped today'); continue; }
      }
      const result = await deps.call({ action: 'deliver', platform, media: { url: media.mediaUrl, sha256: media.sha256 }, derivative });
      if (result.jobId && !result.existing) { try { await deps.call({ action: 'reconcile', jobId: result.jobId }); } catch { /* reconciled next hour */ } }
      deps.log(`[intro-distribution] ${platform}: ${result.existing ? 'already recorded' : result.state ?? 'recorded'}; verify owner receipt`);
    } catch (e) { deps.log(`[intro-distribution] ${platform}: ${e instanceof Error ? e.message : 'blocked'}; no automatic retry`); }
  }
}
