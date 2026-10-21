/** Installed beside the existing daily worker. No generation and no TikTok POST.
 * Disabled unless explicitly enabled AND the server has a valid standing policy.
 */
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
const enabled = () => process.env.BROADCAST_AUTOMATIC_DISTRIBUTION === '1';
async function call(body?: unknown): Promise<any> {
  const base = new URL(process.env.AIVE_BASE_URL!);
  if (base.protocol !== 'https:' || base.username || base.password) throw Error('automatic_app_origin_invalid');
  const response = await fetch(new URL('/api/broadcast/automatic-distribution', base), {
    method: body ? 'POST' : 'GET', redirect: 'error', signal: AbortSignal.timeout(120000),
    headers: { Authorization: `Bearer ${process.env.AIVE_CRON_SECRET}`, 'Content-Type': 'application/json' },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  if (!response.ok) throw Error('automatic_request_blocked'); return response.json();
}
export async function captureAutomatic(jobId: string, creationId: string | null, requestedGeneration?: unknown) {
  if (!enabled()) return;
  try { await call({ action: 'capture', jobId, creationId, requestedGeneration }); }
  catch { console.log('[automatic-distribution] capture blocked; TikTok unchanged; inspect owner status'); }
}
async function xExport(video: any) {
  const url = new URL(video.mediaUrl);
  if (url.protocol !== 'https:' || url.hostname !== 'database.blotato.io' || url.username || url.password || url.port) throw Error('x_source_origin_invalid');
  const ffmpeg = process.env.AIVE_SHORTS_FFMPEG || '/usr/bin/ffmpeg';
  // A supported runner must supply both executables. Never assume Vercel can transcode.
  execFileSync(ffmpeg, ['-version'], { stdio: 'ignore', timeout: 10000, windowsHide: true });
  execFileSync(path.join(path.dirname(ffmpeg), process.platform === 'win32' ? 'ffprobe.exe' : 'ffprobe'), ['-version'], { stdio: 'ignore', timeout: 10000, windowsHide: true });
  const directory = await mkdtemp(path.join(tmpdir(), 'aive-auto-x-'));
  try {
    const response = await fetch(url, { redirect: 'error', signal: AbortSignal.timeout(60000) });
    if (!response.ok || !response.body) throw Error('x_source_unavailable');
    const chunks: Uint8Array[] = []; let length = 0;
    for await (const chunk of response.body as any) { length += chunk.length; if (length > 200 * 1024 * 1024) throw Error('x_source_too_large'); chunks.push(chunk); }
    const bytes = Buffer.concat(chunks);
    if (createHash('sha256').update(bytes).digest('hex') !== video.sourceSha256) throw Error('x_source_hash_changed');
    const input = path.join(directory, 'source.mp4'), output = path.join(directory, 'x.mp4');
    const { writeFile } = await import('node:fs/promises'); await writeFile(input, bytes, { flag: 'wx' });
    execFileSync(process.execPath, [path.join(__dirname, 'prepare-broadcast-x.cjs'), input, output],
      { env: { ...process.env, AIVE_SHORTS_FFMPEG: ffmpeg }, timeout: 360000, stdio: 'pipe', windowsHide: true });
    const verification = JSON.parse(await readFile(output + '.verification.json', 'utf8'));
    const rendered = await readFile(output);
    if (verification.sourceSha256 !== video.sourceSha256 || createHash('sha256').update(rendered).digest('hex') !== verification.outputSha256) throw Error('x_export_identity_changed');
    const headers = { 'blotato-api-key': process.env.BLOTATO_API_KEY!, 'Content-Type': 'application/json' };
    const upload = await fetch('https://backend.blotato.com/v2/media/uploads', { method: 'POST', headers,
      body: JSON.stringify({ filename: `aive-x-${verification.outputSha256}.mp4` }), redirect: 'error', signal: AbortSignal.timeout(30000) });
    if (!upload.ok) throw Error('x_upload_unavailable');
    const target: any = await upload.json();
    const signed = new URL(target.presignedUrl), publicUrl = new URL(target.publicUrl);
    if (signed.protocol !== 'https:' || signed.username || signed.password || publicUrl.protocol !== 'https:' || publicUrl.hostname !== 'database.blotato.io') throw Error('x_upload_origin_invalid');
    const put = await fetch(signed, { method: 'PUT', headers: { 'Content-Type': 'video/mp4' }, body: rendered,
      redirect: 'error', signal: AbortSignal.timeout(60000) });
    if (!put.ok) throw Error('x_upload_failed');
    return { ...verification, url: publicUrl.href };
  } finally {
    if (path.dirname(path.resolve(directory)) !== path.resolve(tmpdir()) || !path.basename(directory).startsWith('aive-auto-x-'))
      throw Error('x_cleanup_path_invalid');
    await rm(directory, { recursive: true, force: true });
  }
}
export async function resumeAutomatic(onlyJob?: string) {
  if (!enabled()) return;
  try {
    const state = await call();
    // Reconcile known submissions even while standing policy is disabled.
    for (const delivery of state.deliveries.slice(-30)) {
      const attempt = state.attempts.find((a: any) => a.id === delivery.attempt_id);
      if (['provider_accepted', 'confirmation_pending'].includes(attempt?.state)) {
        try { await call({ action: 'reconcile', jobId: delivery.job_id }); } catch { console.log('[automatic-distribution] receipt pending; no resubmission'); }
      }
    }
    if (!state.policies.some((p: any) => p.enabled && !p.revoked_at && Date.parse(p.expires_at) > Date.now())) return;
    const videos = state.videos.filter((v: any) => !v.blocked && (!onlyJob || v.sourceJob === onlyJob)
      && state.policies.some((p: any) => p.id === v.policyId && p.enabled && !p.revoked_at && Date.parse(p.expires_at) > Date.now())
      && state.deliveries.filter((d: any) => d.source_job === v.sourceJob).length < 3).slice(0, 3);
    for (const video of videos) for (const platform of ['linkedin', 'instagram', 'twitter']) {
      if (state.deliveries.some((d: any) => d.source_job === video.sourceJob && d.platform === platform)) continue;
      try {
        let derivative;
        if (platform === 'twitter') {
          try { derivative = await xExport(video); }
          catch { await call({ action: 'export-blocked', jobId: video.sourceJob }); continue; }
        }
        const result = await call({ action: 'deliver', jobId: video.sourceJob, platform, derivative });
        if (result.jobId) await call({ action: 'reconcile', jobId: result.jobId });
        console.log(`[automatic-distribution] ${platform}: recorded; verify owner receipt`);
      } catch { console.log(`[automatic-distribution] ${platform}: blocked or uncertain; no automatic submission retry`); }
    }
  } catch { console.log('[automatic-distribution] unavailable; TikTok unchanged'); }
}
