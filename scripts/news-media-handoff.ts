/**
 * Media handoff for ONE approved news video to Blotato storage (owner-run, local). The Daily Post worker later
 * re-downloads the same public URL and checks it again (lib/broadcast/newsApproval.ts verifyNewsMedia).
 *
 *   check  --story-id <id> --file <brief.mp4>                                 local bytes == approved render
 *   upload --story-id <id> --file <brief.mp4> --presigned-url <url> --execute  PUT raw bytes (only with --execute)
 *   verify --story-id <id> --url <publicUrl>                                  public bytes == approved render
 *
 * The presigned URL comes from Blotato's create-presigned-upload-url step (filename e.g. "<story_id>.mp4"); its
 * publicUrl is what `news-approval.ts authorize --media-url` records. The presigned URL is never printed.
 */
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { loadNewsApproval, newsTopicName, verifyNewsMedia, type NewsApproval } from '../lib/broadcast/newsApproval';

const [cmd] = process.argv.slice(2);
const arg = (n: string) => { const i = process.argv.indexOf(`--${n}`); return i > 0 ? process.argv[i + 1] : undefined; };
const need = (n: string) => { const v = arg(n); if (!v) throw new Error(`--${n} is required`); return v; };

function checkLocal(a: NewsApproval, file: string): Buffer {
  const bytes = readFileSync(file);
  const sha = createHash('sha256').update(bytes).digest('hex');
  if (sha !== a.render.sha256 || bytes.length !== a.render.bytes) throw new Error(`local file is not the approved render (sha ${sha.slice(0, 12)}, ${bytes.length} bytes; approved ${a.render.sha256.slice(0, 12)}, ${a.render.bytes})`);
  console.log(`[news-media] local file matches the approved render: sha256 ${sha}, ${bytes.length} bytes`);
  return bytes;
}

async function main() {
  const approval = loadNewsApproval(newsTopicName(need('story-id')));
  if (cmd === 'check') checkLocal(approval, need('file'));
  else if (cmd === 'upload') {
    const bytes = checkLocal(approval, need('file'));
    const url = need('presigned-url');
    if (!/^https:\/\//.test(url)) throw new Error('presigned URL must be https');
    if (!process.argv.includes('--execute')) { console.log('[news-media] dry run: add --execute to upload'); return; }
    const res = await fetch(url, { method: 'PUT', body: new Uint8Array(bytes), headers: { 'Content-Type': 'video/mp4' } });
    if (!res.ok) throw new Error(`upload failed: HTTP ${res.status}`);
    console.log(`[news-media] uploaded ${bytes.length} bytes (HTTP ${res.status}); now run: verify --url <publicUrl>`);
  } else if (cmd === 'verify') {
    const url = need('url');
    await verifyNewsMedia({ ...approval, media_url: url });
    console.log('[news-media] public URL serves exactly the approved render (size and SHA-256 match)');
  } else throw new Error('usage: check | upload | verify');
}

main().catch(e => { console.error(`[news-media] FAILED: ${e instanceof Error ? e.message : String(e)}`); process.exitCode = 1; });
