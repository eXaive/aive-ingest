// The generic (TikTok-era) reconciler must never process kids YouTube distribution attempts.
// Pure by default; with RECONCILE_SCOPE_TEST_DATABASE_URL (loopback only) it also proves the predicate in PostgreSQL.
import assert from 'node:assert/strict';
import { Pool } from 'pg';
import { RECONCILE_CANDIDATES_SQL } from '../workers/reconcile-broadcast-publications';

async function main() {
  assert.match(RECONCILE_CANDIDATES_SQL, /and not \(source_kind='broadcast_content_media' and purpose='editorial-video'\)/);
  assert.match(RECONCILE_CANDIDATES_SQL, /state in \('provider_accepted','confirmation_pending'\)/);
  const url = process.env.RECONCILE_SCOPE_TEST_DATABASE_URL;
  if (url) {
    if (!/@(127\.0\.0\.1|localhost):\d+\//.test(url)) throw new Error('refusing non-loopback RECONCILE_SCOPE_TEST_DATABASE_URL');
    const pool = new Pool({ connectionString: url });
    try {
      await pool.query(`create temporary table broadcast_publication_attempts(id text, provider_submission_id text, state text, source_kind text, purpose text, last_confirmation_at timestamptz, created_at timestamptz default now())`);
      await pool.query(`insert into broadcast_publication_attempts(id,provider_submission_id,state,source_kind,purpose) values
        ('tiktok-quiz','s1','provider_accepted','quiz_question','quiz-card'),('tiktok-text','s2','confirmation_pending','broadcast_topic','text-card'),
        ('tiktok-daily','s3','provider_accepted','broadcast_topic','daily-video'),('kids-youtube','s4','provider_accepted','broadcast_content_media','editorial-video'),
        ('kids-pending','s5','confirmation_pending','broadcast_content_media','editorial-video'),('done','s6','confirmed_published','quiz_question','quiz-card')`);
      const ids = (await pool.query(RECONCILE_CANDIDATES_SQL)).rows.map(r => r.id).sort();
      assert.deepEqual(ids, ['tiktok-daily', 'tiktok-quiz', 'tiktok-text']);
      console.log('PASS reconcile scope (PostgreSQL): TikTok quiz/text/daily attempts reconciled; kids editorial-video attempts excluded');
    } finally { await pool.end(); }
  }
  console.log('PASS reconcile scope: generic reconciler excludes broadcast_content_media editorial-video attempts');
}
main().catch(e => { console.error(e); process.exit(1); });
