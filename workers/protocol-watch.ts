/**
 * Protocol Watch (Protocol Intelligence, Phase 2): the one shared specification/registry watch for all catalogued
 * agent protocols. See lib/protocolWatch/engine.ts for the run contract.
 *
 * MODES
 *   PROTOCOL_WATCH_MODE=dry-run (default)  in-memory store, NO database connection at all; prints the run report.
 *   PROTOCOL_WATCH_MODE=write              writes evidence as aive_ingest. Refused unless PROTOCOL_WATCH_ENABLED
 *                                          is exactly 'true' (the workflow passes the repo variable), and preflights
 *                                          the evidence tables, naming the unapplied migration if they are absent.
 *
 * Fetches ONLY the reviewed catalogue's ALLOWED GitHub repositories and IETF drafts (config/protocol-watch-sources
 * .v1.json): no registries, no documents, no Agent Cards, no x402 endpoints, no crawling. Exits non-zero when the run
 * FAILED, so a red run is the alert; PARTIAL (some sources failed or skipped) exits 0 and is visible in the receipt.
 */
import { loadCatalogue } from '../lib/protocolWatch/catalogue';
import { WatchHttp } from '../lib/protocolWatch/http';
import { manualRunKey, runWatch, scheduleSlotKey } from '../lib/protocolWatch/engine';
import { MemoryWatchStore, PgWatchStore, type WatchStore } from '../lib/protocolWatch/store';

async function main(): Promise<number> {
  const mode = (process.env.PROTOCOL_WATCH_MODE ?? 'dry-run').trim();
  if (mode !== 'dry-run' && mode !== 'write') { console.error(`[protocol-watch] unknown PROTOCOL_WATCH_MODE=${JSON.stringify(mode)}`); return 2; }
  const catalogue = loadCatalogue();
  const now = new Date();
  const trigger = process.env.GITHUB_EVENT_NAME === 'schedule' ? 'schedule' : 'manual';
  const runKey = trigger === 'schedule' ? scheduleSlotKey(now) : manualRunKey(process.env.GITHUB_RUN_ID, now);
  let store: WatchStore;
  let close = async () => undefined as void;
  if (mode === 'write') {
    if (process.env.PROTOCOL_WATCH_ENABLED !== 'true') { console.error('[protocol-watch] write mode refused: PROTOCOL_WATCH_ENABLED is not "true" (monitoring is disabled until approved)'); return 2; }
    const db = await import('../lib/ingest/db');
    const pg = new PgWatchStore(db.q, () => db.connectWithRetry('protocol-watch'));
    await pg.preflight();
    store = pg;
    close = db.endIngestPool;
  } else {
    store = new MemoryWatchStore();
  }
  console.log(`[protocol-watch] mode=${mode} run=${runKey} catalogue=${catalogue.version.slice(0, 12)} reviewed=${catalogue.reviewedOn}`);
  try {
    const report = await runWatch({ catalogue, store, http: new WatchHttp(), runKey, trigger, log: (l) => console.log(`[protocol-watch] ${l}`) });
    const by = (k: string) => report.outcomes.filter((o) => o.outcome === k).length;
    console.log(`[protocol-watch] ${report.status} · planned ${report.planned} · blocked (not fetched) ${report.blocked} · success ${by('SUCCESS')} · failed ${by('FAILED')} · unavailable ${by('UNAVAILABLE')} · skipped ${by('SKIPPED')} · already done ${by('ALREADY_DONE')} · requests ${report.requests}${report.githubRateFloorHit ? ' · GitHub rate floor reached' : ''}`);
    if (mode === 'dry-run' && store instanceof MemoryWatchStore) {
      for (const o of store.observations.values()) console.log(`[protocol-watch] observation ${o.kind} · ${o.subjectId} · ${o.title}${o.sourcePublishedAt ? ` · source date ${o.sourcePublishedAt}` : ''}`);
      console.log('[protocol-watch] dry run: nothing was written anywhere');
    }
    return report.status === 'FAILED' ? 1 : 0;
  } finally {
    await close();
  }
}

main().then((code) => process.exit(code), (err) => { console.error(`[protocol-watch] fatal: ${(err as Error).message}`); process.exit(1); });
