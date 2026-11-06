/**
 * Protocol watch catalogue loader (Protocol Intelligence, Phase 2).
 *
 * config/protocol-watch-sources.v1.json is EXPORTED from the platform's lib/protocols/protocolWatchCatalogue.ts
 * (scripts/export-protocol-watch-catalogue.ts). It is the engine's complete allowlist: nothing outside it is fetched.
 * The file must hash to its own `version` (sha256 of {schema, reviewedOn, sources}), so a hand edit that was not
 * re-exported from the reviewed platform catalogue is refused.
 *
 * Only gate === 'ALLOWED' sources are ever contacted. Every other gate (terms not reviewed, credentials required,
 * Phase 3, covered by an existing collector) is reported as skipped and never fetched.
 */
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

export type WatchMethod = 'GITHUB_REPOSITORY' | 'IETF_DRAFT' | 'REGISTRY_API' | 'DOCUMENT';
export type AutomationGate = 'ALLOWED' | 'BLOCKED_TERMS_NOT_REVIEWED' | 'BLOCKED_CREDENTIALS_REQUIRED' | 'COVERED_BY_EXISTING_COLLECTOR' | 'BLOCKED_PHASE3';
export interface CatalogueSource { sourceId: string; protocolId: string; method: WatchMethod; target: string; gate: AutomationGate; freshnessHours: number }
export interface WatchCatalogue { schema: 'aive.protocol-watch-sources.v1'; reviewedOn: string; version: string; sources: CatalogueSource[] }

export const CATALOGUE_PATH = join(__dirname, '..', '..', 'config', 'protocol-watch-sources.v1.json');
/** The only methods Phase 2 may automate. REGISTRY_API and DOCUMENT are catalogued but never automated here. */
export const AUTOMATABLE_METHODS: ReadonlySet<WatchMethod> = new Set(['GITHUB_REPOSITORY', 'IETF_DRAFT']);
const METHODS = new Set<WatchMethod>(['GITHUB_REPOSITORY', 'IETF_DRAFT', 'REGISTRY_API', 'DOCUMENT']);
const GATES = new Set<AutomationGate>(['ALLOWED', 'BLOCKED_TERMS_NOT_REVIEWED', 'BLOCKED_CREDENTIALS_REQUIRED', 'COVERED_BY_EXISTING_COLLECTOR', 'BLOCKED_PHASE3']);
const GH_REPO = /^[A-Za-z0-9-]{1,39}\/[A-Za-z0-9._-]{1,100}$/;
const IETF_DRAFT = /^draft-[a-z0-9-]{3,120}$/;

export function catalogueVersionOf(body: Pick<WatchCatalogue, 'schema' | 'reviewedOn' | 'sources'>): string {
  return createHash('sha256').update(JSON.stringify({ schema: body.schema, reviewedOn: body.reviewedOn, sources: body.sources })).digest('hex');
}

export function parseCatalogue(raw: unknown): WatchCatalogue {
  const c = raw as WatchCatalogue;
  if (!c || c.schema !== 'aive.protocol-watch-sources.v1') throw new Error('protocol-watch catalogue: unknown schema');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(c.reviewedOn ?? '')) throw new Error('protocol-watch catalogue: reviewedOn missing');
  if (!Array.isArray(c.sources) || c.sources.length === 0 || c.sources.length > 500) throw new Error('protocol-watch catalogue: sources missing or too many');
  const seen = new Set<string>();
  for (const s of c.sources) {
    if (!s || typeof s.sourceId !== 'string' || seen.has(s.sourceId)) throw new Error(`protocol-watch catalogue: bad or duplicate sourceId ${String(s?.sourceId)}`);
    seen.add(s.sourceId);
    if (!METHODS.has(s.method) || !GATES.has(s.gate)) throw new Error(`protocol-watch catalogue: ${s.sourceId} has an unknown method or gate`);
    if (!/^[a-z0-9-]{1,60}$/.test(s.protocolId)) throw new Error(`protocol-watch catalogue: ${s.sourceId} protocolId`);
    if (!Number.isInteger(s.freshnessHours) || s.freshnessHours < 1 || s.freshnessHours > 24 * 90) throw new Error(`protocol-watch catalogue: ${s.sourceId} freshnessHours`);
    if (s.gate === 'ALLOWED') {
      if (!AUTOMATABLE_METHODS.has(s.method)) throw new Error(`protocol-watch catalogue: ${s.sourceId} is ALLOWED but ${s.method} cannot be automated in Phase 2`);
      if (s.method === 'GITHUB_REPOSITORY' && (!GH_REPO.test(s.target) || s.sourceId !== `github:${s.target.toLowerCase()}`)) throw new Error(`protocol-watch catalogue: ${s.sourceId} target`);
      if (s.method === 'IETF_DRAFT' && (!IETF_DRAFT.test(s.target) || s.sourceId !== `ietf:${s.target}`)) throw new Error(`protocol-watch catalogue: ${s.sourceId} target`);
    }
  }
  if (catalogueVersionOf(c) !== c.version) throw new Error('protocol-watch catalogue: content does not match its version (re-export it from the platform catalogue)');
  return c;
}

export function loadCatalogue(path = CATALOGUE_PATH): WatchCatalogue {
  return parseCatalogue(JSON.parse(readFileSync(path, 'utf8')));
}

export function automatedSources(c: WatchCatalogue): CatalogueSource[] {
  return c.sources.filter((s) => s.gate === 'ALLOWED' && AUTOMATABLE_METHODS.has(s.method));
}
