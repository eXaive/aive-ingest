/**
 * Protocol watch collectors (Phase 2): GitHub repository state/releases/tags and IETF Internet-Draft revisions.
 *
 * Each collector returns a NORMALISED STATE (only fields that matter; volatile counters such as stars, forks,
 * open issues and pushed_at are deliberately excluded so the history is write-on-change, not write-every-run) and
 * the observations implied by comparing it with the previous state.
 *
 * Release vs tag: a GitHub Release (non-draft) in a catalogued official repository is RELEASE_PUBLISHED; a git tag
 * with no matching release is only TAG_CREATED. Neither is described as an official protocol release beyond that.
 * A first observation (no previous state) records a baseline: the newest release and the current draft revision only,
 * never the whole back catalogue.
 */
import { createHash } from 'node:crypto';
import type { CatalogueSource } from './catalogue';
import type { WatchHttp } from './http';

export type ObservationKind = 'REPOSITORY_STATE' | 'RELEASE_PUBLISHED' | 'TAG_CREATED' | 'DRAFT_REVISION' | 'REPOSITORY_ARCHIVED' | 'REPOSITORY_RELOCATED';
export interface Observation { observationId: string; kind: ObservationKind; title: string; url: string | null; sourcePublishedAt: string | null; detail: Record<string, unknown> }
export type CollectResult =
  | { outcome: 'SUCCESS'; state: Record<string, unknown>; stateHash: string; observations: Observation[]; httpStatus: number }
  | { outcome: 'UNAVAILABLE'; httpStatus: number; errorCode: string };

const MAX_RELEASES = 10;
const MAX_TAGS = 20;

export function stableStringify(v: unknown): string {
  if (Array.isArray(v)) return `[${v.map(stableStringify).join(',')}]`;
  if (v && typeof v === 'object') return `{${Object.keys(v as object).sort().map((k) => `${JSON.stringify(k)}:${stableStringify((v as Record<string, unknown>)[k])}`).join(',')}}`;
  return JSON.stringify(v ?? null);
}
export const sha256 = (s: string) => createHash('sha256').update(s).digest('hex');
export const hashState = (state: Record<string, unknown>) => sha256(stableStringify(state));
const obsId = (sourceId: string, kind: ObservationKind, key: string) => sha256(`${sourceId}\u0000${kind}\u0000${key}`);
const httpsOrNull = (u: unknown) => (typeof u === 'string' && /^https:\/\//.test(u) && u.length <= 600 ? u : null);
/** Only timestamps that state their zone; an ambiguous local time is recorded as unknown rather than guessed. */
const isoOrNull = (u: unknown) => (typeof u === 'string' && /(Z|[+-]\d{2}:?\d{2})$/.test(u) && Number.isFinite(Date.parse(u)) ? new Date(u).toISOString() : null);
const clip = (s: unknown, n = 300) => (typeof s === 'string' && s.trim() ? s.trim().slice(0, n) : null);

function githubHeaders(): Record<string, string> {
  const h: Record<string, string> = { Accept: 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28' };
  if (process.env.GITHUB_TOKEN) h.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
  return h;
}

interface GhRepo { full_name?: string; html_url?: string; archived?: boolean; disabled?: boolean; default_branch?: string; license?: { spdx_id?: string } | null; visibility?: string }
interface GhRelease { id?: number; tag_name?: string; name?: string | null; html_url?: string; draft?: boolean; prerelease?: boolean; published_at?: string | null }
interface GhTag { name?: string }

export async function collectGithub(source: CatalogueSource, http: WatchHttp, previous: Record<string, unknown> | null): Promise<CollectResult> {
  const base = `https://api.github.com/repos/${source.target}`;
  const repo = await http.getJson<GhRepo>(base, githubHeaders());
  if (!repo.body) return { outcome: 'UNAVAILABLE', httpStatus: repo.status, errorCode: `HTTP_${repo.status}` };
  const rel = await http.getJson<GhRelease[]>(`${base}/releases?per_page=${MAX_RELEASES}`, githubHeaders());
  const tags = await http.getJson<GhTag[]>(`${base}/tags?per_page=${MAX_TAGS}`, githubHeaders());
  const releases = (Array.isArray(rel.body) ? rel.body : [])
    .filter((r) => r && r.draft !== true && typeof r.tag_name === 'string' && typeof r.id === 'number')
    .slice(0, MAX_RELEASES)
    .map((r) => ({ id: r.id!, tag: r.tag_name!.slice(0, 200), name: clip(r.name, 200), prerelease: r.prerelease === true, publishedAt: isoOrNull(r.published_at), url: httpsOrNull(r.html_url) }));
  const tagNames = (Array.isArray(tags.body) ? tags.body : []).map((t) => (typeof t?.name === 'string' ? t.name.slice(0, 200) : null)).filter((t): t is string => !!t).slice(0, MAX_TAGS);
  const state = {
    kind: 'github_repository',
    fullName: typeof repo.body.full_name === 'string' ? repo.body.full_name : source.target,
    archived: repo.body.archived === true,
    disabled: repo.body.disabled === true,
    defaultBranch: clip(repo.body.default_branch, 100),
    license: clip(repo.body.license?.spdx_id, 60),
    releases: releases.map(({ id, tag, name, prerelease, publishedAt }) => ({ id, tag, name, prerelease, publishedAt })),
    tags: tagNames,
  };
  const observations: Observation[] = [];
  const prevReleases = new Set(Array.isArray(previous?.releases) ? (previous!.releases as { id: number }[]).map((r) => r.id) : []);
  const prevTags = new Set(Array.isArray(previous?.tags) ? (previous!.tags as string[]) : []);
  const releaseTags = new Set(releases.map((r) => r.tag));
  const newReleases = previous ? releases.filter((r) => !prevReleases.has(r.id)) : releases.slice(0, 1);
  for (const r of newReleases) {
    observations.push({
      observationId: obsId(source.sourceId, 'RELEASE_PUBLISHED', String(r.id)), kind: 'RELEASE_PUBLISHED', title: (r.name ?? r.tag).slice(0, 300),
      url: r.url, sourcePublishedAt: r.publishedAt, detail: { tag: r.tag, prerelease: r.prerelease, baseline: !previous },
    });
  }
  if (previous) {
    for (const t of tagNames) {
      if (!prevTags.has(t) && !releaseTags.has(t)) {
        observations.push({ observationId: obsId(source.sourceId, 'TAG_CREATED', t), kind: 'TAG_CREATED', title: t, url: `https://github.com/${state.fullName}/tree/${encodeURIComponent(t)}`, sourcePublishedAt: null, detail: { note: 'git tag without a GitHub Release; not necessarily an official release' } });
      }
    }
    if (previous.archived === false && state.archived) observations.push({ observationId: obsId(source.sourceId, 'REPOSITORY_ARCHIVED', state.fullName), kind: 'REPOSITORY_ARCHIVED', title: `${state.fullName} archived`, url: `https://github.com/${state.fullName}`, sourcePublishedAt: null, detail: {} });
    if (typeof previous.fullName === 'string' && previous.fullName.toLowerCase() !== state.fullName.toLowerCase()) {
      observations.push({ observationId: obsId(source.sourceId, 'REPOSITORY_RELOCATED', `${previous.fullName}->${state.fullName}`), kind: 'REPOSITORY_RELOCATED', title: `${previous.fullName} moved to ${state.fullName}`, url: `https://github.com/${state.fullName}`, sourcePublishedAt: null, detail: { from: previous.fullName, to: state.fullName } });
    }
  }
  return { outcome: 'SUCCESS', state, stateHash: hashState(state), observations, httpStatus: repo.status };
}

interface IetfDoc { name?: string; rev?: string; title?: string; time?: string; expires?: string; abstract?: string }

export async function collectIetfDraft(source: CatalogueSource, http: WatchHttp, previous: Record<string, unknown> | null): Promise<CollectResult> {
  const res = await http.getJson<IetfDoc>(`https://datatracker.ietf.org/api/v1/doc/document/${source.target}/?format=json`);
  if (!res.body) return { outcome: 'UNAVAILABLE', httpStatus: res.status, errorCode: `HTTP_${res.status}` };
  const rev = clip(res.body.rev, 10);
  if (!rev || res.body.name !== source.target) return { outcome: 'UNAVAILABLE', httpStatus: res.status, errorCode: 'UNEXPECTED_DOCUMENT' };
  const state = { kind: 'ietf_draft', name: source.target, rev, title: clip(res.body.title), revisedAt: isoOrNull(res.body.time), expires: isoOrNull(res.body.expires) };
  const observations: Observation[] = [];
  if (!previous || previous.rev !== rev) {
    observations.push({
      observationId: obsId(source.sourceId, 'DRAFT_REVISION', rev), kind: 'DRAFT_REVISION', title: `${source.target}-${rev}`,
      url: `https://datatracker.ietf.org/doc/${source.target}/${rev}/`, sourcePublishedAt: state.revisedAt, detail: { previousRev: previous?.rev ?? null, baseline: !previous, note: 'An Internet-Draft is a work in progress, not a standard.' },
    });
  }
  return { outcome: 'SUCCESS', state, stateHash: hashState(state), observations, httpStatus: res.status };
}

export function collectorFor(source: CatalogueSource) {
  if (source.method === 'GITHUB_REPOSITORY') return collectGithub;
  if (source.method === 'IETF_DRAFT') return collectIetfDraft;
  return null;
}
