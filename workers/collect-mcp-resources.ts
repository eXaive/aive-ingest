/**
 * collect-mcp-resources.ts -- one resources/list POST per endpoint that answered
 * server/discover on the current specification, plus cursor follow-ups.
 *
 * THE DISCLOSURE GATE. This worker could not be written until
 * https://aive.global/mcp-trust/census disclosed the method. The page was
 * amended FIRST (aive-platform cc4684d, verified live: 200, no redirect, closed
 * set of four, this worker's agent string rendered, and a note saying the method
 * was not yet in service). When this collector ships, that note is deleted.
 *
 * A LISTING IS NOT AN ACCESS. resources/list returns the URI, name, description
 * and MIME type of each resource. It does NOT return contents, and
 * resources/read -- the method that would -- is never sent by this worker or any
 * other. The page states that; this file honours it; grep enforces it.
 *
 * NO initialize. NO session header sent or read. NO Authorization, no cookies.
 * Mcp-Name is required only for tools/call, resources/read and prompts/get, none
 * of which this worker issues, so it is not sent.
 *
 * The dispatch loop, pacing, exclusion handling and insert path all live in
 * lib/mcp/listCollector.ts, SHARED with collect-mcp-prompts.ts. This file is the
 * spec and nothing else.
 *
 * Env: AIVE_INGEST_DATABASE_URL. Optional: RESOURCES_LIMIT (cap endpoints for a
 * smoke run), RESOURCES_CONCURRENCY (default 8), RESOURCES_DEADLINE_MINUTES
 * (default 60).
 */

import { endIngestPool } from '../lib/ingest/db';
import { runListCollector, type ListSpec } from '../lib/mcp/listCollector';

/**
 * BYTE-IDENTICAL to RESOURCES_USER_AGENT in eXaive/aive-platform
 * app/mcp-trust/census/page.tsx.
 *
 * NOT RETYPED. This value was lifted from the page's own bytes and the byte
 * equality is re-asserted by scripts/verify-list-collectors.ts against both the
 * page source and the served HTML. Retyping is how the two copies drift, and a
 * drifted agent string means the page describes a collector that is not the one
 * calling the operator -- which breaks the round trip the URL inside it exists
 * to serve. If either changes, change both in the same pass.
 *
 * sha256(utf8) = 8aa9bd7b1bc759a3c77a758e0aa54dd9dbe69731f388a764c95f7043378312f9
 */
export const USER_AGENT =
  'AIVE-MCP-Resources/1.0 (+https://aive.global/mcp-trust/census; one resources/list POST per current-spec endpoint, no auth attempted, no resource ever read)';

const str = (v: unknown): string | null => (typeof v === 'string' ? v : null);

export const RESOURCES_SPEC: ListSpec = {
  method: 'resources/list',
  exclusionMethod: 'resources',
  userAgent: USER_AGENT,
  sourceSlug: 'mcp-resources',
  envPrefix: 'RESOURCES',
  resultKey: 'resources',
  captureTable: 'mcp_resource_captures',
  countColumn: 'resource_count',
  itemTable: 'mcp_resources',
  itemColumns: ['uri', 'name', 'title', 'description', 'mime_type', 'annotations', 'raw', 'token_estimate'],
  jsonColumns: new Set(['annotations', 'raw']),
  /**
   * Identity is uri, not name: a Resource is identified by URI, and two
   * resources may legitimately share a name. An object with no uri is kept with
   * an empty string rather than dropped, so a malformed declaration is visible
   * in the corpus instead of silently reducing the count.
   */
  itemToRow: (o: any) => {
    const raw = JSON.stringify(o ?? null);
    return [
      str(o?.uri) ?? '',
      str(o?.name),
      str(o?.title),
      str(o?.description),
      str(o?.mimeType),
      o?.annotations ?? null,
      o ?? null,
      Math.ceil(Buffer.byteLength(raw, 'utf8') / 4),
    ];
  },
  tag: '[collect-mcp-resources]',
};

export async function collectMcpResources() {
  return runListCollector(RESOURCES_SPEC);
}

if (require.main === module) {
  collectMcpResources()
    .then(async (r) => {
      await endIngestPool();
      // NO HOLLOW SUCCESS. Every outcome is a capture row, so zero captures
      // means the write path never worked.
      if (r.captures === 0) {
        console.error(
          `[collect-mcp-resources] FAILED: run completed having written ZERO captures ` +
          `(attempted=${r.attempted}, insert errors=${r.errors}).`,
        );
        process.exit(1);
      }
      process.exit(r.errors > 0 ? 1 : 0);
    })
    .catch(async (e) => { console.error(e); await endIngestPool(); process.exit(1); });
}
