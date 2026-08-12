/**
 * collect-mcp-prompts.ts -- one prompts/list POST per endpoint that answered
 * server/discover on the current specification, plus cursor follow-ups.
 *
 * THE DISCLOSURE GATE. This worker could not be written until
 * https://aive.global/mcp-trust/census disclosed the method. The page was
 * amended FIRST (aive-platform cc4684d, verified live: 200, no redirect, closed
 * set of four, this worker's agent string rendered, and a note saying the method
 * was not yet in service). When this collector ships, that note is deleted.
 *
 * A LISTING IS NOT A RENDER. prompts/list returns each prompt's name, title,
 * description and DECLARED ARGUMENTS. It does not supply arguments and does not
 * ask the server to produce a result: prompts/get is never sent by this worker
 * or any other. The page states that; this file honours it; grep enforces it.
 *
 * NO initialize. NO session header sent or read. NO Authorization, no cookies.
 * Mcp-Name is required only for tools/call, resources/read and prompts/get, none
 * of which this worker issues, so it is not sent.
 *
 * The dispatch loop, pacing, exclusion handling and insert path all live in
 * lib/mcp/listCollector.ts, SHARED with collect-mcp-resources.ts. This file is
 * the spec and nothing else.
 *
 * Env: AIVE_INGEST_DATABASE_URL. Optional: PROMPTS_LIMIT (cap endpoints for a
 * smoke run), PROMPTS_CONCURRENCY (default 8), PROMPTS_DEADLINE_MINUTES
 * (default 60).
 */

import { endIngestPool } from '../lib/ingest/db';
import { runListCollector, type ListSpec } from '../lib/mcp/listCollector';

/**
 * BYTE-IDENTICAL to PROMPTS_USER_AGENT in eXaive/aive-platform
 * app/mcp-trust/census/page.tsx.
 *
 * NOT RETYPED -- lifted from the page's own bytes, with the byte equality
 * re-asserted by scripts/verify-list-collectors.ts against both the page source
 * and the served HTML. See the note in collect-mcp-resources.ts for why this
 * matters; the argument is identical.
 *
 * sha256(utf8) = 6f881fecc6956a56167349edcaae65b1a85e944689de83cb9eff4e1687fd8ead
 */
export const USER_AGENT =
  'AIVE-MCP-Prompts/1.0 (+https://aive.global/mcp-trust/census; one prompts/list POST per current-spec endpoint, no auth attempted, no prompt ever rendered)';

const str = (v: unknown): string | null => (typeof v === 'string' ? v : null);

export const PROMPTS_SPEC: ListSpec = {
  method: 'prompts/list',
  exclusionMethod: 'prompts',
  userAgent: USER_AGENT,
  sourceSlug: 'mcp-prompts',
  envPrefix: 'PROMPTS',
  resultKey: 'prompts',
  captureTable: 'mcp_prompt_captures',
  countColumn: 'prompt_count',
  itemTable: 'mcp_prompts',
  itemColumns: ['name', 'title', 'description', 'arguments', 'argument_count', 'required_count', 'raw', 'token_estimate'],
  jsonColumns: new Set(['arguments', 'raw']),
  /**
   * argument_count and required_count are derived here rather than in SQL so the
   * numbers are computed once, at capture time, from the bytes the server sent.
   * "Did this prompt gain a required argument" is the closest analogue a prompt
   * declaration has to a breaking change, and it should not require parsing
   * jsonb in every query that asks.
   *
   * A non-array arguments field yields NULL counts rather than 0: zero
   * arguments and an unparseable declaration are different facts, and 0 would
   * quietly assert the first.
   */
  itemToRow: (o: any) => {
    const raw = JSON.stringify(o ?? null);
    const args = Array.isArray(o?.arguments) ? o.arguments : null;
    return [
      str(o?.name) ?? '',
      str(o?.title),
      str(o?.description),
      args,
      args === null ? null : args.length,
      args === null ? null : args.filter((a: any) => a?.required === true).length,
      o ?? null,
      Math.ceil(Buffer.byteLength(raw, 'utf8') / 4),
    ];
  },
  tag: '[collect-mcp-prompts]',
};

export async function collectMcpPrompts() {
  return runListCollector(PROMPTS_SPEC);
}

if (require.main === module) {
  collectMcpPrompts()
    .then(async (r) => {
      await endIngestPool();
      if (r.captures === 0) {
        console.error(
          `[collect-mcp-prompts] FAILED: run completed having written ZERO captures ` +
          `(attempted=${r.attempted}, insert errors=${r.errors}).`,
        );
        process.exit(1);
      }
      process.exit(r.errors > 0 ? 1 : 0);
    })
    .catch(async (e) => { console.error(e); await endIngestPool(); process.exit(1); });
}
