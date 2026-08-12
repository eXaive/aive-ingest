/**
 * lib/mcp/exclusions.ts -- operator opt-outs, shared by both MCP collectors.
 *
 * SHARED, NOT DUPLICATED, and deliberately so. The dispatch loop in
 * collect-mcp-tools.ts restates runSweep's logic because runSweep's callbacks are
 * typed to DiscoverRow, and that duplication is now a filed forge todo precisely
 * because a fix to one will not reach the other. Exclusion matching must not
 * repeat that mistake: an opt-out honoured by one collector and missed by the
 * other is worse than no mechanism at all, because the operator was told it was
 * handled. One loadExclusions, one isExcluded, imported by both workers.
 *
 * WHAT THE DISCLOSURE PAGE PROMISES, verbatim:
 *   "Email founder@aive.global with your endpoint URL or hostname. We will:
 *    1. Stop sending requests to it, usually within one business day.
 *    2. Add it to a documented exclusion list.
 *    3. Record it as excluded in our published figures rather than as a gap, so
 *       the record stays honest about what it does and does not cover."
 *
 * This module implements 1 and reads 2. Promise 3 lives in the callers: an
 * excluded target still gets a row written, marked excluded, so it is reported
 * rather than silently dropped from the denominator.
 *
 * FAIL CLOSED. loadExclusions THROWS if the table cannot be read. A worker that
 * cannot see the exclusion list must not dial: skipping a sweep costs one day of
 * data, while dialling an endpoint whose operator asked us to stop breaks a
 * published commitment and cannot be undone. That asymmetry is the whole argument
 * and it is not close.
 *
 * MATCHING IS EXACT. No LIKE, no regex, no prefix logic. A pattern language would
 * eventually exclude something nobody asked to exclude, and the failure mode --
 * quietly not measuring an endpoint that never opted out -- looks identical to
 * coverage loss. Hostnames are compared lowercased because DNS is
 * case-insensitive; URLs and server ids are compared byte-for-byte.
 */

/**
 * The narrowest thing that can answer the question. Deliberately NOT `pg.Pool`:
 * a structural type means the verify script can pass a double that counts calls
 * and throws on demand, so the fail-closed path is DEMONSTRATED against the
 * shipping function rather than re-implemented in a test. A real Pool satisfies
 * this, so nothing is lost at the call site.
 */
export interface ExclusionReader {
  query: (text: string) => Promise<{ rows: unknown[] }>;
}

/** One active exclusion row, as the workers need it. */
export interface Exclusion {
  scope: 'endpoint' | 'host' | 'server';
  pattern: string;
  applies_to: 'all' | 'discover' | 'tools' | 'resources' | 'prompts';
}

/**
 * Which collector is asking. Matches mcp_exclusions.applies_to, widened by
 * aive-platform migration 20260813000001 for the two listing methods.
 *
 * WIDENING IS ADDITIVE AND SAFE IN BOTH DIRECTIONS. A row reading 'all' now
 * expands to four methods rather than two, so an operator who asked us to stop
 * entirely is opted out of the new methods WITHOUT having to ask again -- which
 * is the only acceptable default, since they cannot have known to ask about a
 * method that did not exist. In the other direction, a worker running older
 * code that meets a row saying 'resources' falls into methodsFor's unrecognised
 * branch and honours it across every method it knows: broader than intended,
 * never narrower.
 */
export type Method = 'discover' | 'tools' | 'resources' | 'prompts';

/**
 * The loaded set, pre-indexed. Three maps rather than one list so matching is a
 * constant-time lookup per target instead of a scan per target -- a sweep asks
 * this question 11,109 times and the answer must not get slower as the list
 * grows.
 */
export interface ExclusionSet {
  endpoints: Map<string, Set<Method>>;
  hosts: Map<string, Set<Method>>;
  servers: Map<string, Set<Method>>;
  /** Active row count, for the log line. */
  count: number;
}

/** A target either collector can ask about. */
export interface ExclusionTarget {
  url: string;
  host: string;
  serverId: string;
}

const METHODS: Method[] = ['discover', 'tools', 'resources', 'prompts'];

/** Expand applies_to into the concrete methods it covers. */
function methodsFor(appliesTo: string): Method[] {
  if (appliesTo === 'all') return METHODS;
  if (
    appliesTo === 'discover' || appliesTo === 'tools' ||
    appliesTo === 'resources' || appliesTo === 'prompts'
  ) return [appliesTo];
  // An unrecognised value is treated as covering EVERYTHING. If someone adds a
  // value to the CHECK constraint and forgets this file, the safe reading of
  // "we do not understand this exclusion" is to honour it broadly rather than
  // ignore it.
  return METHODS;
}

const add = (m: Map<string, Set<Method>>, key: string, appliesTo: string): void => {
  const set = m.get(key) ?? new Set<Method>();
  for (const method of methodsFor(appliesTo)) set.add(method);
  m.set(key, set);
};

/**
 * Load every ACTIVE exclusion. Throws on any read failure -- see FAIL CLOSED.
 *
 * `db.query` is used directly rather than the workers' q() helper so this module
 * has no dependency on either worker's plumbing and can be imported by both
 * without a cycle.
 *
 * "Active" is two predicates, not one. revoked_at IS NULL is the withdrawal
 * check; effective_at <= now() is what makes a future-dated exclusion inert
 * until its date arrives -- and both must be here rather than at the call site,
 * or one worker will eventually apply a different definition of active than the
 * other.
 */
export async function loadExclusions(db: ExclusionReader): Promise<ExclusionSet> {
  let rows: Exclusion[];
  try {
    const res = await db.query(
      `SELECT scope, pattern, applies_to
         FROM mcp_exclusions
        WHERE revoked_at IS NULL
          AND effective_at <= now()`,
    );
    rows = res.rows as Exclusion[];
  } catch (err) {
    // Deliberately not a warning-and-continue. The message names the consequence
    // so an operator reading CI output knows why the sweep did not run.
    throw new Error(
      'REFUSING TO SWEEP: mcp_exclusions is unreadable, so operator opt-outs ' +
      'cannot be honoured. Dialling an endpoint whose operator asked us to stop ' +
      'breaks a published commitment and cannot be undone; skipping a sweep costs ' +
      'one day of data. Underlying error: ' +
      (err instanceof Error ? err.message : String(err)),
    );
  }

  const set: ExclusionSet = {
    endpoints: new Map(), hosts: new Map(), servers: new Map(), count: rows.length,
  };
  for (const r of rows) {
    if (r.scope === 'endpoint') add(set.endpoints, r.pattern, r.applies_to);
    else if (r.scope === 'host') add(set.hosts, r.pattern.toLowerCase(), r.applies_to);
    else if (r.scope === 'server') add(set.servers, r.pattern, r.applies_to);
    // An unknown scope is ignored rather than guessed at: unlike applies_to,
    // there is no safe broad reading of a scope we cannot interpret, and a
    // mis-scoped match would exclude the wrong thing.
  }
  return set;
}

/**
 * Is this target excluded for this method? Returns the matching rule so the
 * caller can record WHICH exclusion applied, not merely that one did -- promise 2
 * is a documented list, and a row saying "excluded" without saying by what is not
 * documentation.
 */
export function isExcluded(
  target: ExclusionTarget,
  set: ExclusionSet,
  method: Method,
): { excluded: true; scope: Exclusion['scope']; pattern: string } | { excluded: false } {
  const url = set.endpoints.get(target.url);
  if (url?.has(method)) return { excluded: true, scope: 'endpoint', pattern: target.url };

  const host = set.hosts.get(target.host.toLowerCase());
  if (host?.has(method)) return { excluded: true, scope: 'host', pattern: target.host.toLowerCase() };

  const server = set.servers.get(target.serverId);
  if (server?.has(method)) return { excluded: true, scope: 'server', pattern: target.serverId };

  return { excluded: false };
}

/** The note text both workers write, so the string is identical in both tables. */
export function exclusionNote(match: { scope: string; pattern: string }): string {
  return `excluded: operator opt-out matched ${match.scope}=${match.pattern}`;
}
