import assert from 'node:assert/strict';
import {
  probeEndpoint,
  probeMcpEndpoints,
  type ProbeExecutionDependencies,
  type ProbeRow,
  type ProbeRunProvenance,
} from '../workers/probe-mcp-endpoints';

const provenance: ProbeRunProvenance = {
  scheduledFor: null,
  triggerKind: 'MANUAL',
  collectorVersion: 'fixture-sha',
  externalWorkflowRunId: 'fixture-workflow',
  externalWorkflowRunAttempt: 2,
};

function row(serverId: string, url: string, method: ProbeRow['probe_method'] = 'HEAD'): ProbeRow {
  return {
    server_id: serverId, endpoint_url: url, probed_at: '2026-09-04T12:00:00.000Z',
    http_status: 200, response_time_ms: 1, error_class: 'ok', tls_valid: true,
    redirect_target: null, note: null, probe_method: method,
    content_type: null, response_headers: null,
  };
}

function fixture(options: { runId: string; endpoints?: number; failInsertCall?: number; failRunCreation?: boolean; probeThrowAt?: number }) {
  const calls: string[] = [];
  const runStates: Array<{ state: string; values: unknown[] }> = [];
  const insertedRows: unknown[][] = [];
  let insertCall = 0;
  let probes = 0;
  const endpointCount = options.endpoints ?? 2;

  const query = (async <T>(sql: string, values: unknown[] = []): Promise<T[]> => {
    calls.push(sql);
    if (/INSERT INTO mcp_probe_runs/.test(sql)) {
      if (options.failRunCreation) throw new Error('fixture run creation failure');
      return [{ id: options.runId }] as T[];
    }
    if (/INSERT INTO scan_runs/.test(sql)) return [{ id: `legacy-${options.runId}` }] as T[];
    if (/count\(\*\).*status = 'deleted'/.test(sql)) return [{ n: '0' }] as T[];
    if (/SELECT id, remotes FROM mcp_servers/.test(sql)) {
      return Array.from({ length: endpointCount }, (_, i) => ({
        id: `00000000-0000-4000-8000-${String(i).padStart(12, '0')}`,
        remotes: [{ url: `https://fixture-${i}.invalid/mcp` }],
      })) as T[];
    }
    if (/UPDATE mcp_probe_runs/.test(sql)) {
      runStates.push({ state: String(values[1]), values: [...values] });
      return [];
    }
    if (/UPDATE scan_runs/.test(sql)) return [];
    throw new Error(`unexpected fixture query: ${sql}`);
  }) as ProbeExecutionDependencies['query'];

  const insert: ProbeExecutionDependencies['insert'] = async (_table, _columns, rows) => {
    insertCall++;
    if (options.failInsertCall === insertCall) throw new Error('fixture persistence failure');
    insertedRows.push(...rows);
  };
  const probe: ProbeExecutionDependencies['probe'] = async (serverId, url) => {
    probes++;
    if (options.probeThrowAt === probes) throw new Error('fixture collector abort');
    return row(serverId, url);
  };
  const dependencies: ProbeExecutionDependencies = {
    query, insert, probe, now: () => new Date('2026-09-04T12:00:00.000Z'),
  };
  return { dependencies, calls, runStates, insertedRows, get probes() { return probes; } };
}

async function main() {
  const normal = fixture({ runId: 'run-one' });
  const result = await probeMcpEndpoints(normal.dependencies, provenance);
  assert.equal(result.probeRunId, 'run-one');
  assert.equal(normal.probes, 2);
  assert.equal(normal.insertedRows.length, 2);
  assert.ok(normal.insertedRows.every(values => values.at(-2) === 'run-one' && values.at(-1) === 'REACHABILITY'));
  assert.equal(normal.runStates.at(-1)?.state, 'COMPLETE');
  assert.deepEqual(normal.runStates.at(-1)?.values.slice(2, 10), [2, 2, 2, 2, 2, 0, 0, 0]);
  assert.ok(normal.calls.findIndex(sql => /INSERT INTO mcp_probe_runs/.test(sql)) < normal.calls.findIndex(sql => /SELECT id, remotes/.test(sql)));

  const second = fixture({ runId: 'run-two' });
  assert.equal((await probeMcpEndpoints(second.dependencies, provenance)).probeRunId, 'run-two');
  assert.notEqual(result.probeRunId, 'run-two', 'separate invocation/workflow rerun has a separate durable run');

  const failedOpen = fixture({ runId: 'never', failRunCreation: true });
  await assert.rejects(() => probeMcpEndpoints(failedOpen.dependencies, provenance), /run creation failure/);
  assert.equal(failedOpen.probes, 0, 'run creation failure prevents all probing');
  assert.equal(failedOpen.calls.some(sql => /SELECT id, remotes/.test(sql)), false, 'run creation failure prevents endpoint selection');

  const partial = fixture({ runId: 'run-partial', endpoints: 501, failInsertCall: 2 });
  const partialResult = await probeMcpEndpoints(partial.dependencies, provenance);
  assert.equal(partialResult.errors, 1);
  assert.equal(partial.runStates.at(-1)?.state, 'PARTIAL');
  assert.deepEqual(partial.runStates.at(-1)?.values.slice(2, 10), [501, 501, 501, 501, 500, 0, 1, 0]);

  const aborted = fixture({ runId: 'run-aborted', endpoints: 2, probeThrowAt: 2 });
  await assert.rejects(() => probeMcpEndpoints(aborted.dependencies, provenance), /collector abort/);
  assert.equal(aborted.runStates.at(-1)?.state, 'FAILED');
  assert.deepEqual(aborted.runStates.at(-1)?.values.slice(2, 10), [2, 2, 1, 1, 0, 0, 1, 0]);

  const originalFetch = globalThis.fetch;
  const methods: string[] = [];
  globalThis.fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
    methods.push(String(init?.method));
    return new Response(null, { status: methods.length === 1 ? 405 : 200 });
  }) as typeof fetch;
  try {
    const fallback = await probeEndpoint('server', 'https://fallback.invalid/mcp');
    assert.deepEqual(methods, ['HEAD', 'GET']);
    assert.equal(fallback.probe_method, 'GET');
    assert.equal(fallback.error_class, 'ok');
  } finally {
    globalThis.fetch = originalFetch;
  }

  console.log('MCP probe-run evidence worker contract: passed');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
