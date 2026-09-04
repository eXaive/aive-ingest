import assert from 'node:assert/strict';
import {
  beginToolCollection,
  collectOnce,
  persistCaptureAtomically,
  updateToolCollectionRun,
  type CaptureRow,
  type ToolRunProvenance,
} from '../workers/collect-mcp-tools';

const provenance: ToolRunProvenance = {
  scheduledFor: null,
  triggerKind: 'MANUAL',
  collectorVersion: 'fixture-sha',
  externalWorkflowRunId: 'fixture-workflow',
  externalWorkflowRunAttempt: 1,
};

function runQuery(ids: string[], calls: string[]) {
  return (async <T>(sql: string): Promise<T[]> => {
    calls.push(sql);
    if (/INSERT INTO mcp_tool_collection_runs/.test(sql)) return [{ id: ids.shift() }] as T[];
    if (/UPDATE mcp_tool_collection_runs/.test(sql)) return [];
    throw new Error(`unexpected query: ${sql}`);
  });
}

function capture(endpoint: string, tools = 1): CaptureRow {
  return {
    server_id: '00000000-0000-4000-8000-000000000001', endpoint_url: endpoint,
    captured_at: '2026-09-04T20:00:00.000Z', status: 'ok', http_status: 200,
    response_time_ms: 1, error_class: 'ok', tool_count: tools, page_count: 1,
    truncated: false, raw_json: [{ tools: [] }], raw_bytes: 2, token_estimate: 1,
    cache_ttl_ms: null, protocol_version: '2026-07-28', list_changed: null,
    note: null,
    tools: Array.from({ length: tools }, (_, i) => ({
      tool_name: `tool_${i}`, tool_title: null, description: null,
      input_schema: { type: 'object' }, output_schema: null,
      schema_hash: 'a'.repeat(64), contract_hash: 'b'.repeat(64), param_set: [],
      param_count: 0, required_count: 0, max_depth: 1, type_union: false,
      union_via_composition: false, has_ref: false, has_defs: false,
      annotations: null, token_estimate: 1,
    })),
  };
}

async function main() {
  const calls: string[] = [];
  let selections = 0;
  const first = await beginToolCollection(
    runQuery(['run-one'], calls) as any,
    async () => { selections++; calls.push('SELECT_TARGETS'); return ['endpoint']; },
    new Date('2026-09-04T20:00:00.000Z'), provenance,
  );
  assert.equal(first.runId, 'run-one');
  assert.ok(calls.findIndex(x => /INSERT INTO mcp_tool_collection_runs/.test(x)) < calls.indexOf('SELECT_TARGETS'));

  const second = await beginToolCollection(
    runQuery(['run-two'], []) as any, async () => [],
    new Date('2026-09-04T20:01:00.000Z'), provenance,
  );
  assert.notEqual(first.runId, second.runId, 'separate invocation has a separate run identity');

  let terminalValues: unknown[] = [];
  await updateToolCollectionRun(
    (async <T>(_sql: string, values?: unknown[]): Promise<T[]> => { terminalValues = values ?? []; return []; }) as any,
    'run-one', 'COMPLETE',
    { expected: 2, eligible: 2, attempted: 2, completed: 2, persisted: 2, notAttempted: 0, failedInternal: 0, excluded: 0 },
    null,
  );
  assert.deepEqual(terminalValues.slice(0, 10), ['run-one', 'COMPLETE', 2, 2, 2, 2, 2, 0, 0, 0]);

  const failedCalls: string[] = [];
  await assert.rejects(() => beginToolCollection(
    (async () => { failedCalls.push('OPEN_FAILED'); throw new Error('run creation failed'); }) as any,
    async () => { selections++; return []; }, new Date(), provenance,
  ), /run creation failed/);
  assert.equal(selections, 1, 'run creation failure starts zero additional selection or collection');

  const committed: string[] = [];
  const acquire = async () => ({
    async query(sql: string) {
      committed.push(sql);
      if (/INSERT INTO mcp_tool_captures/.test(sql)) return { rows: [{ id: 'capture-one' }] };
      return { rows: [] };
    },
    release() { committed.push('RELEASE'); },
  });
  await persistCaptureAtomically(capture('https://one.invalid/mcp'), 'run-one', 'compat-one', acquire);
  await persistCaptureAtomically(capture('https://two.invalid/mcp'), 'run-one', 'compat-one', acquire);
  assert.equal(committed.filter(x => /COMMIT/.test(x)).length, 2);
  assert.ok(committed.filter(x => /INSERT INTO mcp_tool_captures/.test(x)).every(x => /tool_collection_run_id/.test(x)));

  let rolledBack = false;
  const failingAcquire = async () => ({
    async query(sql: string) {
      if (/INSERT INTO mcp_tool_captures/.test(sql)) return { rows: [{ id: 'rolled-back-capture' }] };
      if (/INSERT INTO mcp_tools/.test(sql)) throw new Error('tool persistence failed');
      if (sql === 'ROLLBACK') rolledBack = true;
      return { rows: [] };
    },
    release() {},
  });
  await assert.rejects(
    () => persistCaptureAtomically(capture('https://failure.invalid/mcp'), 'run-failure', 'compat-failure', failingAcquire),
    /tool persistence failed/,
  );
  assert.equal(rolledBack, true, 'tool-row failure rolls the capture transaction back');

  const originalFetch = globalThis.fetch;
  const pages = [
    { jsonrpc: '2.0', id: 1, result: { tools: [{ name: 'one', inputSchema: { type: 'object' } }], nextCursor: 'SAME' } },
    { jsonrpc: '2.0', id: 1, result: { tools: [{ name: 'two', inputSchema: { type: 'object' } }], nextCursor: 'SAME' } },
  ];
  globalThis.fetch = (async () => new Response(JSON.stringify(pages.shift()), {
    status: 200, headers: { 'content-type': 'application/json' },
  })) as typeof fetch;
  try {
    const paged = await collectOnce({ serverId: 'server', url: 'https://pages.invalid/mcp', host: 'pages.invalid', attempts: 0 });
    assert.equal(paged.capture.page_count, 2);
    assert.equal(paged.capture.truncated, true, 'repeated cursor makes the one capture partial');

    globalThis.fetch = (async () => new Response(JSON.stringify({
      jsonrpc: '2.0', id: 1, error: { code: -32000, message: 'Mcp-Session-Id required' },
    }), { status: 400, headers: { 'content-type': 'application/json' } })) as typeof fetch;
    const session = await collectOnce({ serverId: 'server', url: 'https://session.invalid/mcp', host: 'session.invalid', attempts: 0 });
    assert.equal(session.capture.status, 'session_required');
  } finally {
    globalThis.fetch = originalFetch;
  }

  console.log('MCP tool collection run and atomic persistence contract: passed');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
