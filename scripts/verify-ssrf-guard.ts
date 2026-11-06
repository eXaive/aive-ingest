/**
 * SSRF guard contract (lib/net/ssrfGuard.ts, 2026-11-06; MCP collection audit 2026-11-05).
 *
 * PROVES
 *   1. Every blocked IPv4/IPv6 range is refused, including IPv4 embedded in IPv6 and the
 *      alternative numeric spellings the WHATWG URL parser normalises (2130706433, 0x7f.1).
 *   2. Public addresses pass.
 *   3. A host with ANY blocked record is refused (mixed-record DNS), and DNS failures stay DNS failures.
 *   4. guardedFetch re-checks every redirect hop: a public URL that redirects to the cloud
 *      metadata address is refused and the metadata address is never contacted.
 *   5. Redirect method rules match fetch: 307/308 keep POST+body, 301/302 POST -> GET, 303 -> GET.
 *   6. More than MAX_REDIRECTS hops are refused.
 *   7. Collectors: probeEndpoint and the tools/list collectOnce never dial a blocked host, and record
 *      "no request made" (probe_method NULL / status not_attempted) with an ssrf_blocked note.
 *
 * No network, no database: DNS is injected through setSsrfTestPolicy and fetch is a recording stub.
 * Run: npx tsx scripts/verify-ssrf-guard.ts
 */
import assert from 'node:assert/strict';
import { isBlockedAddress, assertOutboundAllowed, guardedFetch, SsrfBlockedError, setSsrfTestPolicy, MAX_REDIRECTS } from '../lib/net/ssrfGuard';
import { probeEndpoint } from '../workers/probe-mcp-endpoints';
import { collectOnce } from '../workers/collect-mcp-tools';

const DNS: Record<string, string[]> = {
  'public.test': ['93.184.216.34'],
  'public6.test': ['2606:2800:220:1:248:1893:25c8:1946'],
  'mixed.test': ['93.184.216.34', '10.0.0.7'],         // one public, one private record
  'metadata.test': ['169.254.169.254'],
  'internal.test': ['10.1.2.3'],
};
setSsrfTestPolicy({
  lookup: async (host) => {
    const a = DNS[host];
    if (!a) throw Object.assign(new Error(`getaddrinfo ENOTFOUND ${host}`), { code: 'ENOTFOUND' });
    return a;
  },
});

async function main() {
  // 1-2. Address classification.
  const blockedIps = [
    '0.0.0.0', '10.0.0.1', '100.64.0.1', '127.0.0.1', '127.255.255.254', '169.254.169.254', '172.16.0.1', '172.31.255.255',
    '192.0.0.8', '192.0.2.1', '192.168.1.1', '198.18.0.1', '198.51.100.7', '203.0.113.9', '224.0.0.1', '240.0.0.1', '255.255.255.255',
    '::', '::1', '[::1]', 'fe80::1', 'fe80::1%eth0', 'fc00::1', 'fd12:3456::1', 'fec0::1', 'ff02::1', '2001:db8::1', '2001::1', '2002:7f00:1::',
    '::ffff:127.0.0.1', '::ffff:7f00:1', '::ffff:a9fe:a9fe', '64:ff9b::a9fe:a9fe', '64:ff9b::10.0.0.1', 'not-an-ip', '',
  ];
  for (const ip of blockedIps) assert.equal(isBlockedAddress(ip), true, `blocked: ${ip}`);
  for (const ip of ['93.184.216.34', '8.8.8.8', '1.1.1.1', '172.32.0.1', '100.128.0.1', '2606:4700:4700::1111', '::ffff:8.8.8.8', '64:ff9b::808:808']) {
    assert.equal(isBlockedAddress(ip), false, `allowed: ${ip}`);
  }
  console.log(`  [1-2] ${blockedIps.length} blocked forms refused (incl. mapped/NAT64/6to4), public v4/v6 allowed`);

  // 1b. The URL parser normalises numeric spellings to dotted quads; they are refused as literals, no DNS needed.
  for (const u of ['http://2130706433/', 'http://0x7f.1/', 'http://017700000001/', 'http://[::ffff:127.0.0.1]/', 'http://[::1]:5432/', 'http://169.254.169.254/latest/meta-data/']) {
    await assert.rejects(assertOutboundAllowed(new URL(u)), (e: unknown) => e instanceof SsrfBlockedError && e.reason === 'blocked_address' && e.hop === 0, u);
  }
  await assert.rejects(assertOutboundAllowed(new URL('file:///etc/passwd')), (e: unknown) => e instanceof SsrfBlockedError && e.reason === 'scheme');
  await assert.rejects(assertOutboundAllowed(new URL('gopher://public.test/')), (e: unknown) => e instanceof SsrfBlockedError && e.reason === 'scheme');
  console.log('  [1b] decimal/hex/octal/mapped literals and non-http schemes refused');

  // 3. DNS: mixed records refused; DNS failures propagate as DNS errors (classified dns_failure, as before).
  await assert.doesNotReject(assertOutboundAllowed(new URL('https://public.test/mcp')));
  await assert.doesNotReject(assertOutboundAllowed(new URL('https://public6.test/mcp')));
  await assert.rejects(assertOutboundAllowed(new URL('https://mixed.test/mcp')), (e: unknown) => e instanceof SsrfBlockedError && /10\.0\.0\.7/.test(e.message));
  await assert.rejects(assertOutboundAllowed(new URL('https://internal.test/mcp')), SsrfBlockedError);
  await assert.rejects(assertOutboundAllowed(new URL('https://nowhere.test/mcp')), (e: unknown) => !(e instanceof SsrfBlockedError) && (e as { code?: string }).code === 'ENOTFOUND');
  console.log('  [3] mixed-record host refused; unresolvable host stays a DNS failure');

  // 4-6. guardedFetch with a recording fetch stub.
  type Call = { url: string; method: string; body: unknown };
  const stub = (routes: Record<string, () => Response>) => {
    const calls: Call[] = [];
    const impl = (async (url: string, init?: RequestInit) => {
      calls.push({ url, method: String(init?.method), body: init?.body ?? null });
      assert.equal(init?.redirect, 'manual', 'guardedFetch never lets fetch follow redirects itself');
      const route = routes[url];
      if (!route) throw new Error(`unexpected request to ${url}`);
      return route();
    }) as unknown as typeof fetch;
    return { calls, impl };
  };
  const redirect = (status: number, location: string) => () => new Response(null, { status, headers: { location } });
  const ok = () => new Response('{}', { status: 200 });

  {
    const s = stub({ 'https://public.test/mcp': redirect(302, 'http://169.254.169.254/latest/meta-data/') });
    await assert.rejects(guardedFetch('https://public.test/mcp', { method: 'POST', body: '{}' }, s.impl),
      (e: unknown) => e instanceof SsrfBlockedError && e.hop === 1 && e.reason === 'blocked_address');
    assert.deepEqual(s.calls.map((c) => c.url), ['https://public.test/mcp'], 'the metadata address is never contacted');
    const s2 = stub({ 'https://public.test/a': redirect(301, 'https://metadata.test/x') });
    await assert.rejects(guardedFetch('https://public.test/a', {}, s2.impl), (e: unknown) => e instanceof SsrfBlockedError && e.hop === 1);
    assert.equal(s2.calls.length, 1, 'a redirect to a host that RESOLVES to metadata is refused too');
    console.log('  [4] redirect to 169.254.169.254 (literal or via DNS) refused at hop 1; only the public URL was contacted');
  }
  {
    const s = stub({
      'https://public.test/307': redirect(307, '/308'), 'https://public.test/308': redirect(308, 'https://public6.test/p'),
      'https://public6.test/p': ok,
    });
    const res = await guardedFetch('https://public.test/307', { method: 'POST', body: 'JSONRPC' }, s.impl);
    assert.equal(res.status, 200);
    assert.deepEqual(s.calls.map((c) => [c.method, c.body]), [['POST', 'JSONRPC'], ['POST', 'JSONRPC'], ['POST', 'JSONRPC']], '307/308 keep POST and body');
    const g = stub({ 'https://public.test/302': redirect(302, '/303'), 'https://public.test/303': redirect(303, '/end'), 'https://public.test/end': ok });
    await guardedFetch('https://public.test/302', { method: 'POST', body: 'X' }, g.impl);
    assert.deepEqual(g.calls.map((c) => [c.method, c.body]), [['POST', 'X'], ['GET', null], ['GET', null]], '302 POST -> GET, 303 -> GET, body dropped');
    const h = stub({ 'https://public.test/h': redirect(303, '/h2'), 'https://public.test/h2': ok });
    await guardedFetch('https://public.test/h', { method: 'HEAD' }, h.impl);
    assert.deepEqual(h.calls.map((c) => c.method), ['HEAD', 'HEAD'], '303 keeps HEAD');
    console.log('  [5] redirect method rules match fetch (307/308 keep POST+body; 301/302 POST and 303 become GET)');
  }
  {
    const routes: Record<string, () => Response> = {};
    for (let i = 0; i <= MAX_REDIRECTS + 1; i++) routes[`https://public.test/${i}`] = redirect(302, `/${i + 1}`);
    const s = stub(routes);
    await assert.rejects(guardedFetch('https://public.test/0', {}, s.impl), (e: unknown) => e instanceof SsrfBlockedError && e.reason === 'redirect_limit');
    assert.equal(s.calls.length, MAX_REDIRECTS + 1, `exactly ${MAX_REDIRECTS} redirects are followed`);
    const bad = stub({ 'https://public.test/b': redirect(302, 'http://[::1') });
    await assert.rejects(guardedFetch('https://public.test/b', {}, bad.impl), (e: unknown) => e instanceof SsrfBlockedError && e.reason === 'bad_redirect');
    console.log(`  [6] more than ${MAX_REDIRECTS} redirects refused; malformed Location refused`);
  }

  // 7. Collectors never dial a blocked host and record "no request made".
  {
    const original = globalThis.fetch;
    let dialled = 0;
    globalThis.fetch = (async () => { dialled++; return new Response('{}', { status: 200 }); }) as typeof fetch;
    try {
      const p = await probeEndpoint('server', 'http://169.254.169.254/latest/meta-data/');
      assert.equal(p.error_class, 'other');
      assert.equal(p.probe_method, null, 'probe_method NULL = no request sent');
      assert.match(p.note ?? '', /^ssrf_blocked: blocked_address at hop 0/);
      const p2 = await probeEndpoint('server', 'https://internal.test/mcp');
      assert.equal(p2.probe_method, null);
      const t = await collectOnce({ serverId: 'server', url: 'https://mixed.test/mcp', host: 'mixed.test', attempts: 0 });
      assert.equal(t.capture.status, 'not_attempted', 'tools/list: blocked host is not_attempted, not error');
      assert.match(t.capture.note ?? '', /ssrf_blocked/);
      assert.equal(dialled, 0, 'no collector request reached fetch');
    } finally {
      globalThis.fetch = original;
    }
    console.log('  [7] probe and tools/list collectors: blocked hosts never dialled; recorded as no-request with an ssrf_blocked note');
  }

  setSsrfTestPolicy(null);
  console.log('SSRF guard contract: passed');
}

main().catch((e) => { console.error(e); process.exit(1); });
