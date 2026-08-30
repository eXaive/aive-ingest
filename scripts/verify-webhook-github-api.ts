import assert from 'node:assert/strict';
import {
  WEBHOOK_PROVIDER_REQUEST_BOUNDS,
  ghHeaders,
  githubApiRequest,
  sanitizedGitHubFailure,
} from '../workers/ingest-webhook-providers';

const token = 'token-sentinel';

assert.equal(ghHeaders(token).Authorization, `Bearer ${token}`);
assert.equal(ghHeaders('').Authorization, undefined);

const [url, init] = githubApiRequest('/repos/PipedreamHQ/pipedream', token);
assert.equal(new URL(url).origin, 'https://api.github.com');
assert.equal(init.redirect, 'error', 'redirects must fail before credentials can reach another host');
assert.throws(() => githubApiRequest('//example.com/credential-target', token), /path rejected/);

const headers = new Headers({
  'x-ratelimit-limit': '5000',
  'x-ratelimit-remaining': '0',
  'x-ratelimit-reset': '1788102000',
  'retry-after': '12',
  'authorization': `Bearer ${token}`,
  'x-untrusted': 'do not include',
});
const message = sanitizedGitHubFailure('/repos/example/example', 403, headers);
assert.match(message, /HTTP 403 classification=RATE_LIMITED/);
assert.match(message, /x-ratelimit-limit=5000/);
assert.match(message, /x-ratelimit-remaining=0/);
assert.match(message, /x-ratelimit-reset=1788102000/);
assert.match(message, /retry-after=12/);
assert.ok(!message.includes(token));
assert.ok(!message.includes('authorization'));
assert.ok(!message.includes('do not include'));

const malformed = sanitizedGitHubFailure('/repos/example/example', 500, new Headers({
  'x-ratelimit-limit': '5000 leaked-text',
  'retry-after': 'tomorrow',
}));
assert.match(malformed, /classification=UPSTREAM_FAILURE/);
assert.match(malformed, /x-ratelimit-limit=unavailable/);
assert.match(malformed, /retry-after=unavailable/);

assert.deepEqual(WEBHOOK_PROVIDER_REQUEST_BOUNDS, {
  githubApiCalls: 8,
  rawFetchesPerSource: 400,
  sources: 2,
  rawConcurrency: 8,
});

console.log('PASS: webhook GitHub API authentication, redirect, diagnostics, and request bounds');
