/**
 * Accounts that must receive at least one confirmed post every day, and the scheduled workers that feed them.
 * The hourly reconciler fails its run, naming the account, when one has 0 confirmed posts in the last 24 hours
 * (a stalled or silently skipping worker otherwise stays green). scripts/verify-publication-stall.ts checks these
 * ids against the worker constants.
 */
export const DAILY_POSTING_ACCOUNTS = [
  { accountId: '09726d10-857d-468f-8e01-b70361c17fa1', handle: 'mr.agent.infrastructure', workers: ['Broadcast Daily Post', 'Broadcast Text Card'] },
  { accountId: 'bd7d3219-0d23-4523-9f07-7b8bd4e55e74', handle: 'vice.marshal.kyli', workers: ['Broadcast Quiz Card (GTA)'] },
  { accountId: 'f05c3ccf-630d-4d74-ac28-6bc7cc560ba4', handle: 'wwwsnowbunnymafiacom', workers: ['Broadcast Quiz Card (Cat)'] },
] as const;

export const POSTED_LAST_24H_SQL = `select account_id::text as account_id, count(*)::int as n from broadcast_publication_attempts
  where account_id = any($1::uuid[]) and state = 'confirmed_published' and confirmed_at >= now() - interval '24 hours' group by account_id`;

/** Accounts from DAILY_POSTING_ACCOUNTS with no confirmed post in the counted rows. */
export function postedNothingAccounts(rows: readonly { account_id: string; n: number }[]) {
  const posted = new Set(rows.filter(r => Number(r.n) > 0).map(r => r.account_id));
  return DAILY_POSTING_ACCOUNTS.filter(a => !posted.has(a.accountId));
}

export function postedNothingMessage(accounts: ReturnType<typeof postedNothingAccounts>): string | null {
  if (accounts.length === 0) return null;
  return 'POSTED_NOTHING_24H: ' + accounts.map(a => `${a.handle} (fed by ${a.workers.join(' + ')})`).join('; ')
    + ' had 0 confirmed posts in the last 24 hours';
}
