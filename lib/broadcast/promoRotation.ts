/**
 * lib/broadcast/promoRotation.ts
 *
 * OU1COMES promotion inside the ordinary Daily Post rotation. NOT a scheduler,
 * not an editorial occurrence, not a second publishing path.
 *
 * A promotional topic is an ordinary broadcast_topic_queue row whose
 * topic_name starts with PROMO_TOPIC_PREFIX. It carries its own reviewed
 * prompt_seed and caption like every other topic, and no
 * pregenerated_media_url, so each promotion day renders a NEW video.
 *
 * Marked by name, not by `layer`: broadcast_topic_queue_layer_check only admits
 * the seven protocol layers, so a new layer value would need a migration.
 *
 * On Monday, Wednesday and Friday (UTC) the Daily Post worker picks from the
 * promo pool; on the other days from the regular pool. Each pool keeps its own
 * queue_position order and loops independently. The text-card worker never
 * uses promo rows.
 */

export const PROMO_TOPIC_PREFIX = 'OU1COMES:';

/** Monday, Wednesday, Friday — Date#getUTCDay values. */
export const PROMO_UTC_DAYS: readonly number[] = [1, 3, 5];

/**
 * Promotion day? UTC, the same day boundary the worker's daily-video guard
 * uses. The 14:00 cron has run 5-9 hours late on this repo but has always
 * landed on its own UTC day.
 */
export function isPromoDay(now: Date = new Date()): boolean {
  return PROMO_UTC_DAYS.includes(now.getUTCDay());
}

/* Constant SQL, no caller input. `_` is a LIKE wildcard, so the prefix must not
   contain one; '%' is appended here. */
const PROMO_MATCH = `topic_name LIKE '${PROMO_TOPIC_PREFIX}%'`;

/** WHERE-clause predicate for one pool of broadcast_topic_queue. */
export function topicPoolPredicate(promo: boolean): string {
  return promo ? PROMO_MATCH : `NOT (${PROMO_MATCH})`;
}
