/** Hourly OU1COMES category-introduction distribution (see lib/broadcast/introDistribution.ts). */
import { runIntroDistribution } from '../lib/broadcast/introDistribution';
runIntroDistribution().catch(() => { console.log('[intro-distribution] unavailable; nothing submitted'); });
