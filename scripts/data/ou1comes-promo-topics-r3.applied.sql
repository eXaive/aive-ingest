-- OU1COMES promo topics, revision 3 — APPLIED 2026-10-16 with owner approval (data rows only, no schema change).
-- Applied as conditional per-row updates (id + exact previous text) to all three 'OU1COMES:' rows of
-- broadcast_topic_queue; status/queue_position untouched. This file records the exact change.
--
-- Why: the first promo video (2026-10-16, Blotato creation 987b481c-9d22-4bed-b084-f3cfaa2c1890) showed captions
-- "OUTCOMES", a mangled spoken URL ("www ... outcomes .services") and an invented "OU1COMES.COM" watermark next to the
-- www.outcomes.services CTA. The AI story template writes narration and draws images from this prompt; captions are
-- the narration words (no separate caption-text input), so captions will show the spoken brand "Outcomes".
-- Canonical: spoken "Outcomes" / "outcomes dot services"; written outcomes.services; final card OU1COMES +
-- outcomes.services; never www, never .com, never another domain. ou1comes.com is not a domain we use (NXDOMAIN).
--
-- Next (separate change): explicit inputs.scenes with our exact scripts and an uploaded final card.
UPDATE broadcast_topic_queue SET
  prompt_seed = replace(replace(replace(replace(prompt_seed,
    'for Outcomes, a website at www.outcomes.services.',
    'for Outcomes, a website at outcomes.services.'),
    'Close on the website, www.outcomes.services.',
    'Close on the website: the narrator says "outcomes dot services".'),
    'Any on-screen brand text reads OU1COMES exactly. The narrator always says the name as the word Outcomes and never spells it out.',
    'The narrator says the brand as the word "Outcomes" and the website as "outcomes dot services". The narrator never says "www" and never says any other web address. Any on-screen brand text reads OU1COMES exactly. The only web address anywhere in the video, on cards, in images, as a watermark or in any other text, is outcomes.services. Never show ".com", never show "www", and never show any other domain.'),
    'and no on-screen words other than the captions and a final card reading OU1COMES and www.outcomes.services.',
    'and no text inside any image except the final card, which reads exactly: OU1COMES and outcomes.services.'),
  caption = replace(caption, 'www.outcomes.services', 'outcomes.services')
WHERE topic_name LIKE 'OU1COMES:%';
