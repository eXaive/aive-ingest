# OU1COMES promo videos: explicit scenes (design, for review — not built)

Status: DESIGN. Build week of 2026-10-19, after owner review. Today (revision 3) the promo rows still send a free-text
prompt and Blotato's AI writes the narration and draws every image, including the final card.

## Goal

For `OU1COMES:` topics only, the Daily Post worker sends **our exact narration per scene, our own image prompts, and an
uploaded static final card**, and refuses to generate if any of our text contains a non-canonical domain.
Other Daily Post topics are unchanged.

## What the template accepts (read 2026-10-16, `GET /v2/videos/templates?id=…&fields=id,name,description,inputs`)

`/base/v2/ai-story-video/5903fe43-514d-40ee-a060-0d6628c5f8fd/v1` ("AI Video with AI Voice"):

| input | use |
|---|---|
| `scenes[]` (1–20) | `{ script: text, mediaSource: { aiPrompt: text } \| { uploadedMedia: url } }` — script is read by the AI voice |
| `voiceName` | ElevenLabs voice enum (default `Brian (American, deep)`) |
| `enableVoiceover`, `captionPosition`, `highlightColor`, `transition`, `aspectRatio` (`9:16`), `aiImageModel`, `animateAiImages`, `trimToVoiceover` | presentation |

Manual `inputs` take precedence over AI generation; send `prompt: ""` so nothing is AI-written. There is **no caption-text
input** (captions are the voiceover words) and **no watermark/end-card input**. The finished creation exposes only
`status, mediaUrl, imageUrls, id, createdAt`, so all checks must run **before** generation, on our own text.

## Where the scene data lives

A structured block in the row's existing `prompt_seed` (no migration):

```
OU1COMES_SCENES_V1
{"voiceName":"Brian (American, deep)","scenes":[
  {"script":"Thousands of MCP servers. Which one fits your agent?","image":"A glowing network of connected nodes, dark background, no text"},
  {"script":"On Outcomes you search for MCP servers, save the ones that matter, and see the tools each one offers.","image":"A search icon and a bookmark icon over a node network, no text"},
  {"script":"Find it at outcomes dot services.","card":"final"}
]}
```

- Line 1 is the marker; the rest is JSON. `scenes[].image` → `mediaSource.aiPrompt`; `card: "final"` → `mediaSource.uploadedMedia` = the canonical card URL (below). Exactly one `card` scene, and it is last.
- The row's `caption` (TikTok post text) stays a separate, reviewed column, as today.
- A promo row **without** a valid block fails the run (no silent fallback to the free prompt), matching the worker's existing "no reviewed caption → refuse" rule.

## The final card

- One static asset, 1080×1920, reading exactly **OU1COMES** and **outcomes.services** on the brand background; no other text.
- The template's `uploadedMedia` is typed `video` ("Upload Image/Video"). **To verify in the build:** whether a PNG is accepted; if not, use a 3-second MP4 of the same card.
- Hosted once at a stable public URL (Supabase Storage public object or Blotato media upload), recorded in code as
  `OU1COMES_FINAL_CARD_URL` with its SHA-256; the worker uses that constant, never row data, so a row cannot swap it.
- Reused by every promo video. Changing the card is a reviewed code change.

## Pre-generation check (pure, `lib/broadcast/promoScenes.ts`)

Runs on the parsed block and the row's caption before any Blotato call. Blocks (run fails, row stays pending, nothing
posts) when:

- the block is missing or invalid JSON, has 0 or more than 20 scenes, the `card` scene is missing / not last / repeated,
  or a non-card scene has no script or no image prompt;
- any script, image prompt or caption contains `www`, `w w w`, `.com`, `dot com`, `http`, or any domain-like token other
  than `outcomes.services` (written) — spoken forms checked too: `dot net`, `dot io`, `dot ai`, `dot org`;
- an image prompt contains any URL or domain at all (images carry no web address; only the uploaded card does);
- the final scene's script does not contain `outcomes dot services`;
- a script contains `OU1COMES` (the voice would spell it; the brand is spoken "Outcomes").

Not blocked: the word "outcomes" (the spoken brand and ordinary English).

Captions will show the narration, i.e. "Outcomes" / "outcomes dot services". `OU1COMES` appears on the final card.

## Worker change (`workers/broadcast-daily-post.ts`)

`startGeneration` gains a promo branch: for `OU1COMES:` topics, parse + check the block, then send
`{ templateId: AI_STORY_TEMPLATE, inputs: { scenes, voiceName, aspectRatio: '9:16', enableVoiceover: true, captionPosition: 'center' }, prompt: '', render: true }`.
Non-promo topics keep today's request. Logging stays status-only (public repo).

## Verification

- `verify:ou1comes-scenes` (registered in verify.yml): fixtures for every block rule above, the request body shape for
  promo vs regular topics, and that the card URL comes only from the constant.
- One private test render (as revision 2 was), owner review of the actual video, then the data update below.

## Data update (separate owner approval)

Rewrite the three `OU1COMES:` rows' `prompt_seed` to `OU1COMES_SCENES_V1` blocks with owner-approved scripts and image
prompts (3–4 scenes each). Revision 3 text remains until then.
