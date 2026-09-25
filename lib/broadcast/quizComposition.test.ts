import assert from 'node:assert/strict';
import { buildCaption, buildCardText } from '../../workers/broadcast-quiz-card';

const row = Object.freeze({
  id: 'fixture-only', question_text: '  Which option fits this fixture?  ',
  option_a: 'First', option_b: 'Second', option_c: 'Third', option_d: 'Fourth',
});
const card = 'Which option fits this fixture?\n\nA) First\nB) Second\nC) Third\nD) Fourth';
const original = JSON.stringify(row);
for (const [category, hashtags] of Object.entries({
  CAT: '#cats #cattrivia #catsoftiktok #trivia',
  GTA: '#GTA #GTA6 #gaming #trivia',
})) {
  const caption = buildCaption(row, hashtags);
  assert.equal(caption, 'The Quiz Everything Show\n\nWhich option fits this fixture?\n\nAnswer in the comments \u{1F447}\n\n' + hashtags, category);
  assert.equal(caption.split('The Quiz Everything Show').length - 1, 1, category);
  assert.equal(caption, caption.trim(), category);
  assert.equal(buildCardText(row), card, category);
  assert.equal(JSON.stringify(row), original, category);
}
const tail = '\n\nA) First\nB) Second\nC) Third\nD) Fourth';
const boundary = Object.freeze({ ...row, question_text: 'Q'.repeat(350 - tail.length) });
assert.equal(buildCardText(boundary).length, 350);
assert.ok(buildCaption(boundary, '#trivia').length > 350);
assert.equal(buildCardText(boundary).includes('The Quiz Everything Show'), false);
assert.throws(() => buildCardText({ ...boundary, question_text: boundary.question_text + '?' }), /over the template's 350 limit/);
console.log('PASS: CAT/GTA captions, unchanged immutable cards, and independent 350-character card limit');
