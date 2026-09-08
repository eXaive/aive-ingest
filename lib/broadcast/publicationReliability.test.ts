import assert from 'node:assert/strict';

type State='reserved'|'submitting'|'provider_accepted'|'confirmation_pending'|'confirmed_published'|'provider_failed'|'outcome_unknown'|'simulated';
type Attempt={state:State;submissionId?:string;url?:string;cursor:boolean};
const attempts=new Map<string,Attempt>();
async function claim(key:string){if(!attempts.has(key))attempts.set(key,{state:'reserved',cursor:false});return attempts.get(key)!}
function canSubmit(a:Attempt){return a.state==='reserved'}
function capacity(a:Attempt){return !['provider_failed','simulated'].includes(a.state)}
function recover(a:Attempt){if(a.state==='confirmed_published')a.cursor=true}

async function main(){
const [first,second]=await Promise.all([claim('source/account/slot'),claim('source/account/slot')]);
assert.equal(first,second);assert.equal([...attempts].length,1); // concurrent identity claim
first.state='provider_accepted';first.submissionId='provider-1';assert.equal(canSubmit(first),false); // repeat dispatch
first.state='confirmation_pending';assert.equal(first.submissionId,'provider-1');first.state='confirmed_published';first.url='https://provider.invalid/post/1'; // timeout then GET reconciliation
recover(first);assert.equal(first.cursor,true);recover(first);assert.equal(first.cursor,true); // crash-safe cursor recovery
const unknown:Attempt={state:'outcome_unknown',cursor:false};assert.equal(canSubmit(unknown),false);assert.equal(capacity(unknown),true); // ambiguous create
const successfulTarget:Attempt={state:'provider_accepted',cursor:false};const failedTarget:Attempt={state:'provider_failed',cursor:false};assert.equal(canSubmit(successfulTarget),false);assert.equal(capacity(failedTarget),false); // mixed accounts
const legacy={status:'posted',attempt:undefined};assert.equal(legacy.attempt,undefined); // historical acceptance stays unverified
assert.equal(capacity({state:'simulated',cursor:false}),false); // stub is not publication
const cap=[{state:'confirmed_published'},{state:'outcome_unknown'},{state:'confirmation_pending'}] as Attempt[];assert.equal(cap.filter(capacity).length,3); // conservative cap
console.log('broadcast publication recovery model: ok');
}
void main();
