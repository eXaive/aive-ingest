import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { runProduction, liveDisabled, CONTRACT, TARGET, type Transport, type Request } from '../../workers/broadcast-content-production';
const spec='00000000-0000-4000-8000-000000000014';
const config={origin:'https://aive.global',workerToken:'mock-worker-token-not-a-credential',grantId:'00000000-0000-4000-8000-000000000001',runId:'123:1'};
const envelope={contract:CONTRACT,channel:TARGET.channel,batch_id:TARGET.batch_id};
const selection={spec_id:spec,spec_revision:1,episode:14,initial_state:'STILL_REQUIRED'};
const pre={...envelope,coordination:'SHARED_TRANSACTIONAL',worker_contract_ready:true,grant_valid:true,selection};
const done={...envelope,spec_id:spec,spec_revision:1,outcome:'PRODUCED_TO_OWNER_GATE',final_state:'OWNER_REVIEW_REQUIRED'};
async function scenario(p:unknown=pre, response:unknown=done, fail=false, c=config) {
 const requests:Request[]=[];
 const transport:Transport=async r=>{requests.push(r);if(r.url.endsWith('/preflight'))return{status:200,body:p};if(fail)throw Error('secret raw provider error https://private.invalid/?token=secret');return{status:200,body:response};};
 const result=await runProduction(c,transport);return{result,requests,advance:requests.filter(r=>r.url.endsWith('/advance'))};
}
async function main(){
 const success=await scenario();assert.equal(success.result.outcome,'PRODUCED_TO_OWNER_GATE');assert.equal(success.advance.length,1);
 assert.equal(JSON.parse(success.advance[0].body).max_specs,1);assert.equal(success.advance[0].redirect,'error');assert.equal(success.advance[0].timeout_ms,480000);
 for(const coordination of ['LOCAL','UNAVAILABLE','UNKNOWN']){const r=await scenario({...pre,coordination});assert.equal(r.result.outcome,'BLOCKED_COORDINATION_NOT_SHARED');assert.equal(r.advance.length,0);}
 for(const p of [{...pre,worker_contract_ready:false},{...pre,selection:{...selection,episode:13}},{...pre,selection:{...selection,episode:31}},{...pre,channel:'other-channel'},{...pre,selection:[selection]},{...pre,selection:{...selection,spec_id:'unsafe URL'}}])assert.equal((await scenario(p)).advance.length,0);
 const invalid=await scenario(pre,done,false,{...config,origin:'https://elsewhere.invalid'});assert.equal(invalid.requests.length,0);
 const none=await scenario({...pre,selection:null});assert.equal(none.result.outcome,'NO_ELIGIBLE_WORK');assert.equal(none.advance.length,0);
 const grant=await scenario({...pre,grant_valid:false});assert.equal(grant.result.outcome,'BLOCKED_GRANT');assert.equal(grant.advance.length,0);
 for(const outcome of ['UNKNOWN','CLAIM_LOST','FAILED_DEFINITE','BLOCKED_GRANT','BLOCKED_COORDINATION_NOT_SHARED']){const r=await scenario(pre,{...done,outcome,final_state:'BLOCKED'});assert.equal(r.result.outcome,outcome);assert.equal(r.advance.length,1);}
 for(const response of [null,{...done,spec_id:config.grantId},{...done,final_state:'READY'},{...done,secret:'do not log'}]){const r=await scenario(pre,response);assert.equal(r.result.outcome,'UNKNOWN');assert.equal(r.advance.length,1);}
 const timeout=await scenario(pre,done,true);assert.equal(timeout.result.outcome,'UNKNOWN');assert.equal(timeout.advance.length,1);assert.ok(!JSON.stringify(timeout.result).includes('secret'));
 // Mock AIVE owns stable claim identity across reruns: only the first invocation wins.
 const identities=new Set<string>();let paidPermits=0;
 const api:Transport=async r=>{if(r.url.endsWith('/preflight'))return{status:200,body:pre};const b=JSON.parse(r.body);const key=b.selection.spec_id+':'+b.selection.spec_revision+':'+b.grant_id;
 const won=!identities.has(key);identities.add(key);if(won)paidPermits++;assert.ok(!('action_id' in b));return{status:200,body:{...done,outcome:won?'PRODUCED_TO_OWNER_GATE':'CLAIM_LOST',final_state:won?'OWNER_REVIEW_REQUIRED':'BLOCKED'}};};
 await runProduction(config,api);const rerun=await runProduction({...config,runId:'123:2'},api);assert.equal(rerun.outcome,'CLAIM_LOST');assert.equal(paidPermits,1);
 assert.equal(liveDisabled().reason,'MISSING_AIVE_WORKER_CONTRACT');
 const workflow=readFileSync('.github/workflows/broadcast-content-production.yml','utf8');assert.ok(workflow.includes('workflow_dispatch:'));assert.ok(!/^\s*(schedule|cron):/m.test(workflow));assert.ok(!workflow.includes('secrets.'));assert.ok(workflow.includes('timeout-minutes: 10'));assert.ok(workflow.includes('cancel-in-progress: false'));
 assert.ok(!JSON.stringify(success.result).includes(config.workerToken));assert.ok(!JSON.stringify(success.result).includes(config.grantId));
 console.log('PASS: one episode, shared-only gate, scope, grant, no-op, outcomes, malformed response, timeout/no retry, mocked rerun identity, safe logs, manual mock-only workflow; real requests 0');
}
main().catch(()=>{console.error('FAIL: production orchestration mocked acceptance');process.exitCode=1;});
