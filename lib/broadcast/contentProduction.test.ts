import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {runProduction,liveDisabled,CONTRACT,TARGET,type Request,type Transport} from '../../workers/broadcast-content-production';
const config={origin:'https://aive.global',workerToken:'mock-worker-token-not-a-credential',grantId:'00000000-0000-4000-8000-000000000001',runId:'123:1'};
const pre={contract:CONTRACT,...TARGET,coordination:'SHARED_TRANSACTIONAL',production_enabled:true,eligible_work:true,grant_valid:true,outcome:null,spec_id:'00000000-0000-4000-8000-000000000014',episode:14,initial_state:'STILL_REQUIRED',final_state:'STILL_REQUIRED'};
const done={...pre,production_enabled:false,outcome:'PRODUCED_TO_OWNER_GATE',final_state:'OWNER_REVIEW_REQUIRED'};
async function scenario(p:unknown=pre,r:unknown=done,fail=false,c=config){const calls:Request[]=[];const result=await runProduction(c,async req=>{calls.push(req);if(req.url.endsWith('/preflight'))return{status:200,body:p};if(fail)throw Error('secret');return{status:200,body:r};});return{result,calls};}
async function main(){
 const happy=await scenario();assert.equal(happy.result.outcome,'PRODUCED_TO_OWNER_GATE');assert.equal(happy.calls.length,2);
 for(const req of happy.calls){assert.deepEqual(Object.keys(JSON.parse(req.body)).sort(),['contract','batch_id','grant_id','operational_run_id'].sort());assert.equal(req.redirect,'error');}
 for(const coordination of ['LOCAL','UNAVAILABLE']){const r=await scenario({...pre,coordination,production_enabled:false,outcome:'BLOCKED_COORDINATION_NOT_SHARED'});assert.equal(r.result.outcome,'BLOCKED_COORDINATION_NOT_SHARED');assert.equal(r.calls.length,1);}
 for(const outcome of ['NO_ELIGIBLE_WORK','BLOCKED_GRANT','UNKNOWN','CLAIM_LOST','FAILED_DEFINITE']){const r=await scenario({...pre,production_enabled:false,outcome});assert.equal(r.result.outcome,outcome);assert.equal(r.calls.length,1);}
 for(const grantId of ['', 'bad'])assert.equal((await scenario(pre,done,false,{...config,grantId})).calls.length,0);
 for(const response of [null,{...done,extra:'secret'},{...done,final_state:'READY'},{...done,production_enabled:true}]){const r=await scenario(pre,response);assert.equal(r.result.outcome,'UNKNOWN');assert.equal(r.calls.length,2);}
 const unknown=await scenario(pre,done,true);assert.equal(unknown.result.outcome,'UNKNOWN');assert.equal(unknown.calls.length,2);assert.ok(!JSON.stringify(unknown.result).includes('secret'));
 const calls:Request[]=[];const api:Transport=async req=>{calls.push(req);return{status:200,body:req.url.endsWith('/preflight')?pre:done};};await runProduction(config,api);await runProduction({...config,runId:'123:2'},api);
 assert.deepEqual(JSON.parse(calls[1].body),{...JSON.parse(calls[3].body),operational_run_id:'123:1'});assert.ok(!calls[1].body.includes('spec_id'));
 assert.equal(liveDisabled().reason,'LIVE_TRANSPORT_DISABLED');const yaml=readFileSync('.github/workflows/broadcast-content-production.yml','utf8');assert.ok(yaml.includes('workflow_dispatch:'));assert.ok(!/^\s*(schedule|cron):/m.test(yaml));assert.ok(!yaml.split('  live-still:')[0].includes('secrets.'));assert.ok(yaml.includes('secrets.AIVE_BROADCAST_PRODUCTION_MACHINE_SECRET'));
 assert.ok(!JSON.stringify(happy.result).includes(config.workerToken));assert.ok(!JSON.stringify(happy.result).includes(config.grantId));
 console.log('PASS: scope-only requests, strict response, shared/grant gates, one advance, UNKNOWN no retry, rerun carries no selection/action identity, no secrets, disabled workflow');
}
main().catch(()=>{console.error('FAIL: mocked production client');process.exitCode=1;});
