import assert from 'node:assert/strict';
import {runManual,httpTransport} from '../../workers/broadcast-still-production-manual';
import {runProduction,CONTRACT,TARGET,type Request} from '../../workers/broadcast-content-production';
globalThis.fetch=async()=>{throw Error('REAL HTTP FORBIDDEN')};
const env={GITHUB_ACTIONS:'true',GITHUB_EVENT_NAME:'workflow_dispatch',AIVE_PRODUCTION_MODE:'live',GITHUB_RUN_ID:'123',GITHUB_RUN_ATTEMPT:'1',AIVE_PRODUCTION_GRANT_ID:'00000000-0000-4000-8000-000000000001',AIVE_BROADCAST_PRODUCTION_MACHINE_SECRET:'offline-machine-fixture-secret-only'};
const pre={contract:CONTRACT,...TARGET,coordination:'SHARED_TRANSACTIONAL',production_enabled:true,eligible_work:true,grant_valid:true,outcome:null,spec_id:'00000000-0000-4000-8000-000000000014',episode:14,initial_state:'STILL_REQUIRED',final_state:'STILL_REQUIRED'};
const done={...pre,production_enabled:false,outcome:'PRODUCED_TO_SHORT_REQUIRED',final_state:'SHORT_REQUIRED'};
async function scenario(options:{env?:NodeJS.ProcessEnv;pre?:unknown;done?:unknown;status?:number;error?:boolean}={}){
 const calls:Request[]=[];const result=await runManual(options.env??env,async r=>{calls.push(r);if(r.url.endsWith('/preflight'))return{status:200,body:options.pre===undefined?pre:options.pre};if(options.error)throw Error(env.AIVE_BROADCAST_PRODUCTION_MACHINE_SECRET);return{status:options.status??200,body:options.done===undefined?done:options.done}});return{...result,calls};
}
async function main(){
 // Scheduled calls retain the exact manual transport and one-advance boundary.
 const scheduled={...env,GITHUB_EVENT_NAME:'schedule'};
 const scheduledSuccess=await scenario({env:scheduled});assert.equal(scheduledSuccess.exitCode,0);assert.equal(scheduledSuccess.report.outcome,'PRODUCED_TO_SHORT_REQUIRED');assert.equal(scheduledSuccess.calls.filter(r=>r.url.endsWith('/advance')).length,1);
 for(const change of [{AIVE_PRODUCTION_GRANT_ID:''},{AIVE_PRODUCTION_GRANT_ID:undefined},{AIVE_PRODUCTION_MODE:'mock'},{AIVE_BROADCAST_PRODUCTION_MACHINE_SECRET:''}]){const r=await scenario({env:{...scheduled,...change}});assert.equal(r.calls.length,0);assert.equal(r.exitCode,1);}
 for(const outcome of ['NO_ELIGIBLE_WORK','BLOCKED_GRANT','BLOCKED_COORDINATION_NOT_SHARED','UNKNOWN','FAILED_DEFINITE']){
  const reply={...pre,production_enabled:false,outcome};
  for(let invocation=0;invocation<2;invocation++){const r=await scenario({env:scheduled,pre:reply});assert.equal(r.calls.length,1);assert.equal(r.exitCode,outcome==='NO_ELIGIBLE_WORK'?0:1);}
 }
 for(const options of [{error:true},{done:{...done,outcome:'UNKNOWN',final_state:'BLOCKED'}}]){const r=await scenario({env:scheduled,...options});assert.equal(r.report.outcome,'UNKNOWN');assert.equal(r.exitCode,1);assert.equal(r.calls.length,2);}
 const good=await scenario();assert.equal(good.exitCode,0);assert.equal(good.report.outcome,'PRODUCED_TO_SHORT_REQUIRED');assert.equal(good.calls.length,2);assert.equal(good.calls[1].timeout_ms,330000);
 for(const r of good.calls){assert.equal(r.redirect,'error');assert.equal(r.headers.Authorization,'Bearer '+env.AIVE_BROADCAST_PRODUCTION_MACHINE_SECRET);assert.deepEqual(Object.keys(JSON.parse(r.body)).sort(),['batch_id','contract','grant_id','operational_run_id']);}
 for(const change of [{GITHUB_ACTIONS:'false'},{GITHUB_EVENT_NAME:'push'},{GITHUB_EVENT_NAME:'pull_request'},{GITHUB_EVENT_NAME:'repository_dispatch'},{GITHUB_EVENT_NAME:''},{AIVE_PRODUCTION_MODE:'mock'},{AIVE_PRODUCTION_MODE:''},{AIVE_BROADCAST_PRODUCTION_MACHINE_SECRET:''},{AIVE_PRODUCTION_GRANT_ID:'bad'}]){const r=await scenario({env:{...env,...change}});assert.equal(r.calls.length,0);assert.equal(r.exitCode,1);}
 for(const invalid of [null,{...pre,extra:'secret'},{...pre,grant_valid:false},{...pre,eligible_work:false},{...pre,coordination:'LOCAL'},{...pre,initial_state:'SHORT_REQUIRED'},done]){const r=await scenario({pre:invalid});assert.equal(r.calls.length,1);assert.equal(r.exitCode,1);}
 const noop=await scenario({pre:{...pre,production_enabled:false,outcome:'NO_ELIGIBLE_WORK'}});assert.equal(noop.exitCode,0);assert.equal(noop.calls.length,1);
 for(const invalid of [null,{...done,extra:'private URL'},{...done,final_state:'OWNER_REVIEW_REQUIRED'},{...done,outcome:'invented'},{...done,production_enabled:true}]){const r=await scenario({done:invalid});assert.equal(r.report.outcome,'UNKNOWN');assert.equal(r.report.reason,'TRANSPORT_UNCERTAIN');assert.equal(r.calls.length,2);assert.equal(r.exitCode,1);}
 for(const options of [{error:true},{status:500}]){const r=await scenario(options);assert.equal(r.report.outcome,'UNKNOWN');assert.equal(r.calls.length,2);assert.ok(!JSON.stringify(r.report).includes(env.AIVE_BROADCAST_PRODUCTION_MACHINE_SECRET));}
 const owner=await scenario({done:{...done,outcome:'PRODUCED_TO_OWNER_GATE',final_state:'OWNER_REVIEW_REQUIRED'}});assert.equal(owner.exitCode,1);
 for(const origin of ['https://evil.invalid','https://aive.global/path','http://aive.global','https://aive.global:444']){let calls=0;await runProduction({origin,workerToken:env.AIVE_BROADCAST_PRODUCTION_MACHINE_SECRET,grantId:env.AIVE_PRODUCTION_GRANT_ID,runId:'123:1'},async()=>{calls++;throw Error()});assert.equal(calls,0);}
 let fetches=0;
 const wire=httpTransport(async(url,init)=>{fetches++;assert.equal(init?.redirect,'error');assert.ok(init?.signal);return new Response(JSON.stringify(String(url).endsWith('/preflight')?pre:done),{status:200})});
 assert.equal((await runManual(env,wire)).exitCode,0);assert.equal(fetches,2);
 await assert.rejects(()=>wire({...good.calls[0],url:'https://evil.invalid'}));assert.equal(fetches,2);
 // Exercise actual AbortController cancellation without waiting 330 seconds.
 fetches=0;const timeout=httpTransport(async(url,init)=>{fetches++;if(String(url).endsWith('/preflight'))return new Response(JSON.stringify(pre));return new Promise((_resolve,reject)=>init!.signal!.addEventListener('abort',()=>reject(Error('fixture timeout')),{once:true}))});
 const timed=await runManual(env,r=>timeout({...r,timeout_ms:10}));assert.equal(timed.report.outcome,'UNKNOWN');assert.equal(timed.report.reason,'TRANSPORT_UNCERTAIN');assert.equal(fetches,2);
 fetches=0;const badJson=httpTransport(async()=>{fetches++;return new Response(fetches===1?JSON.stringify(pre):'not json')});assert.equal((await runManual(env,badJson)).report.outcome,'UNKNOWN');assert.equal(fetches,2);
 assert.ok(!JSON.stringify(good.report).includes(env.AIVE_BROADCAST_PRODUCTION_MACHINE_SECRET));
 console.log('PASS: live-mode mocked HTTP, auth/input/mode gates, strict Still success, fixed target, abort timeout, malformed responses, no-op and no retries; real requests=0');
}
main().catch(error=>{console.error(String(error?.stack??'Offline test failure').split(env.AIVE_BROADCAST_PRODUCTION_MACHINE_SECRET).join('[REDACTED FIXTURE]'));process.exitCode=1});
