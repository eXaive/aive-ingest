/** Scope-only orchestration. AIVE owns selection, claims, generation and media. */
export const TARGET=Object.freeze({batch_id:'522f97ea-9397-4045-a810-27086b69dc9b'});
export const CONTRACT='AIVE_CONTENT_PRODUCTION_CRON_V1';
const outcomes=['PRODUCED_TO_OWNER_GATE','NO_ELIGIBLE_WORK','BLOCKED_COORDINATION_NOT_SHARED','BLOCKED_GRANT','CLAIM_LOST','FAILED_DEFINITE','UNKNOWN'] as const;
export type Outcome=typeof outcomes[number];
type State='STILL_REQUIRED'|'SHORT_REQUIRED'|'OWNER_REVIEW_REQUIRED'|'BLOCKED';
export type Report={outcome:Outcome;reason:'AIVE_RESULT'|'CONTRACT_INVALID'|'TRANSPORT_UNCERTAIN'|'CONFIGURATION'|'LIVE_TRANSPORT_DISABLED';batch:string;run_id?:string;spec_id?:string;initial_state?:State;final_state?:State;elapsed_ms:number};
export type Request={method:'POST';url:string;headers:Record<string,string>;body:string;timeout_ms:number;redirect:'error'};
export type Transport=(request:Request)=>Promise<{status:number;body:unknown}>;
type Config={origin:string;workerToken:string;grantId:string;runId:string};
const uuid=(v:unknown):v is string=>typeof v==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v);
type Reply={contract:typeof CONTRACT;batch_id:string;coordination:'SHARED_TRANSACTIONAL'|'LOCAL'|'UNAVAILABLE';production_enabled:boolean;eligible_work:boolean;grant_valid:boolean;outcome:Outcome|null;spec_id:string|null;episode:number|null;initial_state:'STILL_REQUIRED'|'SHORT_REQUIRED'|null;final_state:State|null};
function parse(input:unknown):Reply {
 if(!input||typeof input!=='object'||Array.isArray(input))throw Error('invalid');const r=input as Reply;
 const keys=['contract','batch_id','coordination','production_enabled','eligible_work','grant_valid','outcome','spec_id','episode','initial_state','final_state'];
 if(Object.keys(r).length!==keys.length||keys.some(k=>!(k in r))||r.contract!==CONTRACT||r.batch_id!==TARGET.batch_id
  ||!['SHARED_TRANSACTIONAL','LOCAL','UNAVAILABLE'].includes(r.coordination)||['production_enabled','eligible_work','grant_valid'].some(k=>typeof (r as unknown as Record<string,unknown>)[k]!=='boolean')
  ||(r.outcome!==null&&!outcomes.includes(r.outcome))||(r.spec_id!==null&&!uuid(r.spec_id))||(r.episode!==null&&(!Number.isInteger(r.episode)||r.episode<1))
  ||(r.initial_state!==null&&!['STILL_REQUIRED','SHORT_REQUIRED'].includes(r.initial_state))||(r.final_state!==null&&!['STILL_REQUIRED','SHORT_REQUIRED','OWNER_REVIEW_REQUIRED','BLOCKED'].includes(r.final_state)))throw Error('invalid');
 if(r.production_enabled&&(r.coordination!=='SHARED_TRANSACTIONAL'||!r.grant_valid||!r.eligible_work||r.spec_id===null||r.episode===null||r.initial_state===null||r.outcome!==null))throw Error('invalid');
 if(r.outcome==='PRODUCED_TO_OWNER_GATE'&&(r.final_state!=='OWNER_REVIEW_REQUIRED'||!r.spec_id||r.episode===null))throw Error('invalid');return r;
}
export async function runProduction(c:Config,transport:Transport):Promise<Report> {
 const started=Date.now();const report=(outcome:Outcome,reason:Report['reason'],r?:Reply):Report=>({outcome,reason,batch:TARGET.batch_id,
  ...(/^[0-9]{1,24}:[0-9]{1,4}$/.test(c.runId)?{run_id:c.runId}:{}),...(r?.spec_id?{spec_id:r.spec_id}:{}),...(r?.initial_state?{initial_state:r.initial_state}:{}),...(r?.final_state?{final_state:r.final_state}:{}),elapsed_ms:Date.now()-started});
 try {const u=new URL(c.origin);if(u.origin!==c.origin||u.protocol!=='https:'||u.hostname!=='aive.global'||u.username||u.password||!uuid(c.grantId)||! /^[0-9]{1,24}:[0-9]{1,4}$/.test(c.runId)||! /^[!-~]{32,4096}$/.test(c.workerToken))throw Error();}
 catch{return report('BLOCKED_COORDINATION_NOT_SHARED','CONFIGURATION');}
 const call=async(operation:'preflight'|'advance',timeout:number)=>{let timer:ReturnType<typeof setTimeout>|undefined;
  try{return await Promise.race([transport({method:'POST',url:c.origin+'/api/broadcast/content-production/worker/'+operation,headers:{Authorization:'Bearer '+c.workerToken,'Content-Type':'application/json'},redirect:'error',timeout_ms:timeout,
   body:JSON.stringify({contract:CONTRACT,batch_id:TARGET.batch_id,grant_id:c.grantId,operational_run_id:c.runId})}),new Promise<never>((_,reject)=>{timer=setTimeout(()=>reject(Error('timeout')),timeout)})]);}finally{if(timer)clearTimeout(timer);}};
 try{const p=await call('preflight',15000);if(p.status===401||p.status===403)return report('BLOCKED_GRANT','AIVE_RESULT');if(p.status!==200)return report('BLOCKED_COORDINATION_NOT_SHARED','CONTRACT_INVALID');const r=parse(p.body);
  if(!r.production_enabled)return report(r.outcome??'BLOCKED_COORDINATION_NOT_SHARED','AIVE_RESULT',r);
 }catch{return report('BLOCKED_COORDINATION_NOT_SHARED','CONTRACT_INVALID');}
 try{const p=await call('advance',480000);if(p.status!==200)return report('UNKNOWN','TRANSPORT_UNCERTAIN');const r=parse(p.body);if(r.production_enabled||r.outcome===null)throw Error();return report(r.outcome,'AIVE_RESULT',r);}
 catch{return report('UNKNOWN','TRANSPORT_UNCERTAIN');}
}
export function liveDisabled():Report{return{outcome:'BLOCKED_COORDINATION_NOT_SHARED',reason:'LIVE_TRANSPORT_DISABLED',batch:TARGET.batch_id,elapsed_ms:0};}
if(require.main===module){console.log(JSON.stringify(liveDisabled()));process.exitCode=1;}
