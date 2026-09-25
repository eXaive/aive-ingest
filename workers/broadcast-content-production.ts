/** Clock/orchestration only. Proposed AIVE V1 boundary; no live transport is enabled. */
export const TARGET = Object.freeze({ channel: 'space-explorers-club', batch_id: '522f97ea-9397-4045-a810-27086b69dc9b', first_episode: 14, last_episode: 30, max_specs: 1 });
export const CONTRACT = 'AIVE_CONTENT_PRODUCTION_CRON_V1';
const outcomes = ['PRODUCED_TO_OWNER_GATE','NO_ELIGIBLE_WORK','BLOCKED_COORDINATION_NOT_SHARED','BLOCKED_GRANT','CLAIM_LOST','FAILED_DEFINITE','UNKNOWN'] as const;
export type Outcome = typeof outcomes[number];
const states = ['STILL_REQUIRED','SHORT_REQUIRED','OWNER_REVIEW_REQUIRED','BLOCKED'] as const;
type State = typeof states[number];
export type Report = { outcome: Outcome; reason: 'AIVE_RESULT' | 'CONTRACT_INVALID' | 'TRANSPORT_UNCERTAIN' | 'COORDINATION' | 'CONFIGURATION' | 'MISSING_AIVE_WORKER_CONTRACT'; batch: string; run_id?: string; spec_id?: string; initial_state?: State; final_state?: State; elapsed_ms: number };
export type Request = { method: 'POST'; url: string; headers: Record<string,string>; body: string; timeout_ms: number; redirect: 'error' };
/** Test seam only. A future live transport must honor deadline, reject redirects and never retry. */
export type Transport = (request: Request) => Promise<{ status: number; body: unknown }>;
type Config = { origin: string; workerToken: string; grantId: string; runId: string };
const uuid = (v: unknown): v is string => typeof v === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v);
function object(v: unknown): Record<string,unknown> { if (!v || typeof v !== 'object' || Array.isArray(v)) throw Error('invalid'); return v as Record<string,unknown>; }
function exact(o: Record<string,unknown>, keys: string[]) { if (Object.keys(o).some(k => !keys.includes(k)) || keys.some(k => !(k in o))) throw Error('invalid'); }
function envelope(o: Record<string,unknown>) { if (o.contract !== CONTRACT || o.channel !== TARGET.channel || o.batch_id !== TARGET.batch_id) throw Error('invalid'); }
function configValid(c: Config) {
  const u = new URL(c.origin);
  return u.origin === c.origin && u.protocol === 'https:' && u.hostname === 'aive.global' && !u.username && !u.password
    && uuid(c.grantId) && /^[0-9]{1,24}:[0-9]{1,4}$/.test(c.runId) && /^[\x21-\x7e]{20,4096}$/.test(c.workerToken);
}
/** One AIVE selection, then at most one AIVE-owned full-episode advance. No local production identity. */
export async function runProduction(c: Config, transport: Transport): Promise<Report> {
  const started = Date.now(); let spec: string | undefined, initial: State | undefined;
  const report = (outcome: Outcome, reason: Report['reason'], final?: State): Report => ({ outcome, reason, batch: TARGET.batch_id,
    ...(/^[0-9]{1,24}:[0-9]{1,4}$/.test(c.runId) ? {run_id:c.runId} : {}), ...(spec ? {spec_id:spec} : {}),
    ...(initial ? {initial_state:initial} : {}), ...(final ? {final_state:final} : {}), elapsed_ms:Date.now()-started });
  try { if (!configValid(c)) return report('BLOCKED_COORDINATION_NOT_SHARED','CONFIGURATION'); } catch { return report('BLOCKED_COORDINATION_NOT_SHARED','CONFIGURATION'); }
  const call = async (operation: 'preflight'|'advance', extra: Record<string,unknown>, timeout: number) => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try { return await Promise.race([transport({method:'POST',url:c.origin+'/api/broadcast/content-production/worker/'+operation,
      headers:{Authorization:'Bearer '+c.workerToken,'Content-Type':'application/json'}, redirect:'error',timeout_ms:timeout,
      body:JSON.stringify({contract:CONTRACT,...TARGET,grant_id:c.grantId,operational_run_id:c.runId,...extra})}),
      new Promise<never>((_,reject)=>{timer=setTimeout(()=>reject(Error('timeout')),timeout)})]); }
    finally { if(timer) clearTimeout(timer); }
  };
  let pre: Record<string,unknown>;
  try {
    const response=await call('preflight',{},15000);
    if(response.status===401||response.status===403)return report('BLOCKED_GRANT','AIVE_RESULT');
    if(response.status!==200)return report('BLOCKED_COORDINATION_NOT_SHARED','COORDINATION');
    pre=object(response.body);exact(pre,['contract','channel','batch_id','coordination','worker_contract_ready','grant_valid','selection']);envelope(pre);
    if(pre.coordination!=='SHARED_TRANSACTIONAL'||pre.worker_contract_ready!==true)return report('BLOCKED_COORDINATION_NOT_SHARED','COORDINATION');
    if(pre.grant_valid!==true)return report('BLOCKED_GRANT','AIVE_RESULT');
    if(pre.selection===null)return report('NO_ELIGIBLE_WORK','AIVE_RESULT');
    const selected=object(pre.selection);exact(selected,['spec_id','spec_revision','episode','initial_state']);
    if(!uuid(selected.spec_id)||!Number.isInteger(selected.spec_revision)||Number(selected.spec_revision)<1||!Number.isInteger(selected.episode)
      ||Number(selected.episode)<14||Number(selected.episode)>30||!['STILL_REQUIRED','SHORT_REQUIRED'].includes(String(selected.initial_state)))throw Error('invalid');
    spec=selected.spec_id;initial=selected.initial_state as State;
  } catch {return report('BLOCKED_COORDINATION_NOT_SHARED','CONTRACT_INVALID');}
  try {
    const response=await call('advance',{selection:pre.selection},8*60*1000);
    // Anything unclassified after invoking production is uncertain; never retry.
    if(response.status!==200)return report('UNKNOWN','TRANSPORT_UNCERTAIN');
    const body=object(response.body);exact(body,['contract','channel','batch_id','spec_id','spec_revision','outcome','final_state']);envelope(body);
    if(body.spec_id!==spec||body.spec_revision!==object(pre.selection).spec_revision||!outcomes.includes(body.outcome as Outcome)
      ||(body.final_state!==null&&!states.includes(body.final_state as State))||body.outcome==='NO_ELIGIBLE_WORK')throw Error('invalid');
    if(body.outcome==='PRODUCED_TO_OWNER_GATE'&&body.final_state!=='OWNER_REVIEW_REQUIRED')throw Error('invalid');
    return report(body.outcome as Outcome,'AIVE_RESULT',body.final_state===null?undefined:body.final_state as State);
  } catch {return report('UNKNOWN','TRANSPORT_UNCERTAIN');}
}
export function liveDisabled(): Report { return {outcome:'BLOCKED_COORDINATION_NOT_SHARED',reason:'MISSING_AIVE_WORKER_CONTRACT',batch:TARGET.batch_id,elapsed_ms:0}; }
if(require.main===module) {
  // Deliberate hard gate: no env switch, fetch, owner credential or live client until AIVE implements the contract.
  console.log(JSON.stringify(liveDisabled()));process.exitCode=1;
}
