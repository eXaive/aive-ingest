import {runProduction,liveDisabled,type Transport,type Report} from './broadcast-content-production';
const ORIGIN='https://aive.global';
/** No redirects/retries; abort covers response body consumption as well as headers. */
export function httpTransport(fetcher:typeof fetch=fetch):Transport {
 return async request=>{
  if(!['preflight','advance'].some(op=>request.url===ORIGIN+'/api/broadcast/content-production/worker/'+op)||request.method!=='POST'||request.redirect!=='error'||request.timeout_ms<1||request.timeout_ms>330000)throw Error('Invalid transport scope');
  const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),request.timeout_ms);
  try {
   const response=await fetcher(request.url,{method:'POST',headers:request.headers,body:request.body,redirect:'error',signal:controller.signal});
   const raw=await response.text();if(raw.length>65536)throw Error('Invalid response size');
   return {status:response.status,body:JSON.parse(raw)};
  } finally {clearTimeout(timer);}
 };
}
export async function runManual(env:NodeJS.ProcessEnv,transport:Transport=httpTransport()):Promise<{report:Report;exitCode:number}> {
 if(env.GITHUB_ACTIONS!=='true'||!['workflow_dispatch','schedule'].includes(env.GITHUB_EVENT_NAME??'')||env.AIVE_PRODUCTION_MODE!=='live')return {report:liveDisabled(),exitCode:1};
 const report=await runProduction({origin:ORIGIN,workerToken:env.AIVE_BROADCAST_PRODUCTION_MACHINE_SECRET??'',grantId:env.AIVE_PRODUCTION_GRANT_ID??'',runId:(env.GITHUB_RUN_ID??'')+':'+(env.GITHUB_RUN_ATTEMPT??'')},async request=>{
  const response=await transport(request);
  // This entry point accepts a Still start only; AIVE owns actual grant enforcement.
  if(request.url.endsWith('/preflight')&&response.body&&typeof response.body==='object') {
   const r=response.body as Record<string,unknown>;
   if(r.production_enabled===true&&r.initial_state!=='STILL_REQUIRED')throw Error('Still start required');
  }
  return response;
 });
 return {report,exitCode:report.outcome==='PRODUCED_TO_SHORT_REQUIRED'||report.outcome==='NO_ELIGIBLE_WORK'?0:1};
}
if(require.main===module){
 if(process.argv.length!==3||process.argv[2]!=='--live'){console.log(JSON.stringify(liveDisabled()));process.exitCode=1;}
 else runManual(process.env).then(({report,exitCode})=>{console.log(JSON.stringify(report));process.exitCode=exitCode}).catch(()=>{console.error('Manual production failed closed');process.exitCode=1;});
}
