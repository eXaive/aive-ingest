import { q } from '../ingest/db';
export type AttemptState='reserved'|'submitting'|'provider_accepted'|'confirmation_pending'|'confirmed_published'|'provider_failed'|'outcome_unknown'|'simulated';
export interface Attempt {id:string;intent_key:string;account_id:string;state:AttemptState;provider_submission_id:string|null;job_id:string|null;lease_owner:string|null;created_at?:string|null}
export async function nextPublicationSlot(sourceKind:string,sourceId:string,purpose:string){const rows=await q<{n:string}>("select count(*)::text n from broadcast_publication_attempts where source_kind=$1 and source_id=$2 and purpose=$3 and state='confirmed_published'",[sourceKind,sourceId,purpose]);return `cycle-${Number(rows[0]?.n??'0')+1}`}
export async function claimPublication(input:{intentKey:string;sourceKind:string;sourceId:string;purpose:string;slotKey:string;accountId:string;leaseOwner:string;dailyCap?:number}){const rows=await q<Attempt>('select * from claim_broadcast_publication($1,$2,$3,$4,$5,$6,$7,$8,$9)',[input.intentKey,input.sourceKind,input.sourceId,input.purpose,input.slotKey,input.accountId,input.leaseOwner,900,input.dailyCap??null]);return rows[0]}
export async function markConfirmationPending(id:string,submissionId:string,jobId:string){await q("update broadcast_publication_attempts set state='confirmation_pending',provider_submission_id=$2,job_id=$3,last_confirmation_at=now(),updated_at=now() where id=$1 and state in ('provider_accepted','confirmation_pending')",[id,submissionId,jobId])}
export async function markConfirmed(id:string,publicUrl:string|null){await q('select confirm_broadcast_publication($1,$2)',[id,publicUrl])}
export async function markConfirmationFailed(id:string,error:string){await q("update broadcast_publication_attempts set state='provider_failed',failed_at=now(),last_confirmation_at=now(),last_error=left($2,500),updated_at=now() where id=$1 and state in ('provider_accepted','confirmation_pending')",[id,error])}
export async function touchConfirmationPending(id:string,error:string){await q("update broadcast_publication_attempts set state='confirmation_pending',last_confirmation_at=now(),last_error=left($2,500),updated_at=now() where id=$1 and state in ('provider_accepted','confirmation_pending')",[id,error])}
/** A worker that finds someone else's unconfirmed attempt on its intent cannot post until that attempt is resolved
 * (the source is only marked used on confirmation, so every later run lands on the same intent). Inside this window it
 * may still be in flight; past it, it is a stall and the run must fail loudly so the failure alert opens an issue. */
export const STALL_AFTER_MS=6*60*60*1000;
/** null while the existing attempt is confirmed or younger than STALL_AFTER_MS; otherwise the failure message. */
export function stalledAttemptMessage(attempt:Pick<Attempt,'id'|'state'>&{created_at?:string|null},now:number=Date.now()):string|null{
  if(attempt.state==='confirmed_published')return null;
  const at=Date.parse(String(attempt.created_at??''));
  if(!Number.isFinite(at))return `STALLED_PUBLICATION: existing attempt ${attempt.id} (state=${attempt.state}) has no readable created_at; resolve it before this worker can post`;
  const age=now-at;if(age<=STALL_AFTER_MS)return null;
  return `STALLED_PUBLICATION: existing attempt ${attempt.id} has been ${attempt.state} for ${Math.floor(age/3_600_000)}h (limit ${STALL_AFTER_MS/3_600_000}h); this worker posts nothing until it is reconciled (npm run broadcast:reconcile) or resolved by hand`;
}
