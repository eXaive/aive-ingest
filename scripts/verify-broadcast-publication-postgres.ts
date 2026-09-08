import assert from 'node:assert/strict';
import fs from 'node:fs';
import {Pool,PoolClient} from 'pg';

const url=process.env.BROADCAST_TEST_DATABASE_URL;
if(!url||!url.includes('127.0.0.1:63255/'))throw new Error('refusing non-isolated BROADCAST_TEST_DATABASE_URL');
const pool=new Pool({connectionString:url,max:12});
const root='C:/GMMF/AIVE_Investor_MVP_CLEAN';
const id=()=>crypto.randomUUID();
async function claim(c:PoolClient,x:{intent:string;sourceId:string;account:string;owner:string;cap?:number}){
  const r=await c.query(`select (claim_broadcast_publication($1,'quiz_question',$2,'quiz-card','cycle-1',$3,$4,30,$5)).*`,[x.intent,x.sourceId,x.account,x.owner,x.cap??null]);return r.rows[0];
}
async function roleDenied(role:string,sql:string){const c=await pool.connect();try{await c.query('begin');await c.query(`set local role ${role}`);await assert.rejects(c.query(sql),/permission denied/i);await c.query('rollback')}finally{c.release()}}

async function main(){
  await pool.query(fs.readFileSync(`${root}/scripts/broadcast-publication-postgres-prerequisites.sql`,'utf8'));
  await pool.query(fs.readFileSync(`${root}/supabase/migrations/20260908020000_broadcast_publication_attempts.sql`,'utf8'));
  const account=id(),question=id();await pool.query(`insert into broadcast_accounts(id) values($1)`,[account]);await pool.query(`insert into quiz_questions(id) values($1)`,[question]);

  const c1=await pool.connect(),c2=await pool.connect();
  const [a,b]=await Promise.all([claim(c1,{intent:'same',sourceId:question,account,owner:'one',cap:3}),claim(c2,{intent:'same',sourceId:question,account,owner:'two',cap:3})]);c1.release();c2.release();
  assert.equal(a.id,b.id);assert.equal(a.lease_owner,b.lease_owner);assert.equal((await pool.query(`select count(*)::int n from broadcast_publication_attempts where intent_key='same'`)).rows[0].n,1);

  await pool.query(`update broadcast_publication_attempts set state='provider_accepted',provider_submission_id='p1' where id=$1`,[a.id]);
  const acceptedClient=await pool.connect();try{const retained=await claim(acceptedClient,{intent:'same',sourceId:question,account,owner:'three'});assert.equal(retained.state,'provider_accepted');assert.equal(retained.lease_owner,a.lease_owner)}finally{acceptedClient.release()}
  await pool.query(`update broadcast_publication_attempts set state='outcome_unknown' where id=$1`,[a.id]);const unknownClient=await pool.connect();try{assert.equal((await claim(unknownClient,{intent:'same',sourceId:question,account,owner:'four'})).lease_owner,a.lease_owner)}finally{unknownClient.release()}

  const exp=await pool.connect();let expired:any;try{expired=await claim(exp,{intent:'expired',sourceId:question,account,owner:'old'});await pool.query(`update broadcast_publication_attempts set lease_expires_at=now()-interval '1 second' where id=$1`,[expired.id]);assert.equal((await claim(exp,{intent:'expired',sourceId:question,account,owner:'new'})).lease_owner,'new')}finally{exp.release()}
  await pool.query(`update broadcast_publication_attempts set state='submitting',lease_expires_at=now()-interval '1 second' where id=$1`,[expired.id]);const sub=await pool.connect();try{const retained=await claim(sub,{intent:'expired',sourceId:question,account,owner:'newer'});assert.equal(retained.state,'submitting');assert.equal(retained.lease_owner,'new')}finally{sub.release()}

  const confirmId=id();await pool.query(`insert into broadcast_publication_attempts(intent_key,source_kind,source_id,purpose,slot_key,account_id,state) values('confirm','quiz_question',$1,'quiz-card','cycle-1',$2,'confirmation_pending')`,[confirmId,account]);await pool.query(`insert into quiz_questions(id) values($1)`,[confirmId]);
  await pool.query(`select confirm_broadcast_publication((select id from broadcast_publication_attempts where intent_key='confirm'),'https://example.invalid/post')`);let qr=await pool.query(`select a.state,a.public_url,q.used_as_quiz_card,q.quiz_posted_at from broadcast_publication_attempts a join quiz_questions q on q.id=a.source_id::uuid where a.intent_key='confirm'`);assert.equal(qr.rows[0].state,'confirmed_published');assert.equal(qr.rows[0].used_as_quiz_card,true);const posted=qr.rows[0].quiz_posted_at;
  await pool.query(`select confirm_broadcast_publication((select id from broadcast_publication_attempts where intent_key='confirm'),'https://changed.invalid')`);qr=await pool.query(`select a.public_url,q.quiz_posted_at from broadcast_publication_attempts a join quiz_questions q on q.id=a.source_id::uuid where a.intent_key='confirm'`);assert.equal(qr.rows[0].public_url,'https://example.invalid/post');assert.equal(+qr.rows[0].quiz_posted_at,+posted);

  await pool.query(`insert into broadcast_publication_attempts(intent_key,source_kind,source_id,purpose,slot_key,account_id,state) values('rollback','quiz_question','not-a-uuid','quiz-card','cycle-1',$1,'confirmation_pending')`,[account]);await assert.rejects(pool.query(`select confirm_broadcast_publication((select id from broadcast_publication_attempts where intent_key='rollback'),null)`),/uuid/i);assert.equal((await pool.query(`select state from broadcast_publication_attempts where intent_key='rollback'`)).rows[0].state,'confirmation_pending');

  const account2=id();await pool.query(`insert into broadcast_accounts(id) values($1)`,[account2]);const m1=await pool.connect(),m2=await pool.connect();try{const [x,y]=await Promise.all([claim(m1,{intent:'multi',sourceId:question,account,owner:'m1'}),claim(m2,{intent:'multi',sourceId:question,account:account2,owner:'m2'})]);assert.notEqual(x.id,y.id);await pool.query(`update broadcast_publication_attempts set state='provider_accepted' where id=$1`,[x.id]);await pool.query(`update broadcast_publication_attempts set state='provider_failed' where id=$1`,[y.id]);const states=await pool.query(`select account_id,state from broadcast_publication_attempts where intent_key='multi' order by account_id`);assert.equal(new Set(states.rows.map(r=>r.state)).size,2)}finally{m1.release();m2.release()}

  const capAccount=id();await pool.query(`insert into broadcast_accounts(id) values($1)`,[capAccount]);const clients=await Promise.all([0,1,2,3].map(()=>pool.connect()));try{const rows=await Promise.all(clients.map((c,i)=>claim(c,{intent:`cap-${i}`,sourceId:question,account:capAccount,owner:`c${i}`,cap:2})));assert.equal(rows.filter(r=>r.id).length,2)}finally{clients.forEach(c=>c.release())}

  const dedupeAccount=id(),job=id();await pool.query(`insert into broadcast_accounts(id) values($1)`,[dedupeAccount]);await pool.query(`insert into broadcast_jobs(id,status) values($1,'posted')`,[job]);await pool.query(`insert into broadcast_job_accounts(job_id,account_id) values($1,$2)`,[job,dedupeAccount]);await pool.query(`insert into broadcast_publication_attempts(intent_key,source_kind,source_id,purpose,slot_key,account_id,job_id,state,capacity_day) values('linked','quiz_question',$1,'quiz-card','cycle-1',$2,$3,'confirmed_published',(now() at time zone 'UTC')::date)`,[question,dedupeAccount,job]);const dc=await pool.connect();try{assert.ok((await claim(dc,{intent:'dedupe-second',sourceId:question,account:dedupeAccount,owner:'d',cap:2})).id);assert.equal((await claim(dc,{intent:'dedupe-third',sourceId:question,account:dedupeAccount,owner:'d2',cap:2})).id,null)}finally{dc.release()}

  const midnightAccount=id(),midQuestion=id();await pool.query(`insert into broadcast_accounts(id) values($1)`,[midnightAccount]);await pool.query(`insert into quiz_questions(id) values($1)`,[midQuestion]);await pool.query(`insert into broadcast_publication_attempts(intent_key,source_kind,source_id,purpose,slot_key,account_id,state,capacity_day,created_at) values('midnight','quiz_question',$1,'quiz-card','cycle-1',$2,'confirmation_pending',(now() at time zone 'UTC')::date-1,now()-interval '1 day')`,[midQuestion,midnightAccount]);const mc=await pool.connect();try{assert.ok((await claim(mc,{intent:'mid-2',sourceId:question,account:midnightAccount,owner:'x',cap:3})).id);assert.ok((await claim(mc,{intent:'mid-3',sourceId:question,account:midnightAccount,owner:'y',cap:3})).id);assert.equal((await claim(mc,{intent:'mid-4',sourceId:question,account:midnightAccount,owner:'z',cap:3})).id,null);await pool.query(`select confirm_broadcast_publication((select id from broadcast_publication_attempts where intent_key='midnight'),null)`);assert.equal((await pool.query(`select capacity_day=(now() at time zone 'UTC')::date today from broadcast_publication_attempts where intent_key='midnight'`)).rows[0].today,true);assert.equal((await claim(mc,{intent:'mid-after-confirm',sourceId:question,account:midnightAccount,owner:'z2',cap:3})).id,null)}finally{mc.release()}

  for(const role of ['anon','authenticated']){await roleDenied(role,`select claim_broadcast_publication('denied','quiz_question','${question}','quiz-card','cycle-1','${account}','bad',30,3)`);await roleDenied(role,`select confirm_broadcast_publication('${a.id}',null)`);await roleDenied(role,`update quiz_questions set used_as_quiz_card=true where id='${question}'`)}
  const defs=await pool.query(`select p.proname,p.prosecdef,pg_get_functiondef(p.oid) def,coalesce(array_to_string(p.proacl,','),'') acl from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname='public' and p.proname in ('claim_broadcast_publication','confirm_broadcast_publication') order by p.proname`);assert.equal(defs.rows.length,2);for(const d of defs.rows){assert.equal(d.prosecdef,false);assert.match(d.def,/SET search_path TO 'public'/);assert.doesNotMatch(d.acl,/PUBLIC|anon|authenticated/)}
  console.log('REAL POSTGRES: all broadcast publication migration tests passed');
}
main().finally(()=>pool.end());
