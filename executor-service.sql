-- CONNECTION: url=jdbc:postgresql://localhost:5432/work
create schema if not exists raft;

drop table raft.executor_service

create table raft.executor_service(
  uuid uuid primary key,
  depends_on uuid[],
  run_at timestamptz default now(),
  fn varchar(129) not null,
  params jsonb,
  context jsonb,
  error_message text,
  sqlstate varchar(6),
  is_done boolean not null default false
);
create index on raft.executor_service(uuid) where not is_done;
create index on raft.executor_service using gin(depends_on);
create unique index exec_uniq on raft.executor_service((md5(fn||':'||(coalesce(params,'null')::text))));

delete from raft.executor_service


select md5(jsonb_build_object('key',10,'val','dodo')::text), md5(jsonb_build_object('val','dodo','key',10)::text)

do $code$
begin
call raft.worker();
end;
$code$

select * from raft.executor_service



select coalesce(dblink.dblink_get_connections(),array[]::text[])

create or replace procedure raft.run_workers(num_workers int, self_conn_str text) as
$code$
declare
 cns text[];
 r record;
 cn text;
begin
    for r in select n from generate_series(1,num_workers) as gs(n) loop
        cn='executor_service_worker'||r.n;
        if cn=any(coalesce(dblink.dblink_get_connections(),array[]::text[])) then
            perform dblink.dblink_disconnect(cn);
        end if;
        raise notice 'establish connection to %', cn;
        perform dblink.dblink_connect(cn, self_conn_str);
        perform dblink.dblink_send_query(cn, 'call raft.worker()');
        cns=cns||cn;
    end loop;
--
    while true loop
        perform pg_sleep(5);
        foreach cn in array cns loop
            if dblink.dblink_is_busy(cn)=0 and dblink.dblink_error_message(cn)<>'OK' then
                raise notice 'errmsg=%', dblink.dblink_error_message(cn);
                perform dblink.dblink_disconnect(cn);
                perform dblink.dblink_connect(cn, self_conn_str);
                perform dblink.dblink_send_query(cn, 'call raft.worker()');
            end if;
        end loop;
        commit;
    end loop;
end;
$code$
language plpgsql;

select * from pg_stat_activity

create or replace procedure raft.worker() as
$code$
<<code>>
declare
 context jsonb;
 r record;
 ss text;
 ok boolean;
 nr interval;
 errm text;
begin
    while true loop
        select * into r from raft.executor_service es 
            where not es.is_done and es.run_at<now() and (es.depends_on is null or 
            not exists(select * from raft.executor_service es1
                where es1.uuid=any(es.depends_on) and not es1.is_done)
               )
        order by uuid for update limit 1;
        if not found then
            commit;
            perform pg_sleep(1);
            continue;
        end if;
        begin
            execute 'select * from '||r.fn||'($1,$2)' using r.params, coalesce(r.context, jsonb_build_object()) into context, nr;
            ok=true;
        exception
            when others then ok=false; ss=sqlstate; errm=sqlerrm;
        end;
--
        if ok then
            update raft.executor_service es 
                    set context=code.context, error_message=code.context->>'error_message', sqlstate=null, 
                    is_done=case when nr is null then true else false end,
                    run_at=clock_timestamp()+nr
                where es.uuid=r.uuid;
            commit;
        else
            rollback;
            update raft.executor_service es set error_message=errm, sqlstate=ss, is_done=true where es.uuid=r.uuid;
        end if;
    end loop;    
end;
$code$
language plpgsql
--set enable_seqscan to false
;



create or replace function raft.submit(fn text, params jsonb, depends_on uuid[] default null::uuid[], run_at timestamptz default '1970-01-01'::timestamptz) returns uuid as
$code$
    insert into raft.executor_service(uuid, fn, params, depends_on, run_at) values(gsp.gen_v7_uuid(), fn, params, depends_on, run_at)
    on conflict do nothing
    returning uuid;
$code$
language sql;

create or replace function raft.clear_jobs(params jsonb, ctx in out jsonb, next_run out interval) as
$code$
begin
    next_run:=make_interval(secs:=60);
    delete from raft.executor_service as es1 where es1.is_done
        and not exists(select * from raft.executor_service es2 where not es2.is_done and es2.depends_on && array[es1.uuid]);
end;
$code$
language plpgsql;



create or replace function raft.es_test(params jsonb, ctx in out jsonb, next_run out interval) as
$code$
declare
 cnt int = coalesce((ctx->>'cnt')::int, 0);
begin
    insert into raft.ll values(format('%s:%s', cnt, params->>'message'));
    if cnt<3 then
        --ctx=jsonb_build_object('cnt',cnt+1, 'error_message', format('%s processed', cnt)); 
        cnt=cnt+1;
        next_run=make_interval(secs:=3);
        ctx['cnt']=cnt;
        ctx['error_message']=to_jsonb(format('%s processed', cnt));
    else
        return;
    end if;
    return;
end
$code$
language plpgsql;

do $code$
declare
 r jsonb; nr interval;
begin
    select * into r, nr from raft.es_test(jsonb_build_object('message','lala'));
    raise notice 'r=% nr=%', r, nr;
end;
$code$

create or replace function raft.es_test1(params jsonb, result out jsonb, next_run out interval) as
$code$
begin
    insert into raft.ll values(params['message']::text);
    --return null;
    result=jsonb_build_object('result', true);
    next_run=make_interval(secs:=3);
end
$code$
language plpgsql;


create table raft.ll(msg text);

select count(*) from raft.ll

select * from raft.ll

select * from raft.executor_service es 

drop FUNCTION raft.submit(fn text, params jsonb)

commit

delete from raft.ll

vacuum verbose raft.ll

insert into raft.executor_service(uuid,fn, params)values(gsp.gen_v7_uuid(), 'raft.es_test', jsonb_build_object('message','"message"'));

select raft.submit('raft.es_test', jsonb_build_object('message','next attempt'))

select raft.submit('raft.clear_jobs', jsonb_build_object())

select * from raft.executor_service es 



select * from raft.executor_service

select count(*) from raft.executor_service

select * from raft.executor_service

select to_json(format('%s rows processed', 10))



        select * from raft.executor_service es 
            where not es.is_done and es.run_at<now() and (es.depends_on is null or 
            not exists(select * from raft.executor_service es1
                where es1.depends_on=es.uuid and not es1.is_done)
               )
        order by uuid for update limit 1;

do $code$
declare
 v jsonb;
 res interval;
begin
    select * into v, res from raft.es_test1(jsonb_build_object('message','laladodo'));
    raise notice 'v=% res=%', v, res;
end;
$code$


select to_json(row(gen_random_uuid()))

select gen_random_uuid()::text::jsonb;