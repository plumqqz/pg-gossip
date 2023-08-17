create schema if not exists raft;



create unlogged table raft.executor_service(
  uuid uuid primary key,
  fn varchar(129) not null,
  params jsonb,
  result jsonb,
  error_message text,
  sqlstate varchar(6),
  is_done boolean not null default false
);
create index on raft.executor_service(uuid) where not is_done;

do $code$
begin
call raft.worker();
end;
$code$

select * from raft.executor_service

select 'lala'<>all(array['lala','dodo','ququ'])

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
        if cn<>all(coalesce(dblink.dblink_get_connections(),array[]::text[])) then
            raise notice 'establish connection to %', cn;
            perform dblink.dblink_connect(cn, self_conn_str);
        end if;
        perform dblink.dblink_send_query(cn, 'call raft.worker()');
        cns=cns||cn;
    end loop;
    while true loop
        perform pg_sleep(5);
        commit;
    end loop;    
end;
$code$
language plpgsql;

select * from pg_stat_activity

create or replace procedure raft.worker() as
$code$
declare
 r raft.executor_service;
 ss text;
 ok boolean;
begin
    while true loop
        select * into r from raft.executor_service where not is_done order by uuid for update limit 1;
        if not found then
            commit;
            perform pg_sleep(1);
            continue;
        end if;
        begin
            execute 'select '||r.fn||'($1)' using r.params into r.result;
            ok=true;
        exception
            when others then ok=false;
        end;
--
        if ok then
            if r.result is not null then
                update raft.executor_service es set result=r.result, error_message=null, sqlstate=null, is_done=true where es.uuid=r.uuid;
            else
                delete from raft.executor_service where uuid=r.uuid;
            end if;
            commit;
        else
            rollback;
            ss=sqlstate;
            update raft.executor_service es set result=null, error_message=sqlerrm, sqlstate=ss, is_done=true where es.uuid=r.uuid;
        end if;
        exit;
    end loop;    
end;
$code$
language plpgsql
--set enable_seqscan to false
;



create or replace function raft.submit(fn text, params jsonb) returns uuid as
$code$
    insert into raft.executor_service(uuid, fn, params) values(gsp.gen_v7_uuid(), fn, params) returning uuid;
$code$
language sql;

create or replace function raft.es_test(params jsonb) returns jsonb as
$code$
begin
    insert into raft.ll values(params['message']);
    return null;
    return jsonb_build_object('result', true);
end
$code$
language plpgsql;

create table raft.ll(msg text);

select * from raft.ll
delete from raft.ll

insert into raft.executor_service(uuid,fn, params)values(gsp.gen_v7_uuid(), 'raft.es_test', jsonb_build_object('message','message')); 

select raft.submit('raft.es_test', jsonb_build_object('message','from submit3'))
