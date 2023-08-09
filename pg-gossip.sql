call exec_at_all_hosts($sql2script$
--------------------------------------------------
--create schema if not exists dblink;
--create extension if not exists dblink with schema dblink;
--create schema gsp;
--create table gsp.self(id int primary key default 1 check(id=1), 
--    name varchar(64) not null check (name~'^[a-zA-Z][A-Za-z0-9]*$'),
--    group_name text not null default 'default',
--    conn_str text
--);
--
--CREATE OR REPLACE FUNCTION gsp.gen_v7_uuid()
-- RETURNS uuid
-- LANGUAGE plpgsql
-- PARALLEL SAFE
--AS $function$
--declare
-- tp text = lpad(to_hex(floor(extract(epoch from clock_timestamp())*1000)::int8),12,'0')||'7';
-- entropy text = md5(gen_random_uuid()::text);
-- begin
--    return (tp || substring(entropy from 1 for 3) || to_hex(8+(random()*3)::int) || substring(entropy from 4 for 15))::uuid;
-- end
--$function$
--;
--CREATE OR REPLACE FUNCTION gsp.gen_v8_uuid(ts timestamp without time zone, v text)
-- RETURNS uuid
-- LANGUAGE plpgsql
-- PARALLEL SAFE
--AS $function$
--declare
-- hash text = encode(sha256(v::bytea),'hex');
-- tp text = lpad(to_hex((extract(epoch from ts)::int8*1000)),12,'0');
-- begin
--    return (tp ||'8'|| substring(hash from 2 for 3)||'8'||substring(hash from 6 for 15))::uuid;
-- end
--$function$
--;
--
--CREATE OR REPLACE FUNCTION gsp.gen_v7_uuid(ts timestamp without time zone)
-- RETURNS uuid
-- LANGUAGE plpgsql
-- PARALLEL SAFE
--AS $function$
--declare
-- tp text = lpad(to_hex(floor(extract(epoch from ts)*1000)::int8),12,'0')||'7';
-- begin
--    return (tp || '0008000000000000000')::uuid;
-- end
--$function$
--;
--
--
--create or replace function gsp.check_group(group_name text) returns boolean as
--$code$
--begin
--    if not exists(select * from gsp.self where self.group_name=check_group.group_name) then
--        raise sqlstate 'LB001' using message=format('Invalid group:%s', group_name);
--    end if;
--    return true;
--end;
--$code$
--language plpgsql;
--
--create or replace procedure gsp.clear_gsp() as
--$code$
--begin
--    delete from gsp.gsp where not exists(select * from gsp.peer_gsp pg where gsp.uuid=pg.gsp_uuid) and gsp.uuid<gsp.gen_v7_uuid(now()::timestamp-make_interval(hours:=2))
--        and gsp.uuid<>(select uuid from gsp.gsp order by uuid desc limit 1);
--end;
--$code$
--language plpgsql;
--
--
--
--
--create table gsp.gsp(
-- uuid uuid primary key,
-- topic varchar(64) not null,
-- payload json
--);
--
--create table gsp.peer(
-- name varchar(64) primary key check (name ~ '^[a-zA-Z][a-zA-Z0-9]*$'),
-- conn_str text not null,
-- connected_at timestamptz
--);
--
--create table gsp.peer_gsp(
--    peer_name varchar(64) references gsp.peer(name),
--    gsp_uuid uuid not null references gsp.gsp(uuid)
--);
--
--create table gsp.mapping(
-- topic varchar(64) primary key,
-- handler text not null
--);
--
--create or replace function gsp.handle_peer(iuuid uuid, payload json) returns void as
--$code$
--begin
--    if exists(select * from gsp.self where name=payload->>name) then
--        return;
--    end if;
--    insert into gsp.peer(name, conn_str)values(payload->>'name', payload->>'conn_str') on conflict(name) do update set conn_str=excluded.conn_str;
--end;
--$code$
--language plpgsql;
--         
--create or replace function gsp.i_have(gsps uuid[]) returns table(uuid uuid) as 
--$code$
--begin
--    return query select 
--            gsps.uuid 
--    from unnest(gsps) gsps 
--    where not exists(select * from gsp.gsp where gsp.uuid=gsps.uuid)
--    and coalesce(gsps.uuid>(select gsp.uuid from gsp.gsp order by gsp.uuid limit 1), true); 
--end;
--$code$
--language plpgsql;
--
--create or replace function gsp.send_gsp(gsps gsp.gsp[]) returns text as
--$code$
--declare
--    r record;
--begin
--    insert into gsp.gsp select gsps.* from unnest(gsps) as gsps on conflict do nothing;
--    insert into gsp.peer_gsp select p.name, gsps.uuid from gsp.peer p, unnest(gsps) as gsps on conflict do nothing;
--    for r in select gsp.*, m.* from unnest(gsps) as gsp, gsp.mapping m where m.topic like gsp.topic loop
--        execute format('select %s($1, $2)', r.handler) using r.uuid, r.payload;
--    end loop;
--    return 'OK';
--end;
--$code$
--language plpgsql;
--
--create or replace function gsp.gossip(topic text, payload json) returns uuid as
--$code$
--declare
-- iuuid uuid = gsp.gen_v7_uuid();
--begin
--    perform gsp.send_gsp(array[(iuuid, topic, payload)::gsp.gsp]);
--    return iuuid;
--end;
--$code$
--language plpgsql;
--
--create or replace function gsp.spread_gossips(peer_name text) returns int as
--$code$
--declare
--    gsps uuid[]=array(select pg.gsp_uuid 
--                        from gsp.peer_gsp pg 
--                       where pg.peer_name=spread_gossips.peer_name
--                         and pg.gsp_uuid>gsp.gen_v7_uuid(now()::timestamp-make_interval(days:=1))
--                    order by gsp_uuid for update skip locked limit 1000);
--    sendme uuid[];
--    conn_str text = (select p.conn_str from gsp.peer p where p.name=spread_gossips.peer_name);
--    self gsp.self;
--begin
--    if conn_str is null then
--        raise notice 'spread_gossips(%):Unknown peer:%', (select name from gsp.self), peer_name;
--        return 0; 
--    end if;
--    if cardinality(gsps)=0 then
--        return 0;
--    end if;
--    select * into self from gsp.self;
--    sendme=array(select uuid from dblink.dblink(conn_str, 
--        format($q$select uuid from gsp.i_have(%L) where gsp.check_group(%L)$q$, gsps, self.group_name)) as rs(uuid uuid));
--    update gsp.peer set connected_at=now() where name=peer_name;
--    if sendme is null or cardinality(sendme)=0 then
--        delete from gsp.peer_gsp pg where pg.peer_name=spread_gossips.peer_name and pg.gsp_uuid=any(gsps);
--        return 0;
--    end if;
--    perform from dblink.dblink(conn_str,format('select gsp.send_gsp(%L::gsp.gsp[])', (select array_agg(gsp) from gsp.gsp where uuid=any(sendme)))) as rs(v text);
--    delete from gsp.peer_gsp pg where pg.peer_name=spread_gossips.peer_name and pg.gsp_uuid=any(gsps);
--    return cardinality(gsps);
--end;
--$code$
--language plpgsql;
--


--create or replace procedure gsp.constantly_spread_gossips(peer text) as 
--$code$
--declare
--   cnt int;
--   ok boolean;
--begin
--    while true loop
--        begin
--            raise notice 'Sending gossips';
--            cnt=gsp.spread_gossips(peer);
--            ok = true;
--        exception
--            when sqlstate '08000' then
--                ok = false;
--                raise notice 'Connection problems:%', sqlerrm;
--        end;
--        if ok then
--            commit;
--            if cnt=0 then
--                perform pg_sleep(3);
--            end if;
--        else
--            rollback;
--            perform pg_sleep(30);
--        end if;
--    end loop;
--end;
--$code$
--language plpgsql;


--insert into gsp.mapping values('peer-height','ldg.handle_peer_height') on conflict do nothing;
--insert into gsp.mapping values('txpool','ldg.handle_txpool') on conflict do nothing;
--insert into gsp.mapping values('peer','ldg.handle_peer') on conflict do nothing;
--
----**********************************
----************ LEDGER **************
----**********************************
--create schema ldg;
--
--create table ldg.txpool(
-- uuid uuid primary key,
-- payload json not null
--);
--
--
--
--create or replace function ldg.broadcast_tx(tx json) returns uuid as
--$code$
--    begin
--        return gsp.gossip('txpool',tx);
--    end;
--$code$
--language plpgsql;
--
--
--create or replace function ldg.handle_txpool(iuuid uuid, payload json) returns void as
--$code$
--begin
--    insert into ldg.txpool(uuid, payload) values(iuuid, payload) on conflict do nothing;
--end;
--$code$
--language plpgsql;
--
--create table ldg.ldg(
--    uuid uuid primary key,
--    height bigint not null,
--    payload json[] not null
--);
--
--create table if not exists ldg.peer_height(
--    peer_name varchar(64) not null primary key check(peer_name ~'^[a-zA-Z][a-zA-Z0-9]+$'),
--    height bigint not null
--);
--
--
--
--create or replace function ldg.handle_peer_height(uuid uuid, payload json) returns void as
--$code$
--begin
--    insert into ldg.peer_height values(payload->>'name', (payload->>'height')::bigint) on conflict(peer_name) do update set height=excluded.height;
--end;
--$code$
--language plpgsql;
--
--create or replace procedure ldg.constantly_gossip_my_height() as 
--$code$
--begin
--    while true loop
--        raise notice 'Sending own height';
--        perform gsp.gossip('peer-height',json_build_object('name',self.name,'height',coalesce(ldg.max_height,0), 'sent-at',clock_timestamp()))
--            from gsp.self, (select max(height) as max_height from ldg.ldg) ldg;
--        commit;
--        perform pg_sleep(45);
--    end loop;
--    
--end;
--$code$
--language plpgsql;
--
--create or replace function ldg.is_ready() returns boolean as
--$code$
--begin
--    return exists(select * from gsp.peer p where connected_at>now()-make_interval(secs:=90))
--       and coalesce((select max(height) from ldg.ldg),0)>=coalesce((select max(height) from ldg.peer_height),-1);
--end;
--$code$
--language plpgsql;
--
--create table ldg.proposed_block(
--  uuid uuid not null primary key,
--  peer_name varchar(64) not null check(peer_name ~ '^[a-zA-Z][a-zA-Z0-9]+$'),
--  height bigint not null,
--  block json[],
--  unique(peer_name, height)
--);


create or replace procedure ldg.make_proposed_block() as
$code$
<<code>>
declare
  p_b ldg.proposed_block;
  self gsp.self;
  height bigint = coalesce((select max(height) from ldg.ldg),0);
begin
    select * into self from gsp.self;
    if not found then return; end if;
    if not ldg.is_ready() or exists(select * from ldg.proposed_block pb where pb.peer_name=self.name and pb.height=code.height)
    then
        return;
    end if;
    if exists(select * from ldg.ldg where ldg.height>=code.height) then
        return;
    end if;
    p_b.uuid=gsp.gen_v7_uuid();
    p_b.peer_name = (select name from gsp.self);
    p_b.height = height;
    p_b.block = array(select payload from ldg.txpool order by uuid limit 1000);
    raise notice 'Propose block from % height %', self.name, height;
    perform gsp.gossip('proposed-blocks', row_to_json(p_b));
end;
$code$
language plpgsql;

--create or replace function ldg.get_proposed_block_at_height(height bigint) returns uuid as
--$code$
--begin
--    return (select pb.uuid from ldg.proposed_block pb, gsp.self where pb.height=get_proposed_block_at_height.height and pb.peer_name<>self.name limit 1);
--end;
--$code$
--language plpgsql;

--insert into gsp.mapping values('proposed-blocks', 'ldg.handle_proposed_block');
--
--create or replace function ldg.handle_proposed_block(uuid uuid, payload json) returns void as
--$code$
--declare
--  p_b ldg.proposed_block;
--begin
--    insert into ldg.proposed_block(uuid, peer_name, height, block) 
--        select (payload->>'uuid')::uuid,
--               payload->>'peer_name',
--               (payload->>'height')::bigint,
--               array(select v from json_array_elements(payload->'block') as v)
--    on conflict do nothing;
--end;
--$code$
--language plpgsql;

------------------------------------------------------------------
         $sql2script$::text);
