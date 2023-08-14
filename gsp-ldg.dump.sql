--
-- PostgreSQL database dump
--

-- Dumped from database version 15.3 (Debian 15.3-1.pgdg110+1)
-- Dumped by pg_dump version 15.2 (Debian 15.2-1.pgdg110+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: gsp; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA gsp;


ALTER SCHEMA gsp OWNER TO postgres;

--
-- Name: ldg; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA ldg;


ALTER SCHEMA ldg OWNER TO postgres;

--
-- Name: check_group(text); Type: FUNCTION; Schema: gsp; Owner: postgres
--

CREATE FUNCTION gsp.check_group(group_name text) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
begin
    if not exists(select * from gsp.self where self.group_name=check_group.group_name) then
        raise sqlstate 'LB001' using message=format('Invalid group:%s', group_name);
    end if;
    return true;
end;
$$;


ALTER FUNCTION gsp.check_group(group_name text) OWNER TO postgres;

--
-- Name: clear_gsp(); Type: PROCEDURE; Schema: gsp; Owner: postgres
--

CREATE PROCEDURE gsp.clear_gsp()
    LANGUAGE plpgsql
    AS $$
begin
    delete from gsp.gsp where not exists(select * from gsp.peer_gsp pg where gsp.uuid=pg.gsp_uuid) and gsp.uuid<gsp.gen_v7_uuid(now()::timestamp-make_interval(hours:=2))
        and gsp.uuid<>(select uuid from gsp.gsp order by uuid desc limit 1);
end;
$$;


ALTER PROCEDURE gsp.clear_gsp() OWNER TO postgres;

--
-- Name: constantly_spread_gossips(text); Type: PROCEDURE; Schema: gsp; Owner: postgres
--

CREATE PROCEDURE gsp.constantly_spread_gossips(IN peer text)
    LANGUAGE plpgsql
    AS $$
declare
   cnt int;
   ok boolean;
begin
    while true loop
        begin
            cnt=gsp.spread_gossips(peer);
            ok = true;
        exception
            when deadlock_detected then
                ok = false;
                raise notice 'Deadlock detected';
        end;
        if ok then
            commit;
            if cnt=0 then
                perform pg_sleep(3);
            end if;
        else
            rollback;
            perform pg_sleep(30);
        end if;
    end loop;
end;
$$;


ALTER PROCEDURE gsp.constantly_spread_gossips(IN peer text) OWNER TO postgres;

--
-- Name: gen_v7_uuid(); Type: FUNCTION; Schema: gsp; Owner: postgres
--

CREATE FUNCTION gsp.gen_v7_uuid() RETURNS uuid
    LANGUAGE plpgsql PARALLEL SAFE
    AS $$
declare
 tp text = lpad(to_hex(floor(extract(epoch from clock_timestamp())*1000)::int8),12,'0')||'7';
 entropy text = md5(gen_random_uuid()::text);
 begin
    return (tp || substring(entropy from 1 for 3) || to_hex(8+(random()*3)::int) || substring(entropy from 4 for 15))::uuid;
 end
$$;


ALTER FUNCTION gsp.gen_v7_uuid() OWNER TO postgres;

--
-- Name: gen_v7_uuid(timestamp without time zone); Type: FUNCTION; Schema: gsp; Owner: postgres
--

CREATE FUNCTION gsp.gen_v7_uuid(ts timestamp without time zone) RETURNS uuid
    LANGUAGE plpgsql PARALLEL SAFE
    AS $$
declare
 tp text = lpad(to_hex(floor(extract(epoch from ts)*1000)::int8),12,'0')||'7';
 begin
    return (tp || '0008000000000000000')::uuid;
 end
$$;


ALTER FUNCTION gsp.gen_v7_uuid(ts timestamp without time zone) OWNER TO postgres;

--
-- Name: gen_v8_uuid(timestamp without time zone, text); Type: FUNCTION; Schema: gsp; Owner: postgres
--

CREATE FUNCTION gsp.gen_v8_uuid(ts timestamp without time zone, v text) RETURNS uuid
    LANGUAGE plpgsql PARALLEL SAFE
    AS $$
declare
 hash text = encode(sha256(v::bytea),'hex');
 tp text = lpad(to_hex((extract(epoch from ts)::int8*1000)),12,'0');
 begin
    return (tp ||'8'|| substring(hash from 2 for 3)||'8'||substring(hash from 6 for 15))::uuid;
 end
$$;


ALTER FUNCTION gsp.gen_v8_uuid(ts timestamp without time zone, v text) OWNER TO postgres;

--
-- Name: gossip(text, json); Type: FUNCTION; Schema: gsp; Owner: postgres
--

CREATE FUNCTION gsp.gossip(topic text, payload json) RETURNS uuid
    LANGUAGE plpgsql
    AS $$
declare
 iuuid uuid = gsp.gen_v7_uuid();
begin
    perform gsp.send_gsp(array[(iuuid, topic, payload)::gsp.gsp]);
    return iuuid;
end;
$$;


ALTER FUNCTION gsp.gossip(topic text, payload json) OWNER TO postgres;

--
-- Name: handle_peer(uuid, json); Type: FUNCTION; Schema: gsp; Owner: postgres
--

CREATE FUNCTION gsp.handle_peer(iuuid uuid, payload json) RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    if exists(select * from gsp.self where name=payload->>name) then
        return;
    end if;
    insert into gsp.peer(name, conn_str)values(payload->>'name', payload->>'conn_str') on conflict(name) do update set conn_str=excluded.conn_str;
end;
$$;


ALTER FUNCTION gsp.handle_peer(iuuid uuid, payload json) OWNER TO postgres;

--
-- Name: i_have(uuid[]); Type: FUNCTION; Schema: gsp; Owner: postgres
--

CREATE FUNCTION gsp.i_have(gsps uuid[]) RETURNS TABLE(uuid uuid)
    LANGUAGE plpgsql
    AS $$
begin
    return query select 
            gsps.uuid 
    from unnest(gsps) gsps 
    where not exists(select * from gsp.gsp where gsp.uuid=gsps.uuid)
    and coalesce(gsps.uuid>(select gsp.uuid from gsp.gsp order by gsp.uuid limit 1), true); 
end;
$$;


ALTER FUNCTION gsp.i_have(gsps uuid[]) OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: gsp; Type: TABLE; Schema: gsp; Owner: postgres
--

CREATE TABLE gsp.gsp (
    uuid uuid NOT NULL,
    topic character varying(64) NOT NULL,
    payload json
);


ALTER TABLE gsp.gsp OWNER TO postgres;

--
-- Name: send_gsp(gsp.gsp[]); Type: FUNCTION; Schema: gsp; Owner: postgres
--

CREATE FUNCTION gsp.send_gsp(gsps gsp.gsp[]) RETURNS text
    LANGUAGE plpgsql
    AS $_$
declare
    r record;
begin
    insert into gsp.gsp select gsps.* from unnest(gsps) as gsps on conflict do nothing;
    insert into gsp.peer_gsp select p.name, gsps.uuid from gsp.peer p, unnest(gsps) as gsps on conflict do nothing;      
    for r in select gsp.*, m.* from unnest(gsps) as gsp, gsp.mapping m where m.topic like gsp.topic loop
        execute format('select %s($1, $2)', r.handler) using r.uuid, r.payload;
    end loop;
    return 'OK';
end;
$_$;


ALTER FUNCTION gsp.send_gsp(gsps gsp.gsp[]) OWNER TO postgres;

--
-- Name: spread_gossips(text); Type: FUNCTION; Schema: gsp; Owner: postgres
--

CREATE FUNCTION gsp.spread_gossips(peer_name text) RETURNS integer
    LANGUAGE plpgsql
    AS $_$
declare
    gsps uuid[]=array(select pg.gsp_uuid 
                        from gsp.peer_gsp pg 
                       where pg.peer_name=spread_gossips.peer_name
                         and pg.gsp_uuid>gsp.gen_v7_uuid(now()::timestamp-make_interval(days:=1))
                    order by gsp_uuid for update skip locked limit 1000);
    sendme uuid[];
    conn_str text = (select p.conn_str from gsp.peer p where p.name=spread_gossips.peer_name);
    self gsp.self;
begin
    if conn_str is null then
        raise notice 'spread_gossips(%):Unknown peer:%', (select name from gsp.self), peer_name;
        return 0; 
    end if;
    if cardinality(gsps)=0 then
        return 0;
    end if;
    select * into self from gsp.self;
    sendme=array(select uuid from dblink.dblink(conn_str, 
        format($q$select uuid from gsp.i_have(%L) where gsp.check_group(%L)$q$, gsps, self.group_name)) as rs(uuid uuid));
    update gsp.peer set connected_at=now() where name=peer_name;
    if sendme is null or cardinality(sendme)=0 then
        delete from gsp.peer_gsp pg where pg.peer_name=spread_gossips.peer_name and pg.gsp_uuid=any(gsps);
        return 0;
    end if;
    perform from dblink.dblink(conn_str,format('select gsp.send_gsp(%L::gsp.gsp[])', (select array_agg(gsp) from gsp.gsp where uuid=any(sendme)))) as rs(v text);
    delete from gsp.peer_gsp pg where pg.peer_name=spread_gossips.peer_name and pg.gsp_uuid=any(gsps);
    return cardinality(gsps);
end;
$_$;


ALTER FUNCTION gsp.spread_gossips(peer_name text) OWNER TO postgres;

--
-- Name: apply_proposed_block(uuid); Type: PROCEDURE; Schema: ldg; Owner: postgres
--

CREATE PROCEDURE ldg.apply_proposed_block(IN block_uuid uuid)
    LANGUAGE plpgsql
    AS $$
declare
    r record;
    nh bigint;
begin
    insert into ldg.ldg(uuid, height, payload) select uuid, height, block from ldg.proposed_block pb where pb.uuid=block_uuid and (pb.height=0 or exists(select * from ldg.ldg l1 where l1.height=pb.height-1))
        on conflict do nothing;
    if not found then
        for r in select conn_str from gsp.peer order by connected_at desc loop
            begin
                insert into ldg.ldg(uuid, height, payload) 
                    select (ldg).* 
                      from dblink.dblink(r.conn_str,
                          format('select ldg from ldg.ldg where uuid=%L', block_uuid)) as ldg(ldg ldg.ldg)
                      where exists(select * from ldg.ldg l1 where l1.height=(ldg.ldg).height-1)
                  on conflict do nothing
                  returning height into nh;
                if not found then
                    continue;
                end if;
                delete from ldg.txpool where txpool.uuid in(select (p.block->>'uuid')::uuid from ldg.ldg pb, unnest(pb.payload) as p(block) where pb.height=nh);
                delete from ldg.proposed_block as pba where pba.height<nh;
            exception
                when sqlstate '08000' then continue;
            end;
        end loop;
    else
        delete from ldg.txpool where txpool.uuid in(select (p.block->>'uuid')::uuid from ldg.proposed_block pb, unnest(pb.block) as p(block) where pb.uuid=block_uuid);
        delete from ldg.proposed_block as pba where pba.height<(select pb.height from ldg.proposed_block pb where pb.uuid=block_uuid);
    end if;
end;
$$;


ALTER PROCEDURE ldg.apply_proposed_block(IN block_uuid uuid) OWNER TO postgres;

--
-- Name: broadcast_tx(json); Type: FUNCTION; Schema: ldg; Owner: postgres
--

CREATE FUNCTION ldg.broadcast_tx(tx json) RETURNS uuid
    LANGUAGE plpgsql
    AS $$
    begin
        return gsp.gossip('txpool',tx);
    end;
$$;


ALTER FUNCTION ldg.broadcast_tx(tx json) OWNER TO postgres;

--
-- Name: clear_txpool(); Type: PROCEDURE; Schema: ldg; Owner: postgres
--

CREATE PROCEDURE ldg.clear_txpool()
    LANGUAGE plpgsql
    AS $$
begin
    delete from ldg.txpool 
        where exists(select * from ldg.ldg where ldg.expand_ldg_payload_uuids(ldg)@>array[txpool.uuid]);
    commit;
end;
$$;


ALTER PROCEDURE ldg.clear_txpool() OWNER TO postgres;

--
-- Name: constantly_gossip_my_height(); Type: PROCEDURE; Schema: ldg; Owner: postgres
--

CREATE PROCEDURE ldg.constantly_gossip_my_height()
    LANGUAGE plpgsql
    AS $$
begin
    while true loop
        raise notice 'Sending own height';
        perform gsp.gossip('peer-height',json_build_object('name',self.name,'height',coalesce(ldg.max_height,0), 'sent-at',clock_timestamp()))
            from gsp.self, (select max(height) as max_height from ldg.ldg) ldg;
        commit;
        perform pg_sleep(45);
    end loop;
    
end;
$$;


ALTER PROCEDURE ldg.constantly_gossip_my_height() OWNER TO postgres;

--
-- Name: ldg; Type: TABLE; Schema: ldg; Owner: postgres
--

CREATE TABLE ldg.ldg (
    uuid uuid NOT NULL,
    height bigint NOT NULL,
    payload json[] NOT NULL
);


ALTER TABLE ldg.ldg OWNER TO postgres;

--
-- Name: expand_ldg_payload_uuids(ldg.ldg); Type: FUNCTION; Schema: ldg; Owner: postgres
--

CREATE FUNCTION ldg.expand_ldg_payload_uuids(ldg ldg.ldg) RETURNS uuid[]
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
  select array_agg((uo->>'uuid')::uuid) from (select ldg.*) ldg, unnest(ldg.payload) as uo
$$;


ALTER FUNCTION ldg.expand_ldg_payload_uuids(ldg ldg.ldg) OWNER TO postgres;

--
-- Name: get_proposed_block_at_height(bigint); Type: FUNCTION; Schema: ldg; Owner: postgres
--

CREATE FUNCTION ldg.get_proposed_block_at_height(height bigint) RETURNS uuid
    LANGUAGE plpgsql
    AS $$
begin
    return (select pb.uuid from ldg.proposed_block pb, gsp.self where pb.height=get_proposed_block_at_height.height and pb.peer_name<>self.name limit 1);
end;
$$;


ALTER FUNCTION ldg.get_proposed_block_at_height(height bigint) OWNER TO postgres;

--
-- Name: gossip_my_height(); Type: PROCEDURE; Schema: ldg; Owner: postgres
--

CREATE PROCEDURE ldg.gossip_my_height()
    LANGUAGE plpgsql
    AS $$
begin
    perform gsp.gossip('peer-height',json_build_object('name',self.name,'height',coalesce(ldg.max_height,0), 'sent-at',clock_timestamp()))
        from gsp.self, (select max(height) as max_height from ldg.ldg) ldg;
    commit;
end;
$$;


ALTER PROCEDURE ldg.gossip_my_height() OWNER TO postgres;

--
-- Name: handle_peer_height(uuid, json); Type: FUNCTION; Schema: ldg; Owner: postgres
--

CREATE FUNCTION ldg.handle_peer_height(uuid uuid, payload json) RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    insert into ldg.peer_height values(payload->>'name', (payload->>'height')::bigint) on conflict(peer_name) do update set height=excluded.height;
end;
$$;


ALTER FUNCTION ldg.handle_peer_height(uuid uuid, payload json) OWNER TO postgres;

--
-- Name: handle_proposed_block(uuid, json); Type: FUNCTION; Schema: ldg; Owner: postgres
--

CREATE FUNCTION ldg.handle_proposed_block(uuid uuid, payload json) RETURNS void
    LANGUAGE plpgsql
    AS $$
declare
  p_b ldg.proposed_block;
begin
    insert into ldg.proposed_block(uuid, peer_name, height, block) 
        select (payload->>'uuid')::uuid,
               payload->>'peer_name',
               (payload->>'height')::bigint,
               array(select v from json_array_elements(payload->'block') as v)
    on conflict do nothing;
end;
$$;


ALTER FUNCTION ldg.handle_proposed_block(uuid uuid, payload json) OWNER TO postgres;

--
-- Name: handle_txpool(uuid, json); Type: FUNCTION; Schema: ldg; Owner: postgres
--

CREATE FUNCTION ldg.handle_txpool(iuuid uuid, payload json) RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
    insert into ldg.txpool(uuid, payload) values(iuuid, payload) on conflict do nothing;
end;
$$;


ALTER FUNCTION ldg.handle_txpool(iuuid uuid, payload json) OWNER TO postgres;

--
-- Name: is_ready(); Type: FUNCTION; Schema: ldg; Owner: postgres
--

CREATE FUNCTION ldg.is_ready() RETURNS boolean
    LANGUAGE plpgsql
    AS $$
begin
    return exists(select * from gsp.peer p where connected_at>now()-make_interval(secs:=90))
       and coalesce((select max(height) from ldg.ldg),0)>=coalesce((select max(height) from ldg.peer_height),-1);
end;
$$;


ALTER FUNCTION ldg.is_ready() OWNER TO postgres;

--
-- Name: make_proposed_block(); Type: PROCEDURE; Schema: ldg; Owner: postgres
--

CREATE PROCEDURE ldg.make_proposed_block()
    LANGUAGE plpgsql
    AS $$
<<code>>
declare
  p_b ldg.proposed_block;
  self gsp.self;
  height bigint = coalesce((select max(height)+1 from ldg.ldg),0);
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
    p_b.block = array(select json_build_object('uuid', txpool.uuid, 'payload', txpool.payload) from ldg.txpool 
                where not exists(select * from ldg.ldg where ldg.expand_ldg_payload_uuids(ldg)@>array[txpool.uuid]) order by uuid limit 64*10);
    if cardinality(p_b.block)>0 then
        perform gsp.gossip('proposed-blocks', row_to_json(p_b));
    end if;
end;
$$;


ALTER PROCEDURE ldg.make_proposed_block() OWNER TO postgres;

--
-- Name: make_proposed_block(bigint); Type: PROCEDURE; Schema: ldg; Owner: postgres
--

CREATE PROCEDURE ldg.make_proposed_block(IN height bigint)
    LANGUAGE plpgsql
    AS $$
<<code>>
declare
  p_b ldg.proposed_block;
  self gsp.self;
begin
    select * into self from gsp.self;
    if not found then return; end if;
    if not ldg.is_ready() or exists(select * from ldg.proposed_block pb where pb.peer_name=self.name and pb.height=make_proposed_block.height)
    then
        return;
    end if;
    p_b.uuid=gsp.gen_v7_uuid();
    p_b.peer_name = (select name from gsp.self);
    p_b.height = height;
    p_b.block = array(select payload from ldg.txpool order by uuid limit 1000);
    raise notice 'Propose block from % height %', self.name, height;
    perform gsp.gossip('proposed-blocks', row_to_json(p_b));
end;
$$;


ALTER PROCEDURE ldg.make_proposed_block(IN height bigint) OWNER TO postgres;

--
-- Name: mapping; Type: TABLE; Schema: gsp; Owner: postgres
--

CREATE TABLE gsp.mapping (
    topic character varying(64) NOT NULL,
    handler text NOT NULL
);


ALTER TABLE gsp.mapping OWNER TO postgres;

--
-- Name: peer; Type: TABLE; Schema: gsp; Owner: postgres
--

CREATE TABLE gsp.peer (
    name character varying(64) NOT NULL,
    conn_str text NOT NULL,
    connected_at timestamp with time zone,
    CONSTRAINT peer_name_check CHECK (((name)::text ~ '^[a-zA-Z][a-zA-Z0-9]*$'::text))
);


ALTER TABLE gsp.peer OWNER TO postgres;

--
-- Name: peer_gsp; Type: TABLE; Schema: gsp; Owner: postgres
--

CREATE TABLE gsp.peer_gsp (
    peer_name character varying(64),
    gsp_uuid uuid NOT NULL
);


ALTER TABLE gsp.peer_gsp OWNER TO postgres;

--
-- Name: self; Type: TABLE; Schema: gsp; Owner: postgres
--

CREATE TABLE gsp.self (
    id integer DEFAULT 1 NOT NULL,
    name character varying(64) NOT NULL,
    group_name text DEFAULT 'default'::text NOT NULL,
    conn_str text,
    CONSTRAINT self_id_check CHECK ((id = 1)),
    CONSTRAINT self_name_check CHECK (((name)::text ~ '^[a-zA-Z][A-Za-z0-9]*$'::text))
);


ALTER TABLE gsp.self OWNER TO postgres;

--
-- Name: etcd; Type: TABLE; Schema: ldg; Owner: postgres
--

CREATE TABLE ldg.etcd (
    id integer DEFAULT 1 NOT NULL,
    height bigint,
    connected_at timestamp with time zone,
    cluster_id text,
    CONSTRAINT etcd_id_check CHECK ((id = 1))
);


ALTER TABLE ldg.etcd OWNER TO postgres;

--
-- Name: peer_height; Type: TABLE; Schema: ldg; Owner: postgres
--

CREATE TABLE ldg.peer_height (
    peer_name character varying(64) NOT NULL,
    height bigint NOT NULL,
    CONSTRAINT peer_height_peer_name_check CHECK (((peer_name)::text ~ '^[a-zA-Z][a-zA-Z0-9]+$'::text))
);


ALTER TABLE ldg.peer_height OWNER TO postgres;

--
-- Name: proposed_block; Type: TABLE; Schema: ldg; Owner: postgres
--

CREATE TABLE ldg.proposed_block (
    uuid uuid NOT NULL,
    peer_name character varying(64) NOT NULL,
    height bigint NOT NULL,
    block json[],
    CONSTRAINT proposed_block_peer_name_check CHECK (((peer_name)::text ~ '^[a-zA-Z][a-zA-Z0-9]+$'::text))
);


ALTER TABLE ldg.proposed_block OWNER TO postgres;

--
-- Name: txpool; Type: TABLE; Schema: ldg; Owner: postgres
--

CREATE TABLE ldg.txpool (
    uuid uuid NOT NULL,
    payload json NOT NULL
);


ALTER TABLE ldg.txpool OWNER TO postgres;

--
-- Name: gsp gsp_pkey; Type: CONSTRAINT; Schema: gsp; Owner: postgres
--

ALTER TABLE ONLY gsp.gsp
    ADD CONSTRAINT gsp_pkey PRIMARY KEY (uuid);


--
-- Name: mapping mapping_pkey; Type: CONSTRAINT; Schema: gsp; Owner: postgres
--

ALTER TABLE ONLY gsp.mapping
    ADD CONSTRAINT mapping_pkey PRIMARY KEY (topic);


--
-- Name: peer peer_pkey; Type: CONSTRAINT; Schema: gsp; Owner: postgres
--

ALTER TABLE ONLY gsp.peer
    ADD CONSTRAINT peer_pkey PRIMARY KEY (name);


--
-- Name: self self_pkey; Type: CONSTRAINT; Schema: gsp; Owner: postgres
--

ALTER TABLE ONLY gsp.self
    ADD CONSTRAINT self_pkey PRIMARY KEY (id);


--
-- Name: etcd etcd_pkey; Type: CONSTRAINT; Schema: ldg; Owner: postgres
--

ALTER TABLE ONLY ldg.etcd
    ADD CONSTRAINT etcd_pkey PRIMARY KEY (id);


--
-- Name: ldg ldg_pkey; Type: CONSTRAINT; Schema: ldg; Owner: postgres
--

ALTER TABLE ONLY ldg.ldg
    ADD CONSTRAINT ldg_pkey PRIMARY KEY (uuid);


--
-- Name: peer_height peer_height_pkey; Type: CONSTRAINT; Schema: ldg; Owner: postgres
--

ALTER TABLE ONLY ldg.peer_height
    ADD CONSTRAINT peer_height_pkey PRIMARY KEY (peer_name);


--
-- Name: proposed_block proposed_block_peer_name_height_key; Type: CONSTRAINT; Schema: ldg; Owner: postgres
--

ALTER TABLE ONLY ldg.proposed_block
    ADD CONSTRAINT proposed_block_peer_name_height_key UNIQUE (peer_name, height);


--
-- Name: proposed_block proposed_block_pkey; Type: CONSTRAINT; Schema: ldg; Owner: postgres
--

ALTER TABLE ONLY ldg.proposed_block
    ADD CONSTRAINT proposed_block_pkey PRIMARY KEY (uuid);


--
-- Name: txpool txpool_pkey; Type: CONSTRAINT; Schema: ldg; Owner: postgres
--

ALTER TABLE ONLY ldg.txpool
    ADD CONSTRAINT txpool_pkey PRIMARY KEY (uuid);


--
-- Name: ldg_expand_ldg_payload_uuids_idx; Type: INDEX; Schema: ldg; Owner: postgres
--

CREATE INDEX ldg_expand_ldg_payload_uuids_idx ON ldg.ldg USING gin (ldg.expand_ldg_payload_uuids(ldg.*));


--
-- Name: peer_gsp peer_gsp_gsp_uuid_fkey; Type: FK CONSTRAINT; Schema: gsp; Owner: postgres
--

ALTER TABLE ONLY gsp.peer_gsp
    ADD CONSTRAINT peer_gsp_gsp_uuid_fkey FOREIGN KEY (gsp_uuid) REFERENCES gsp.gsp(uuid);


--
-- Name: peer_gsp peer_gsp_peer_name_fkey; Type: FK CONSTRAINT; Schema: gsp; Owner: postgres
--

ALTER TABLE ONLY gsp.peer_gsp
    ADD CONSTRAINT peer_gsp_peer_name_fkey FOREIGN KEY (peer_name) REFERENCES gsp.peer(name);


--
-- PostgreSQL database dump complete
--

