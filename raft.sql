create schema raft;

create domain raft.name varchar(64) check(value~'^[a-zA-Z0-9][-_a-zA-Z0-9]*$');
create type raft.peer_state as enum('LEADER','FOLLOWER','CANDIDATE');

drop table raft.state

create table raft.state(
 id int primary key default 1 check(id=1),
 current_term int check(current_term>=0),
 voted_for raft.name,
 commit_id bigint,
 last_applied_id bigint,
 state raft.peer_state not null,
 vote_timer timestamptz,
 vote_interval interval,
 just_elected boolean default false
);

create or replace function raft.start(force boolean default false) returns void as
$code$
begin
    insert into raft.state(
    id, current_term, voted_for,
    commit_id, last_applied_id,state,
    vote_timer, vote_interval
    ) values (
    1, 0, null,
    -1, -1, 'FOLLOWER',
    now(), make_interval(secs:=10)
    ) on conflict(id) do update
      set current_term=excluded.current_term, voted_for=excluded.voted_for,
        commit_id=excluded.commit_id, last_applied_id=excluded.last_applied_id, state=excluded.state,
        vote_timer=excluded.vote_timer, vote_interval=excluded.vote_interval
    where force;
end;    
$code$
language plpgsql;

create or replace procedure raft.handle_peer(peer_name raft.name) as
$code$
declare
 node_status raft.peer_state;
 peer raft.peer;
 cn_name text='raft.'||peer_name;
 peer raft.peer;
 remote_term int;
 call_result boolean;
 prev_log_id bigint;
 prev_log_term int;
begin
    select * into peer from raft.peer where peer.name=peer_name;
    if not found then
        raise sqlstate='RF003' using message='Unknown peer';
    end if;
    if cn_name<>all(dblink.dblink_get_connections()) then
        perform dblink.dblink_connect(cn_name, peer.conn_str);
    end if;
    while true loop
        select state into node_status from raft.state;
        if not found then
            raise sqlstate 'RF001' using message='This raft node has not been initialized';
        end if;
        if status='LEADER' then
        /*
         * ae_term int, leader_name raft.name, prev_log_id bigint, prev_log_term int, entries raft.log[], leader_commit_id bigint
         */
            if now()-state.vote_timer>make_interval() then
                select id, term into prev_log_id, prev_log_term from raft.log order by term desc, id desc limit 1;
                select * into remote_term, call_result from dblink.dblink(cn_name, 
                    'select raft.append_entries(%s,%L, %s, %s, array[]::raft.log[]'
            end if;
        elsif status='CANDIDATE' then
        elsif status='FOLLOWER' then
        else
            raise sqlstate 'RF002' using message=format('Unknown node state:%s', node_state);
        end if;
        commit;
        perform pg_sleep(1);
    end loop;
end;
$code$
language plpgsql;

select * from raft.state;
select raft.start();


create table raft.log(
  id bigint,
  term int not null check(term>=0),
  command text not null,
  primary key(id, term)
);

create table raft.peer(
 name raft.name not null primary key,
 conn_str text not null,
 next_index bigint check(next_index>=0),
 match_index bigint check(match_index>=0)
);

create or replace function raft.append_entries(ae_term int, leader_name raft.name,
prev_log_id bigint, prev_log_term int, entries raft.log[], leader_commit_id bigint) 
returns table(term int, success boolean) as
$code$
declare
 state raft.state;
 id bigint;
begin
    select raft.state into state from raft.state;
    if not found then raise sqlstate 'RF001' using message='Node is not initialized'; end if;
    if ae_term<state.current_term then return query select raft.current_term, false; return; end if;
    if not exists(select * from raft.log where log.term=prev_log_term and log.id=prev_log_id) then 
        return query select raft.current_term, false; return; 
    end if;
--
    if ae_term>state.term then
        update raft.state set term=ae_term, state='FOLLOWER', voted_for=null, vote_timer=vote_timer+vote_interval;
    end if;
--    
    delete from raft.log where log.id>=(
            select min(log1.id) 
                from raft.log log1, unnest(entries) as e
                where log1.term<>e.term and log.id=e.id
    );
    insert into raft.log select * from unnest(entries) as e;
    update raft.state set commit_id=least(leader_commit_id,(select max(e.id) from unnest(entries) e))
     where leader_commit_id>coalesce(state.commit_id,-1);
    if leader_commit_id>state.last_applied_id then
        for id in state.last_applied_id+1..leader_commit_id loop
            raise notice 'apply id %', id;
        end loop;
        update raft.state set last_applied_id=leader_commit_id;
    end if;
    return query select raft.current_term, true; return;
end;
$code$
language plpgsql;

create or replace function raft.request_vote(c_term int, c_name raft.name, last_log_id bigint, last_log_term bigint)
returns table(term int, vote_granted boolean) as 
$code$
declare
    state raft.state;
begin
    select * into state from raft.state; 
    if not found then raise sqlstate 'RF001' using message='Node is not initialized'; end if;
    if (state.voted_for is null or state.voted_for=c_name) 
        and (last_log_term, last_log_id)>=(select term, id from raft.log order by term desc, id desc)
    then
        return query select state.term, true;
    else
        if c_term>state.term then
            update raft.state set term=c_term, state='FOLLOWER', voted_for=null;
        end if;
        return query select state.term, false;
    end if;
end;
$code$
language plpgsql;
 

create or replace function raft.candidating() returns int as
$code$
declare
    next_call interval = (select vote_timer - now() from raft.state); 
begin
    if next_call>make_interval() then
        return 
    end if;
end;
$code$
language plpgsql


select extract(epoch from now()-vote_timer) from raft.state


create or replace function idler() returns void as
$code$
begin
    while true loop
        perform pg_sleep(10);
    end loop;    
end;
$code$
language plpgsql

select * from pg_stat_activity

select pg_terminate_backend(16836)

show client_connection_check_interval

alter system set client_connection_check_interval=2000