-- CONNECTION: url=jdbc:postgresql://localhost:5432/work

create schema raft;

create domain raft.name varchar(64) check(value~'^[a-zA-Z0-9][-_a-zA-Z0-9]*$');
create type raft.peer_state as enum('LEADER','FOLLOWER','CANDIDATE');

drop table raft.state

create table raft.state(
 id int primary key default 1 check(id=1),
 current_term int check(current_term>=0),
 voted_for raft.name,
 commit_index bigint,
 last_applied_index bigint,
 state raft.peer_state not null,
 next_vote timestamptz,
 self raft.name,
 elected raft.name
);

--drop table raft.voting;
create table raft.voting(
    term int check(term>=0) primary key,
    total_votes_for_me int not null check(total_votes_for_me>0)
);


create table raft.log(
  index bigint primary key,
  term int not null check(term>=0),
  command text not null
);
create index on raft.log(term);

create or replace procedure raft.start_election(params jsonb, result out jsonb, next_run_after interval) as
$code$
<<code>>
declare 
 ct int;
 self raft.name;
 elected raft.name;
 ct2 int;
 r record;
 total_peers int;
begin
    select count(*) into total_peers from raft.peer;
    if total_peers<2 then
        raise sqlstate 'RF003' using message=format('Peers count must be >=2, but found only %s', total_peers);
    end if;
--
    update raft.state as s set current_term=s.current_term+1, voted_for=s.self, state='CANDIDATE' returning s.current_term, s.self into code.ct, code.self;
    insert into raft.voting(term, candidate,total_votes, stop)values(ct, self,1);
    commit;
--
    while true loop
        if exists(select * from raft.state s where s.current_term>ct) then
            update raft.voting set stop_all=true where term=ct;
            commit;
            result=null; next_run_after=null; return;
        end if;
        select v.elected into code.elected from raft.voting v where v.term=ct;
--    
     if not found then
            commit;
            raise sqlstate 'RF003' using message='Term has been deleted during election';
        end if;
--
     if elected=self then
            update raft.voting set stop_all=true where term=ct;
            update raft.state set state='LEADER';
            commit;
        end if;
    end loop;
    
end;
$code$
language plpgsql;

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
        raise sqlstate 'RF003' using message='Unknown peer';
    end if;
    if cn_name<>all(dblink.dblink_get_connections()) then
        perform dblink.dblink_connect(cn_name, peer.conn_str);
    end if;
    while true loop
        select state into node_status from raft.state;
        if not found then
            raise sqlstate 'RF001' using message='This raft node has not been initialized';
        end if;
        if node_status.status='LEADER' then
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
 id bigint;
begin
    if not exists(select * from raft.state) then raise sqlstate 'RF001' using message='Node is not initialized'; end if;
    if exists(select * from raft.state where ae_term<state.current_term) then return query select raft.current_term, false; return; end if;
    if ae_term>state.term then
        update raft.state set term=ae_term, state='FOLLOWER', voted_for=null, vote_timer=vote_timer+vote_interval+random()*vote_interval;
    end if;
    if not exists(select * from raft.log where log.term=prev_log_term and log.id=prev_log_id) then 
        return query select raft.current_term, false; return; 
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
 

