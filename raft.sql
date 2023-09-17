-- CONNECTION: url=jdbc:postgresql://localhost:5432/work
/*
 * воркеры - у каждого интервал запуска 3 сек
 * начинатель голосования - проверяет, что next_election>now() && state<>'LEADER' и если да, 
 *      переключает свою ноду в CANDIDATE
 *      сбрасывает state.leader_name
 *      инкрементирует term
 *      голосует за себя - ставит voted_for_me_in_current_term=1 и voted_for=self
 *      ставит next_election=now()+make_interval(secs=10+10*random())
 * 
 * голосователь для каждой соседней ноды - если его родная нода CANDIDATE а он еще не голосовал за нее в этом терме, то:
 *      прописывает в контекст текущий терм из state
 *      пытается проголосовать за себя на назначенной ему ноде (вызывает request_vote), если нода еще CANDIDATE
 *          если не CANDIDATE, то просто возврат
 *          если ошибка класса 08000 - просто возврат
 *      если набралось достаточно голосов, переключает ноду в LEADER, устанавливает leader_name в себя, если нода все еще CANDIDATE
 *          если не CANDIDATE, то просто возврат
 *      если term с той ноды > current_term, переключается в FOLLOWER, сбрасывает state.leader_name, если нода все еще CANDIDATE
 * 
 * рассылатель для каждой соседней ноды - если его родная нода LEADER, отправляет на назначенную лог и заодно heartbeat (вызывает append_entries)
 *      если term с той ноды > current_term, переключается в FOLLOWER, сбрасывает state.leader_name, если нода все еще LEADER
 * 
 * отправлятель команд
 *      если leader_name не пуст, отправляет на него команды
 */

create schema raft;

create domain raft.name varchar(64) check(value~'^[a-zA-Z0-9][-_a-zA-Z0-9]*$');
create type raft.peer_state as enum('LEADER','FOLLOWER','CANDIDATE');

drop table raft.state

create table raft.state(
 id int primary key default 1 check(id=1),
 current_term int check(current_term>=0),
 voted_for raft.name,
 commit_index bigint,
 leader_name raft.name,
 last_applied_index bigint,
 state raft.peer_state not null,
 next_election timestamptz,
 self raft.name,
 cluster_name raft.name not null,
 voted_for_me_in_current_term int
);


create table raft.peer(
 name raft.name not null primary key,
 conn_str text not null,
 next_index bigint check(next_index>=0),
 match_index bigint check(match_index>=0)
);

create table raft.log(
  term int not null check(term>=0),
  index bigint primary key,
  command text not null
);
create index on raft.log(term);

select raft.start('test1','gsp9')


create or replace function raft.start(cluster_name raft.name, self raft.name, force boolean default false) returns void as
$code$
begin
    insert into raft.state(
    id, current_term, voted_for,
    commit_index, last_applied_index,state,
    next_election, self, cluster_name
    ) values (
    1, 0, null,
    -1, -1, 'FOLLOWER',
    now(), start.self, start.cluster_name
    ) on conflict(id) do update
      set current_term=excluded.current_term, voted_for=excluded.voted_for,
        commit_index=excluded.commit_index, last_applied_index=excluded.last_applied_index, state=excluded.state,
        next_election=excluded.next_election
    where force;
end;    
$code$
language plpgsql;


create or replace function raft.append_entries(cluster_name raft.name, ae_term int, leader_name raft.name,
prev_log_index bigint, prev_log_term int, entries raft.log[], leader_commit_index bigint) 
returns table(term int, success boolean) as
$code$
declare
 id bigint;
 apply_to_index bigint;
begin
    if not exists(select * from raft.state where state.cluster_name=append_entries.cluster_name) then raise sqlstate 'RF001' using message='Node is not initialized or cluster_name is invalid'; end if;
    if exists(select * from raft.state where ae_term<state.current_term) then return query select raft.current_term, false; return; end if;
    if ae_term>=state.term then
        update raft.state set term=ae_term, state='FOLLOWER', voted_for=null, state.leader_node=append_entries.leader_node, vote_timer=vote_timer+vote_interval+random()*vote_interval;
    end if;
    if not exists(select * from raft.log where log.term=prev_log_term and log.index=prev_log_index) then 
        return query select raft.current_term, false; return; 
    end if;
--
    delete from raft.log where log.index>prev_log_index;
--
    insert into raft.log select * from unnest(entries) as e;
--
    update raft.state set commit_index=least(leader_commit_id,(select max(e.id) from unnest(entries) e)), 
                            next_run=now()+make_interval(secs:=10),
                            commit_index=leader_commit_index,
                            leader_name=append_entries.leader_name
        where leader_commit_index>state.last_applied_index
        returning commit_index into apply_to_index;
    if found then
        for id in state.last_applied_index+1..apply_to_index loop
            raise notice 'apply id %', id;
        end loop;
    else
        update raft.state set commit_id=least(leader_commit_id,(select max(e.id) from unnest(entries) e)), 
                                next_run=now()+make_interval(secs:=10);
    end if;
    return query select raft.current_term, true; return;
end;
$code$
language plpgsql;



create or replace function raft.request_vote(cluster_name raft.name, c_term int, c_name raft.name, last_log_id bigint, last_log_term bigint)
returns table(term int, vote_granted boolean) as 
$code$
declare
    state raft.state;
begin
    if not exists(select * from raft.state s where s.cluster_name=request_vote.cluster_name) then 
        raise sqlstate 'RF001' using message=format('Node is not initialized or cluster_name=%s is invalid', request_vote.cluster_name); 
    end if;
--
    update raft.state s set current_term=c_term, voted_for=c_name, state='FOLLOWER', voted_for_me_in_current_term=0, leader_node=null
        where (s.current_term<c_term or s.current_term=c_term and coalesce(voted_for, c_name)=c_name)  
        and (last_log_term, last_log_id)>=(select log.term, log.index from raft.log order by log.term desc, log.index desc);
    if found then
        return query select state.term, true from raft.state;
    else
        return query select state.term, false from raft.state;
    end if;
end;
$code$
language plpgsql;

create or replace function raft.start_voting_job(params jsonb, ctx in out jsonb, next_run out interval)  as
$code$
/*
 * начинатель голосования - проверяет, что next_election>now() && state<>'LEADER' и если да, 
 *      переключает свою ноду в CANDIDATE
 *      сбрасывает state.leader_name
 *      инкрементирует term
 *      голосует за себя - ставит voted_for_me_in_current_term=1 и voted_for=self
 *      ставит next_election=now()+make_interval(secs=10+10*random()) 
 */
begin
    next_run=make_interval(secs=3);
-- switches node from follower state
    update raft.state set 
            state='CANDIDATE', current_term=current_term+1, voted_for_me_in_current_term=1, 
            voted_for=self, leader_name=null,
            next_election=now()+make_interval(secs=10+10*random())
        where next_election>now() and state.state in ('FOLLOWER','CANDIDATE');
    ctx=jsonb_build_object();
end;
$code$
language plpgsql;

create or replace function raft.call_request_vote_job(params jsonb, ctx in out jsonb, next_run out interval) as
$code$
/*
 *  голосователь для каждой соседней ноды - если его родная нода CANDIDATE а он еще не голосовал за нее в этом терме, то:
 *      прописывает в контекст текущий терм из state
 *      пытается проголосовать за себя на назначенной ему ноде (вызывает request_vote), если нода еще CANDIDATE
 *          если не CANDIDATE, то просто возврат
 *          если ошибка класса 08000 - просто возврат
 *      если нода проголосовала за и набралось достаточно голосов, переключает ноду в LEADER, устанавливает leader_name в себя, если нода все еще CANDIDATE
 *          если не CANDIDATE, то просто возврат
 *      если term с той ноды > current_term, переключается в FOLLOWER, сбрасывает state.leader_name, если нода все еще CANDIDATE
 */
declare
  p raft.peer;
  self raft.state;
  rt int; -- Remote Term
  vg boolean; -- Vote Granted
begin
-- initial setup; in self we will use only such fields, which are constant, for volatile fields we must use queries
    select * into self from raft.state;
    if not found then
        raise sqlstate 'RF001' using message=format('Node is not initialized');
    end if;
    select * into p from raft.peer where peer.name=params->>'peer_name';
    if not found then
        raise sqlstate 'RF003' using message=format('Peer with name=%s is not found', params->>'peer_name');
    end if;
--
    next_run=make_interval(secs=3);
    ctx['last_seen_term']=self.current_term;
-- real work goes here
    begin
       select *
            into rt, vg
            from raft.state, 
            (select * from raft.log order by term desc, index desc limit 1) as log,
            dblink.dblink(p.conn_str, format($Q$select * from raft.request_vote(%L,%d,%L,%d,%d)$Q$, 
                                                state.cluster_name, state.current_term, state.self, log.term, log.index)) as t(term int, vg boolean) 
           where state.state='CANDIDATE';
       if not found then
            return;
       end if;
    exception
        when sqlstate '08000' then return;
    end;
    update raft.state set current_term=rt, state='FOLLOWER', voted_for=null, voted_for_me_in_current_term=0 where rt>current_term and state='CANDIDATE';
    if found then
        return;
    end if;
    update raft.state set state='LEADER', voted_for_me_in_current_term=voted_for_me_in_current_term+1, leader_name=null
    where state.voted_for_me_in_current_term+1>(select count(*)/2+1 from raft.peer) and state='CANDIDATE';
end;
$code$
language plpgsql;
 


do $code$
 declare
  term int;
  vg boolean;
  begin
      select * into term, vg from dblink.dblink('dbname=work password=root', $Q$select * from raft.request_vote('test',0,'g1',0,0)$Q$) as t(term int, vg boolean);
      raise notice 'term=% success=%', term, vg;
  end;
$code$

select * from dblink.dblink('dbname=work password=root', $Q$select * from raft.request_vote('test',0,'g1',0,0)$Q$) as t(term int, vg boolean);


do $code$
declare
  ctx jsonb;
  begin
    ctx['last_seen_term']=(select current_term from raft.state);
    raise notice 'ctx=% term=%', ctx, (ctx->>'last_seen_term')::int+1;
  end;
$code$
