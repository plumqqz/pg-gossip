#!/bin/bash
PSQL=/usr/bin/psql
[[ ! "$1" ]] && PSQL_OPTS="host=localhost user=postgres password=root dbname=work application_name=psql-gossip-worker" || PSQL_OPTS="$1 application_name=psql-gossip-worker"

trap 'kill_all_psqls; kill $(jobs -p); kill $pids' EXIT TERM INT

function spread_to_peer(){
     $PSQL -X "$PSQL_OPTS" -c "call gsp.constantly_spread_gossips('$1')" >/dev/null 2>&1
}

function gossip_my_height(){
     $PSQL -X "$PSQL_OPTS" -c "call ldg.constantly_gossip_my_height()" >/dev/null 2>&1
}

function kill_all_psqls(){
     $PSQL -X "$PSQL_OPTS" -c "select pg_terminate_backend(pid) from pg_stat_activity where application_name='psql-gossip-worker'" >/dev/null
}


echo "$PSQL $PSQL_OPTS"
peers=$($PSQL -X --csv -t "$PSQL_OPTS" -c 'select name from gsp.peer')

pids=[]
i=0;
for peer in $peers ; do
     spread_to_peer $peer &
     pids[i++]=$!
done

gossip_my_height &
pids[i++]=$!


if [ "X$pids" != "X" ]; then
    wait $pids
fi
