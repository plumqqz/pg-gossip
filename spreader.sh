#!/bin/sh
PSQL=/usr/bin/psql
[ ! "$1" ] && PSQL_OPTS="host=localhost user=postgres password=root dbname=work application_name=psql-gossip-worker-$$" || PSQL_OPTS="$1 application_name=psql-gossip-worker-$$"


spread_to_peer(){
     $PSQL -X "$PSQL_OPTS" -c "call gsp.constantly_spread_gossips('$1')" >/dev/null 2>&1
}

gossip_my_height(){
     $PSQL -X "$PSQL_OPTS" -c "call ldg.constantly_gossip_my_height()" >/dev/null 2>&1
}

kill_all_psqls(){
     $PSQL -X "$PSQL_OPTS" -c "select pg_terminate_backend(pid) from pg_stat_activity where application_name='psql-gossip-worker-$$'" >/dev/null 2>&1
}

trap 'kill_all_psqls; kill $pids' EXIT TERM INT

echo "$PSQL $PSQL_OPTS"
peers=`$PSQL -X --csv -t "$PSQL_OPTS" -c 'select name from gsp.peer'`

pids=''
for peer in $peers ; do
     spread_to_peer $peer &
     pids="$pids $!"
done

gossip_my_height &

height_pid=$!


while [ true ]; do
    sleep 3600
done