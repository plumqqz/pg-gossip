#!/bin/bash
PSQL=/usr/bin/psql
#PSQL_OPTS="host=localhost user=postgres password=root dbname=work"
[[ ! "$1" ]] && PSQL_OPTS="host=localhost user=postgres password=root dbname=work" || PSQL_OPTS=$1

trap 'kill $(jobs -p)' EXIT TERM INT

function spread_to_peer(){
     $PSQL -X "$PSQL_OPTS" -c "call gsp.constantly_spread_gossips('$1')" >/dev/null
}

echo "$PSQL $PSQL_OPTS"
peers=$($PSQL -X --csv -t "$PSQL_OPTS" -c 'select name from gsp.peer')

pids=[]
i=0;
for peer in $peers ; do
     spread_to_peer $peer &
     pids[i++]=$!
done

if [ "X$pids" != "X" ]; then
    wait $pids
fi
