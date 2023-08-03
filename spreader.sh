#!/bin/bash
PSQL=/usr/bin/psql
PSQL_OPTS="host=localhost user=postgres password=root dbname=work"

trap 'kill $(jobs -p)' EXIT TERM INT

function spread_to_peer(){
  while true; do
     $PSQL -X "$PSQL_OPTS" -c "select gsp.spread_gossips('$1')" >/dev/null
     echo Sleeping
     sleep 1
  done
}

echo "$PSQL $PSQL_OPTS"
peers=$($PSQL -X --csv -t "$PSQL_OPTS" -c 'select name from gsp.peer')

pids=[]
i=0;
for peer in $peers ; do
     spread_to_peer $peer &
     pids[i++]=$!
done

echo Pids: $pids
wait $pids
