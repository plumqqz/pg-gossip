#!/bin/sh
ledger_prefix=/ldg/
lp64=$(echo $ledger_prefix|base64)
q='"'
echo $q
echo "{${q}key${q}: ${q}$lp64${q}, ${q}sort_order$q:${q}DESCEND${q}}"
curl -L http://localhost:23790/v3/kv/range -X POST -d "{${q}key${q}: ${q}$lp64${q}, ${q}sort_order$q:${q}DESCEND${q}}"
