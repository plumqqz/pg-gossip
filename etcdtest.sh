#!/bin/sh
PSQL=/usr/bin/psql
[ ! "$1" ] && PSQL_OPTS="host=localhost port=45432 user=postgres password=root application_name=psql-gossip-worker-$$" || PSQL_OPTS="$1 application_name=psql-gossip-worker-$$"

CURL=/usr/bin/curl
[ ! "$2" ] && ETCD_URL="http://localhost:23790" || ETCD_URL="$2"


 # {${q}compare${q}:[{${q}version${q}:${q}0${q},${q}result${q}:${q}EQUAL${q},${q}target${q}:${q}VERSION${q},${q}key${q}:${q}$key${q}}]}]}
#key=`echo /zzqq/01|base64`
#val=`echo dodo-version2|base64`
#echo $val
#curl -s -L http://localhost:23790/v3/kv/txn -X POST -d '{"compare":[{"version":"0","target":"CREATE","key":"L3p6cXEvMDEK"}],"success":[{"requestPut":{"key":"L3p6cXEvMDEK","value":"ZG9kbwo="}}]}'
#curl -s -L http://localhost:23790/v3/kv/txn -X POST -d '{"compare":[{"version":"0","target":"CREATE","key":"L3p6cXEvMDEK"}],"success":[{"requestPut":{"key":"L3p6cXEvMDEK","value":"ZG9kby12ZXJzaW9uMgo="}}]}'

#exit
q='"'
ldg_root=`echo '/ldg/000000000000000'|base64`

while [ true ] ; do
  $PSQL -X "$PSQL_OPTS" -c "call ldg.make_proposed_block()" >/dev/null 2>&1
  if value=`$CURL -s -L ${ETCD_URL}/v3/kv/range -X POST -d "{${q}key${q}: ${q}$ldg_root${q}, ${q}sort_order${q}:${q}DESCEND${q}, ${q}limit${q}:${q}1${q},${q}limit${q}:true }"| jq -r .kvs[0].key`; then
    if [ -z "$value" ] ; then
      sleep 5
      continue
    fi

    if test "$value" = "null" ; then
      height='0'
    else
      value=`echo "$value"|base64 -d| sed s@/ldg/0*@@`
      height=`expr $height + 1`
    fi
    if test "$height" != "" ; then
      block_uuid=`$PSQL -X "$PSQL_OPTS" --csv -t -c "select ldg.get_proposed_block_at_height($height);"`
      key=`printf '/ldg/%015d' $height`
      echo "[$key]->[$block_uuid]"
      key64=`echo "$key"|base64`
      block_uuid_b64=`echo $block_uuid|base64`
      reply=`$CURL -s -L ${ETCD_URL}/v3/kv/txn -X POST -d "{${q}compare${q}:[{${q}createRevision${q}:${q}0${q},${q}target${q}:${q}CREATE${q},${q}key${q}:${q}$key64${q}}],${q}success${q}:[{${q}requestPut${q}:{${q}key${q}:${q}$key64${q},${q}value${q}:${q}$block_uuid_b64${q}}}]}"`
      echo $reply
      reply=`$CURL -s -L $ETCD_URL/v3/kv/range -X POST -d "{${q}key${q}: ${q}$key64${q},${q}limit${q}:${q}1${q}"}|jq -r .kvs[0].value|base64 -d`
      echo $reply
    fi

    sleep 1
  fi
done