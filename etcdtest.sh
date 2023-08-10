#!/bin/sh
PSQL=/usr/bin/psql
[ ! "$1" ] && PSQL_OPTS="host=localhost port=45432 user=postgres password=root application_name=psql-gossip-worker-$$" || PSQL_OPTS="$1 application_name=psql-gossip-worker-$$"

CURL=/usr/bin/curl
[ ! "$2" ] && ETCD_URL="http://localhost:23790/v3" || ETCD_URL="$2"

q='"'
LDG_PREFIX="/ldg/"
LDF_RANGE_END="/ldg1"

ldg_root=`echo $LDG_PREFIX|base64`
ldg_root_range_end=`echo $LDF_RANGE_END|base64`

while [ true ] ; do
  if value=`$CURL -s -L ${ETCD_URL}/kv/range -X POST -d "{${q}key${q}: ${q}$ldg_root${q}, ${q}range_end${q}:${q}$ldg_root_range_end${q}, ${q}sort_order${q}:${q}DESCEND${q}, ${q}limit${q}:${q}1${q}}"| jq -r .kvs[0].key`; then
    if [ -z "$value" ] ; then
      sleep 5
      continue
    fi

    if test "$value" = "null" ; then
      height=0
    else
      old_height=`echo "$value"|base64 -d| sed s@${LDG_PREFIX}0*@@`
      height=`expr $old_height + 1`
      echo "Current height is $height"
    fi

    my_height=`$PSQL -X "$PSQL_OPTS" -t --csv -c 'select coalesce(max(height),0) from ldg.ldg'`

    echo "heights $my_height $old_height"

    for ch in `seq $my_height $old_height` ; do
      key64=`printf "${LDG_PREFIX}%015d" $ch`
      key64=`echo $key64|base64`
      reply=`$CURL -s -L $ETCD_URL/kv/range -X POST -d "{${q}key${q}: ${q}$key64${q}}"|jq -r .kvs[0].value`

      if [ "$reply" = "null" ]; then
        echo "Unexpected reply:$reply"
        exit
      fi
      reply=`echo $reply|base64 -d`
      reply=`$PSQL -X "$PSQL_OPTS" -c "call ldg.apply_proposed_block('$reply'::uuid)"`
    done


    if test "$height" != "" ; then
      $PSQL -X "$PSQL_OPTS" -c "call ldg.make_proposed_block()" >/dev/null 2>&1
      key=`printf "${LDG_PREFIX}%015d" $height`
      #echo "[$key]->[$block_uuid]"
      key64=`echo "$key"|base64`

      block_uuid=`$PSQL -X "$PSQL_OPTS" --csv -t -c "select ldg.get_proposed_block_at_height($height);"`
      if [ -n "$block_uuid" ] ; then
        block_uuid_b64=`echo $block_uuid|base64`
        reply=`$CURL -s -L ${ETCD_URL}/kv/txn -X POST -d "{${q}compare${q}:[{${q}createRevision${q}:${q}0${q},${q}target${q}:${q}CREATE${q},${q}key${q}:${q}$key64${q}}],${q}success${q}:[{${q}requestPut${q}:{${q}key${q}:${q}$key64${q},${q}value${q}:${q}$block_uuid_b64${q}}}]}"`
      fi
    fi

    sleep 1
  fi
done