import json
import pprint
import time

import psycopg2
import psycopg2.pool
import sys
import concurrent.futures
import requests
import base64
import re
import logging

import urllib3.exceptions


def etcd_url():
    return "http://localhost:23790/v3"

def ldg_prefix():
    return "/ldg/"

def ldg_range_end():
    return "/ldg1"

if len(sys.argv)>1:
    dict_arg = sys.argv[1]
    conn_dict = dict( pair.split('=') for pair in dict_arg.split(' ') )
else:
    raise "No connection string specified!"

es = concurrent.futures.ThreadPoolExecutor(max_workers=20)

cns = psycopg2.pool.ThreadedConnectionPool(1,20, **conn_dict)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s:%(message)s')

urllib_logger = logging.getLogger('urllib3.connectionpool')
urllib_logger.setLevel(logging.ERROR)


def do_work():
    log=logging.getLogger("do_work")
    futures=[]
    cn: psycopg2.connection = cns.getconn()
    try:
        with cn.cursor() as cr:
            cr.execute("select name from gsp.peer")
            res = cr.fetchall()
            for peer in res:
                futures.append(es.submit(handle_peer, peer[0]))
        cn.commit()
        log.debug("Peer %s submitted"%(peer[0]))
    except BaseException as ex:
        log.error("Exception:%s"%ex)
        exit(1)
    finally:
        cns.putconn(cn)

    futures.append(es.submit(gossip_my_height))
    futures.append(es.submit(get_etcd_height))
    futures.append(es.submit(put_proposed_block_to_etcd))
    futures.append(es.submit(clear_gsp))
    log.info("All worker threads are added, will wait")
    es.shutdown()
    concurrent.futures.wait(futures, None, concurrent.futures.FIRST_EXCEPTION)


def handle_peer(peer):
    log=logging.getLogger("handle_peer")
    cn : psycopg2.connection = cns.getconn()
    cn.autocommit=True
    log.info("Start handling peer %s"%(peer,))
    try:
        with cn.cursor() as cr:
            cr.execute("call gsp.constantly_spread_gossips(%s)", [peer])
            cr.fetchall()
    except BaseException as ex:
        print(type(ex).__name__+":"+str(ex))
    finally:
        cns.putconn(cn)


def gossip_my_height():
    log=logging.getLogger("gossip_my_height")
    cn: psycopg2.connection = cns.getconn()
    cn.autocommit=True
    log.info("Start gossip height")
    try:
        with cn.cursor() as cr:
            cr.execute("call ldg.constantly_gossip_my_height()")
            cr.fetchall()
    except Exception as ex:
        print(ex)
    finally:
        cns.putconn(cn)

def get_etcd_height():
    log=logging.getLogger("get_etcd_height")
    log.info("Start get_etcd_height")

    while True:
        success, json, timeout, errmsg = query_etcd("/kv/range", {
            "key": base64.standard_b64encode(ldg_prefix().encode()).decode(),
            "range_end": base64.b64encode(ldg_range_end().encode("ascii")).decode(),
            "sort_order": "DESCEND",
            "limit":"1"
        })

        if not success:
            log.warning(errmsg)
            time.sleep(timeout)
            continue

        if json.get("kvs")!=None and len(json.get("kvs"))>0:
            height = base64.standard_b64decode(json["kvs"][0]["key"]).decode()
            height=re.sub("/ldg/0*","", height)
            height=int(height)
            log.debug("Current etcd height is %s"%(height,))
        else:
            log.info("Got empty kvs field in etcd reply, assuming height=0")
            height=0

        cn: psycopg2.connection = cns.getconn()
        old_autocommit=cn.autocommit
        cn.autocommit=True

        try:
            with cn.cursor() as cr:
                cr.execute("""
                insert into ldg.etcd(height,connected_at) values(%s,now()) 
                    on conflict(id) do update set height=excluded.height, connected_at=now()
                """, (height,))
                cr.execute("delete from ldg.proposed_block pb where height<=%s", (height,))
            log.debug("DB updated with current etcd height")
        finally:
            cn.autocommit=old_autocommit
            cns.putconn(cn)

        time.sleep(15)

def query_etcd(suburl:str, js:dict):
    log = logging.getLogger("query_etcd")
    try:
        reply = requests.post(etcd_url()+suburl, json=js)
        reply_json = reply.json()
        log.debug("Json:%s" % reply_json)
        if reply_json.get("kvs")!=None and len(reply_json.get("kvs"))>0:
            return True, reply_json, 0, None
        else:
            return True, None, 0, None
    except KeyError as ex:
        msg = "Cannot find kvs key in reply"
        log.warning(msg)
        return False, None, 30, msg
    except IndexError as ex:
        msg = "Cannot find 0 index in kvs key in reply"
        log.warning(msg)
        return False, None, 30, msg
    except (requests.exceptions.RequestException, json.decoder.JSONDecodeError) as ex:
        msg = "Connection or parsing error:" + str(ex)
        logging.warning(msg)
        return False, None, 30, msg


def put_proposed_block_to_etcd():
    log=logging.getLogger("put_proposed_block_to_etcd")
    while True:
        cn: psycopg2.connection = cns.getconn()
        old_autocommit=cn.autocommit
        cn.autocommit=True
        try:
            success, json, timeout, errmsg = query_etcd("/kv/range", {
                    "key": base64.standard_b64encode(ldg_prefix().encode()).decode(),
                    "range_end": base64.b64encode(ldg_range_end().encode("ascii")).decode(),
                    "sort_order": "DESCEND",
                    "limit":"1"
                })
            if not success:
                log.warning(errmsg)
                time.sleep(timeout)
                continue

            if json.get("kvs")!=None and len(json.get("kvs"))>0:
                height = base64.standard_b64decode(json["kvs"][0]["key"]).decode()
                height = int(re.sub(ldg_prefix()+"0+","", height))
            else:
                height=0


            with cn.cursor() as cr:
                cr.execute("select coalesce(max(height),0) from ldg.ldg")
                my_height = cr.fetchone()[0]
            log.debug("My height is %s"%(my_height,))

            for ch in range(int(my_height)+1, int(height)+1):
                log.debug("     Working with height %s"%(ch))

                try:
                    reply = requests.post(etcd_url()+"/kv/range", json={
                        "key": base64.standard_b64encode((ldg_prefix()+"%015d"%ch).encode()).decode()
                    })
                    if reply=="null":
                        log.error("Unexpected reply when trying to get id of block at height %s"%(ch))
                        return
                    reply_json : dict = reply.json()
                except (requests.exceptions.RequestException, json.decoder.JSONDecodeError) as ex:
                    msg = "Connection or parsing error:" + str(ex)
                    logging.warning(msg)
                    time.sleep(10)
                    continue


                if reply_json.get("kvs")==None:
                    msg = "Get empty kvs for page defined in etcd"
                    log.critical(msg)
                    raise Exception(msg)

                block_uuid = base64.standard_b64decode(reply_json["kvs"][0]["value"]).decode()
                log.debug("Block uuid=%s"%(block_uuid))

                with cn.cursor() as cr:
                    cr.execute("call ldg.apply_proposed_block(%s)", (block_uuid,))
                    log.info("Block %s at height %s has been applied", block_uuid, ch)

            with cn.cursor() as cr:
                cr.execute("call ldg.make_proposed_block()")
                cr.execute("select ldg.get_proposed_block_at_height(%s)"%(height+1))
                new_block_uuid=cr.fetchone()[0]

                if new_block_uuid==None:
                    log.debug("Cannot build a new block at height %s"%(height+1))
                    time.sleep(5)
                    continue

                log.debug("New block uuid=%s"%(new_block_uuid))

            #{"compare":[{"createRevision":"0","target":"CREATE","key":"$key64"}],"success":[{"requestPut":{"key":"$key64","value":"$block_uuid_b64"}}]}
            height_key = base64.standard_b64encode((ldg_prefix() + "%015d" % (height + 1)).encode()).decode()
            new_block_uuid_b64 = base64.standard_b64encode(str(new_block_uuid).encode()).decode()
            log.error("new height:%d height_key=%s block_uuid:%s"%(height+1, height, new_block_uuid))
            requests.post(etcd_url()+"/kv/txn", json={
                "compare": [{
                    "createRevision":"0",
                    "target":"CREATE",
                    "key": height_key
                }],
                "success":[{
                    "requestPut":{
                        "key": height_key,
                        "value": new_block_uuid_b64
                    }
                }]
            })


        finally:
            if cn!=None:
                cn.autocommit=old_autocommit
                cns.putconn(cn)

        logging.debug("Block is sent")
        time.sleep(1)

def clear_gsp():
    while True:
        cn:psycopg2.connection=cns.getconn()
        try:
            with cn.cursor() as cr:
                cr.execute("call gsp.clear_gsp()")
                cn.commit()
        finally:
            cns.putconn(cn)
        time.sleep(60)

if __name__=="__main__":
    do_work()
    #get_etcd_height()
    #put_proposed_block_to_etcd()
