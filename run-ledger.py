import base64
import concurrent.futures
import json
import logging
import pprint
import re
import sys
import threading
import time

import psycopg2
import psycopg2.pool
import requests
import traceback
from contextlib import contextmanager

def etcd_url():
    return "http://localhost:42379/v3"

def ldg_prefix():
    return "/ldg/"

def ldg_range_end():
    return "/ldg1"

def etcd_username():
    return "root"

def etcd_password():
    return "root"

if len(sys.argv)>1:
    dict_arg = sys.argv[1]
    conn_dict = dict( pair.split('=') for pair in dict_arg.split(' ') )
else:
    raise "No connection string specified!"


def get_etcd_cluster_id():
    log = logging.getLogger("get_etcd_cluster_id")
    try:
        return get_etcd_cluster_id.cluster_id
    except AttributeError:
        with get_cn() as cn, cn.cursor() as cr:
            cr.execute("select cluster_id from ldg.etcd")
            get_etcd_cluster_id.cluster_id=cr.fetchone()[0]
            log.info("etcd cluster id=%s", get_etcd_cluster_id.cluster_id)
            return get_etcd_cluster_id.cluster_id

cns = psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=20, **conn_dict)

@contextmanager
def get_cn():
    cn = cns.getconn()
    #cn = psycopg2.connect(**conn_dict)
    cn.autocommit=True
    try:
        yield cn
    finally:
        cns.putconn(cn)
        #cn.close()


es = concurrent.futures.ThreadPoolExecutor(max_workers=20)

stop_all = threading.Event()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name).16s: %(message)s')

urllib_logger = logging.getLogger('urllib3.connectionpool')
urllib_logger.setLevel(logging.ERROR)


def main():
    log=logging.getLogger("main")
    futures=[]
    try:
        with get_cn() as cn, cn.cursor() as cr:
            cn.autocommit=True
            cr.execute("select name from gsp.peer")
            res = cr.fetchall()
            for peer in res:
                futures.append(es.submit(handle_peer, peer[0]))

        log.debug("Peer %s submitted"%(peer[0]))

        futures.append(es.submit(gossip_my_height))
        futures.append(es.submit(get_etcd_height))
        futures.append(es.submit(put_proposed_block_to_etcd))
        futures.append(es.submit(clear_gsp))
        futures.append(es.submit(clear_txpool))
        log.info("All worker threads are added, will wait")

        es.shutdown()
        concurrent.futures.wait(futures, None, concurrent.futures.FIRST_EXCEPTION)
    except BaseException as ex:
        stop_all.set()
        log.error("Exception:%s", str(ex))


def handle_peer(peer):
    log=logging.getLogger("handle_peer")
    try:
        log.info("Start handling peer %s"%(peer,))
        while True:
            if stop_all.isSet():
                return
            with get_cn() as cn, cn.cursor() as cr:
                log.debug("Starting peer %s", peer)
                cr.execute("select gsp.spread_gossips(%s)", (peer,))
                time.sleep(1)
    except BaseException as ex:
        log.error("Exception %s", traceback.format_exc(100))
        stop_all.set()
        raise ex


def gossip_my_height():
    log=logging.getLogger("gossip_my_height")
    last_run=0
    try:
        log.info("Start gossip height")
        while True:
            if stop_all.isSet():
                return
            if time.time()-last_run >=45:
                with get_cn() as cn, cn.cursor() as cr:
                    cn.autocommit=True
                    cr.execute("call ldg.gossip_my_height()")
                    last_run=time.time()
            time.sleep(3)
    except BaseException as ex:
        log.error("Exception %s", traceback.format_exc(100))
        stop_all.set()
        raise ex

def get_etcd_height():
    log=logging.getLogger("get_etcd_height")
    try:
        log.info("Start get_etcd_height")

        while True:
            if stop_all.isSet():
                return

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

            if json['header']['cluster_id']!=get_etcd_cluster_id():
                raise RuntimeError("Cluster id mismatch: expected %s, got %s"%(get_etcd_cluster_id(), json['header']['cluster_id']))

            if json.get("kvs")!=None and len(json.get("kvs"))>0:
                height = base64.standard_b64decode(json["kvs"][0]["key"]).decode()
                height=re.sub("/ldg/0*","", height)
                height=int(height)
                log.debug("Current etcd height is %s"%(height,))
            else:
                log.info("Got empty kvs field in etcd reply, assuming height=0")
                height=0

            try:
                with get_cn() as cn, cn.cursor() as cr:
                    cn.autocommit=True
                    cr.execute("""
                    insert into ldg.etcd(height,connected_at) values(%s,now()) 
                        on conflict(id) do update set height=excluded.height, connected_at=now()
                    """, (height,))
                    cr.execute("delete from ldg.proposed_block pb where height<=%s", (height,))
                log.debug("DB updated with current etcd height")
            except BaseException as ex:
                log.error("Exception %s", ex)
                raise ex

            time.sleep(15)
    except BaseException as ex:
        log.error("Exception %s", traceback.format_exc(100))
        stop_all.set()
        raise ex

thread_local=threading.local()
def get_etcd_headers():
    if etcd_username() is not None:
        try:
            return {"Authorization": thread_local.etcd_token}
        except AttributeError:
            rv = get_sess().post(etcd_url()+"/auth/authenticate",
                json={
                    "name": etcd_username(),
                    "password": etcd_password()
                }
            )
            if rv.json()['header']['cluster_id']!=get_etcd_cluster_id():
                raise RuntimeError("Cluster id mismatch: expected %s, got %s"%(get_etcd_cluster_id(), json['header']['cluster_id']))

            thread_local.etcd_token=rv.json()["token"]
            return {"Authorization": thread_local.etcd_token}
    return {}

def get_sess():
    rv = requests.session()
    if rv is not None:
        return rv
    try:
        return thread_local.sess
    except AttributeError:
        thread_local.sess = requests.session()
        return thread_local.sess

def query_etcd(suburl:str, js:dict):
    log = logging.getLogger("query_etcd")

    try:
        reply = get_sess().post(etcd_url()+suburl, json=js, headers=get_etcd_headers())
        reply_json = reply.json()
        log.debug("Json:%s" % reply_json)

        if reply_json['header']['cluster_id']!=get_etcd_cluster_id():
            raise RuntimeError("Cluster id mismatch: expected %s, got %s"%(get_etcd_cluster_id(), json['header']['cluster_id']))

        if reply_json.get("kvs")!=None and len(reply_json.get("kvs"))>0:
            return True, reply_json, 0, None
        else:
            return True, {}, 0, None
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

    try:
        idle_sleep=0.05;

        while True:
            if stop_all.isSet():
                return
            log.debug("Connection obtained")
            try:
                log.debug("Query etcd for last block")
                success, json, timeout, errmsg = query_etcd("/kv/range", {
                    "key": base64.standard_b64encode(ldg_prefix().encode()).decode(),
                    "range_end": base64.b64encode(ldg_range_end().encode("ascii")).decode(),
                    "sort_order": "DESCEND",
                    "limit":"1"
                })
                log.debug("Etcd reply has been obtained")
                if not success:
                    log.warning(errmsg)
                    time.sleep(timeout)
                    continue

                if json.get("kvs")!=None and len(json.get("kvs"))>0:
                    height = base64.standard_b64decode(json["kvs"][0]["key"]).decode()
                    height = int(re.sub(ldg_prefix()+"0{,14}","", height))
                else:
                    height=-1


                with get_cn() as cn, cn.cursor() as cr:
                    cn.autocommit=True
                    cr.execute("select coalesce(max(height),-1) from ldg.ldg")
                    my_height = cr.fetchone()[0]
                log.debug("My height is %s", my_height)


                for ch in range(int(my_height+1), int(height)+1):
                    log.debug("     Working with height %s"%(ch))
                    idle_sleep=0.2
                    try:
                        log.debug("Query etcd for uuid of block %s", ch)
                        reply = get_sess().post(etcd_url()+"/kv/range", json={
                            "key": base64.standard_b64encode((ldg_prefix()+"%015d"%ch).encode()).decode()
                        }, headers=get_etcd_headers())
                        log.debug("Reply from etcd obtained")
                        if reply=="null":
                            log.error("Unexpected reply when trying to get id of block at height %s"%(ch))
                            stop_all.set()
                            return
                        reply_json : dict = reply.json()

                        if reply_json['header']['cluster_id']!=get_etcd_cluster_id():
                            raise RuntimeError("Cluster id mismatch: expected %s, got %s"%(get_etcd_cluster_id(), json['header']['cluster_id']))

                    except (requests.exceptions.RequestException, json.decoder.JSONDecodeError) as ex:
                        msg = "Connection or parsing error:" + str(ex)
                        logging.warning(msg)
                        time.sleep(10)
                        continue

                    if reply_json.get("kvs") is None and ch>0:
                        msg = "Get empty kvs for page defined in etcd"
                        log.critical(msg)
                        stop_all.set()
                        raise Exception(msg)

                    if reply_json.get("kvs") is not None:
                        block_uuid = base64.standard_b64decode(reply_json["kvs"][0]["value"]).decode()
                        log.info("Block uuid=%s", block_uuid)
                        if block_uuid is None:
                            log.error("json:{}", reply_json)

                        with get_cn() as cn, cn.cursor() as cr:
                            cn.autocommit=True
                            cr.execute("call ldg.apply_proposed_block(%s)", (block_uuid,))
                            log.info("Block %s at height %s has been applied", block_uuid, ch)

                log.debug("Going to create proposed block")
                with get_cn() as cn, cn.cursor() as cr:
                    cr.execute("call ldg.make_proposed_block()")
                    log.debug("Call to ldg.make_proposed_block() is done")

                    cr.execute("select ldg.get_proposed_block_at_height(%s)", [height+1])
                    new_block_uuid=cr.fetchone()[0]

                    log.debug("New block uuid received:%s", new_block_uuid)

                    if new_block_uuid is None:
                        log.debug("Cannot build a new block at height %s"%(height+1))
                        if idle_sleep<3:
                            idle_sleep+=0.05

                        time.sleep(idle_sleep)
                        continue
                    idle_sleep=0.05
                    log.debug("New block uuid=%s"%(new_block_uuid))

                #{"compare":[{"createRevision":"0","target":"CREATE","key":"$key64"}],"success":[{"requestPut":{"key":"$key64","value":"$block_uuid_b64"}}]}
                height_key64 = base64.standard_b64encode((ldg_prefix() + "%015d" % (height+1)).encode()).decode()
                new_block_uuid_b64 = base64.standard_b64encode(str(new_block_uuid).encode()).decode()
                log.info("Going to execute txn in etcd; new height:%d height_key64=%s block_uuid:%s, type %s" % (height+1, height_key64, new_block_uuid, type(new_block_uuid)))
                reply = get_sess().post(etcd_url()+"/kv/txn", json={
                    "compare": [{
                        "createRevision":"0",
                        "target":"CREATE",
                        "key": height_key64
                    }],
                    "success":[{
                        "requestPut":{
                            "key": height_key64,
                            "value": new_block_uuid_b64
                        }
                    }]
                }, headers = get_etcd_headers())
                log.debug("txn executed, reply=%s", reply.json())
                if reply.json().get("error") is not None:
                    raise RuntimeError(reply.json()["error"])

                if reply.json()['header']['cluster_id']!=get_etcd_cluster_id():
                    raise RuntimeError("Cluster id mismatch: expected %s, got %s"%(get_etcd_cluster_id(), json['header']['cluster_id']))

            except BaseException as ex:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                log.error("Exception(%s) %s", ex.__class__, ex)
                log.error("stack trace:", traceback.format_exception(exc_type, exc_value, exc_traceback))
                return

            logging.debug("Block is sent")

    except BaseException as ex:
        log.error("Exception %s", ex)
        stop_all.set()
        raise ex

def clear_gsp():
    log=logging.getLogger("clear_gsp")
    last_run=0
    try:
        while True:
            if stop_all.isSet():
                return
            if time.time()-last_run>=3:
                with get_cn() as cn, cn.cursor() as cr:
                    cr.execute("call gsp.clear_gsp()")
                    log.debug("Clear gsp was executed")
                last_run=time.time()
            time.sleep(1)
    except BaseException as ex:
        log.error("Exception %s", ex)
        stop_all.set()
        raise ex


def clear_txpool():
    log=logging.getLogger("clear_txpool")
    last_run=0
    try:
        while True:
            if stop_all.isSet():
                return
            if time.time()-last_run>=10:
                with get_cn() as cn, cn.cursor() as cr:
                    cr.execute("call ldg.clear_txpool();")
                last_run=time.time()
            time.sleep(1)
    except BaseException as ex:
        log.error("Exception %s", ex)
        stop_all.set()
        raise ex

if __name__=="__main__":
    main()
