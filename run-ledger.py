import pprint

import psycopg2
import psycopg2.pool
import sys
import concurrent.futures

if len(sys.argv)>1:
    dict_arg = sys.argv[1]
    conn_dict = dict( pair.split('=') for pair in dict_arg.split(' ') )
else:
    raise "No connection string specified!"

es = concurrent.futures.ThreadPoolExecutor(max_workers=20)

cns = psycopg2.pool.ThreadedConnectionPool(1,20, **conn_dict)

def do_work():
    futures=[]
    cn: psycopg2.connection = cns.getconn()
    try:
        with cn.cursor() as cr:
            cr.execute("select name from gsp.peer")
            res = cr.fetchall()
            for peer in res:
                futures.append(es.submit(handle_peer, peer[0]))
    except BaseException as ex:
        pprint.pprint(ex)
    finally:
        cns.putconn(cn)

    futures.append(es.submit(gossip_my_height))
    es.shutdown()
    concurrent.futures.wait(futures, None, concurrent.futures.FIRST_EXCEPTION)


def handle_peer(peer):
    print(f"peer:{peer}\n")
    cn : psycopg2.connection = cns.getconn()
    cn.autocommit=True
    try:
        with cn.cursor() as cr:
            cr.execute("call gsp.constantly_spread_gossips(%s)", [peer])
            cr.fetchall()
    except BaseException as ex:
        print(type(ex).__name__+":"+str(ex))

def gossip_my_height():
    cn: psycopg2.connection = cns.getconn()
    cn.autocommit=True
    try:
        with cn.cursor() as cr:
            cr.execute("call ldg.constantly_gossip_my_height()")
            cr.fetchall()
    except Exception as ex:
        print(ex)

def hanlde_ldg_height():

if __name__=="__main__":
    do_work()