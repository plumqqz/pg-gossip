import logging
import time

import psycopg2

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s %(levelname)s:%(message)s')


def retry_forever(fn):
    def wrapper(*args, **kwargs):
        log = logging.getLogger(fn.__name__)
        sleep_timeout=1

        while True:
            try:
                rv = fn()
                sleep_timeout=1
            except Exception as ex:
                log.error("Exception in %s:%s", fn.__name__, ex)
                log.error("Going to sleep")
                time.sleep(sleep_timeout)
                if sleep_timeout<60:
                    sleep_timeout+=1
                continue
        return rv
    return wrapper

@retry_forever
def calltest():
    conn = psycopg2.connect(dbname='work', user='postgres', host='localhost', password='root')
    cursor=conn.cursor()
    three=0
    cursor.execute("call public.test(%s,%s,%s)",(10,2,three))
    result = cursor.fetchone()
    print(result[0])

params = {
    "name":"vasya",
    "age":111
}
calltest(100, **params)