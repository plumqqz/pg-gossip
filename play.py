import base64
import concurrent
import concurrent.futures
import logging
import pprint
import time

import psycopg2

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s %(levelname)s:%(message)s')
new_block_uuid=''
print(type(new_block_uuid))
exit

es = concurrent.futures.ThreadPoolExecutor(max_workers=20)
def start_work():
    cn=psycopg2.connect(host="localhost", port=45432, user="postgres", password="root")
    while True:
        with cn.cursor() as cr:
            rv = cr.execute("select ldg.get_proposed_block_at_height(%s)", (50,))
            #pprint.pp({ "rv":rv})
            val = cr.fetchone()
            new_block_uuid= val[0]
            if new_block_uuid is None:
                continue
            logging.error("new_block_uuid:%s", pprint.pformat(new_block_uuid))
            logging.error("val=%s", pprint.pp(val))
            return

def main():
    futures=[]
    for v in range(1,10):
        futures.append(es.submit(start_work))
    concurrent.futures.wait(futures, None, concurrent.futures.FIRST_EXCEPTION)

if __name__=="__main__":
    main()
