import base64
import concurrent
import concurrent.futures
import logging
import pprint
import time

import psycopg2
import psycopg2.extensions

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s %(levelname)s:%(message)s')

cn:psycopg2.extensions.connection=psycopg2._connect("host=127.0.0.1 port=5432 user=postgres password=root dbname=work")
with cn.cursor() as cr:
    cr.itersize=1
    cr.execute("select idler()")
    while True:
        cr.fetchone()