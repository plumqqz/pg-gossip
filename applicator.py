import threading
import concurrent.futures
import psycopg2
import psycopg2.extensions
import psycopg2.pool
from contextlib import contextmanager
import sys

if len(sys.argv)==2:
    ldg_dict_arg = sys.argv[1]
    ldg_conn_dict = dict( pair.split('=') for pair in ldg_dict_arg.split(' ') )

    app_dict_arg = sys.argv[2]
    app_conn_dict = dict( pair.split('=') for pair in app_dict_arg.split(' ') )


ldg_cns= psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=20, **ldg_conn_dict)
app_cns= psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=20, **app_conn_dict)

@contextmanager
def get_ldg_cn():
    cn = ldg_cns.getconn()
    try:
        yield cn
    finally:
        ldg_cns.putconn(cn)

@contextmanager
def get_app_cn() -> psycopg2.extensions.connection:
    cn = app_cns.getconn()
    try:
        yield cn
    finally:
        app_cns.putconn(cn)

def main():
    with get_app_cn() as acn, acn.cursor() as cr:


if __name__=="__main__":
    main()
