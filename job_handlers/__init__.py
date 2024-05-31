import psycopg2.pool

cns = psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=20, host="localhost", port=5432, dbname="work", user="postgres", password="root")
handlers={}