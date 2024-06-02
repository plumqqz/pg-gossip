import json
import time
from queue import Queue
import psycopg2
import psycopg2.extras
import concurrent.futures
import re
import logging
import fcntl

import pkgutil
import traceback
import job_handlers
import job_handlers.examples

# create table playground.job(
#     id bigint generated always as identity primary key,
# lock_hash text,
# next_run_at timestamptz not null default now(),
# created_at timestamp default now(),
# finished_at timestamp,
# error_message text,
# type text not null,
# action text not null,
# params json,
# context json,
# depends_on bigint[]
# );


CONNECTION_COUNT=2
CQ = Queue(maxsize=CONNECTION_COUNT)
TP = concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTION_COUNT)

def exec_job(cn, row):
    try:
        if not re.fullmatch(r'[:a-zA-Z.0-9_]+', row['action']):
            raise RuntimeError("Wrong action:"+row['action'])

        if row['type']=='sql':
            with cn, cn.cursor() as cr:
                cr.execute("with tq as(select "+row['action']+"(job.params, job.context) as reply from playground.job where job.id=%s)"
                            " update playground.job set finished_at=case when tq.reply is null then now() else null end, next_run_at=(tq.reply->>'next_run')::timestamp,"
                            " error_message=null, context=tq.reply->'context' from tq where job.id=%s returning context", [row['id'],row['id']])
        elif row['type']=='python':
            res = pkgutil.resolve_name('job_handlers:handlers')[row['action']](row['params'], row['context'] if row['context'] else {})
            if res is not None:
                with cn, cn.cursor() as cur:
                    cur.execute("update playground.job set next_run_at= clock_timestamp()+make_interval(secs := %s), context= %s where id=%s", [float(res['next_run_after_secs']),json.dumps(res), row['id']])
            else:
                with cn, cn.cursor() as cur:
                    cur.execute('update playground.job set next_run_at= null where id=%s', [row['id']])
        else:
            raise RuntimeError("Unknown job type:"+row['type'])

    except Exception as e:
        logging.error("Exception:%s", traceback.print_exc())
        with cn, cn.cursor() as cur:
            cur.execute("rollback")
            cur.execute("update playground.job set error_message= %s where id=%s", [str(e),row['id']])
            res=cur.fetchone()
            logging.error("Res=%s", res)
    finally:
        setConnectionApplicationNameToIdle(cn)
        CQ.put(cn)

def run():
    while True:
        try:
            cn = CQ.get(True)
            found=False
            with cn, cn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("select * from playground.job"+
                            " where error_message is null and next_run_at <now() and not exists(select * from pg_stat_activity where application_name='job_executor:job_id:'||job.id)"
                            " and not exists(select * from playground.job j1 where j1.id=any(job.depends_on))"+
                            " order by created_at limit 1 for update skip locked")
                row = cur.fetchone()
                if not row is None:
                    found=True
                    cur.execute("set application_name to %s", ['job_executor:job_id:' + str(row['id'])])
            if found:
                TP.submit(exec_job, cn, row)
            else:
                CQ.put(cn)
                time.sleep(3)
                continue

        except Exception as e:
            traceback.print_exc()
            logging.error("Exception:%s", str(e))


def setConnectionApplicationNameToIdle(cn):
    with cn, cn.cursor() as cur:
        cur.execute("set application_name to 'job-executor:idle'")

if __name__ == '__main__':
    with open(__file__,"r") as file:
        try:
            fcntl.flock(file.fileno(), fcntl.LOCK_EX|fcntl.LOCK_NB)
            for i in range(CONNECTION_COUNT):
                cn=psycopg2.connect("host=localhost port=5432 dbname=work user=postgres password=root")
                setConnectionApplicationNameToIdle(cn)
                CQ.put(cn, True)
            run()
        except BlockingIOError as e:
            None