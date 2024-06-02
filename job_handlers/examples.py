import job_handlers

def echo(params:str, context:str):
    prefix = params['prefix'] if params['prefix'] is not None else ''
    cnt = context['cnt'] if context.get('cnt') is not None else 0
    with job_handlers.cns.getconn() as cn, cn.cursor() as cr:
        cr.execute("insert into playground.log(added_at, message)values(clock_timestamp(), %s)", [prefix+":cnt="+str(cnt)])

    context['cnt']=cnt+1
    context['next_run_after_secs']=10
    return context

job_handlers.handlers['example.echo'] = echo