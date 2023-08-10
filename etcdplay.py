import json

import requests
import base64
import pprint
import logging


def etcd_url():
    return "http://localhost:23790/v3"

def ldg_prefix():
    return "/ldg/"

def ldg_range_end():
    return "/ldg1"

reply=None
try:
    reply = requests.post(etcd_url()+"/kv/range", json={
        "key": base64.standard_b64encode(ldg_prefix().encode()).decode(),
        "range_end": base64.b64encode(ldg_range_end().encode("ascii")).decode(),
        "sort_order": "DESCEND",
        "limit":"1"
    })
    if reply.json().get("kvs")!=None and len(reply.json().get("kvs"))>0:
        pprint.pprint(reply.json())
    else:
        logging.error("Got empty kvs field in etcd reply, assuming height=0")
except (requests.exceptions.RequestException, json.decoder.JSONDecodeError) as ex:
    logging.warning("Connection error:"+str(ex))


