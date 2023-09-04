import threading
import concurrent.futures
from enum import Enum

import psycopg2
import psycopg2.extensions
import psycopg2.pool
from contextlib import contextmanager
import sys

import uuid

import sha3
import hashlib

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
    pass


class TxType(Enum):
    ORDERED=1
    SEQ=

def get_tx_type_and_keys(tx):
    action=tx.get("action")

    if action in ["create-doc", "approve-doc","revoke-doc"]:
        return TxType.ORDERED, [hashlib.sha256(tx["doc-uuid"]).digest()]
    if action in ["create-user", "enable-user", "disable-user"]:
        return TxType.ORDERED, [hashlib.sha256(tx["user-uuid"]).digest()]
    elif action in ["money-income"]:
        return TxType.ANY, [hashlib.sha256(tx["usr-uuid"])]
    elif action in ["money-transfer"]:
        return TxType.ORDERED, [hashlib.sha256(tx["debit-usr-uuid"]).digest(), hashlib.sha256(tx["credit-usr-uuid"]).digest()]
    else:
        return TxType.SEQ, []



class ParsedBlock:
    # все транзакции, uuid=>tx
    txs:{}={}
    # транзакции, порядок выполнения которых неважен, например, создание юзеров, начисление денег и проч - просто список uuid-ов транзакций
    any_order_txs:[]=[]
    # транзaкции, которые должны выполняться строго последовательно; но их можно разбить на несвязные секции, которые
    # могут выполняться параллельно; например, списания-начисления для юзера должны выполняться последовательно
    # (в общем-то неважно в каком порядке, главное, чтобы в детерминированном), но т.к. юзера несвязаны, то эти секции для разных юзеров могут быть выполнены
    # параллельно, последовательно или как угодно
    # в данном случае тут словарь, где ключи - хеши от каких-то атрибутов транзакций, а значения = последовательности uuid-ов транзакций
    ordered_seqs:{}={} # dict of lists of uuid of txns
    #для некоторых транзакций по тем или иным причинам нельзя определить ключ секционирования
    unordered_txs:[]=[]

    def __init__(self, payload:[]):
        for tx in payload:
            self.txs[tx.uuid]=tx
            tx_type, keys = get_tx_type_and_keys(tx)

            if tx_type==TxType.ANY:
                self.any_order_txs.append(tx)
            elif tx_type==TxType.SEQ:
                self.unordered_txs.append(tx)

if __name__=="__main__":
    main()
