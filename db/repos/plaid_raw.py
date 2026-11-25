from functools import partial
from json import dumps

from psycopg.types.json import Jsonb

from config import TABLES

PLAID_BALANCES_RAW_TABLE = TABLES["plaid_balances_raw"]
PLAID_TRANSACTIONS_RAW_TABLE = TABLES["plaid_transactions_raw"]

serialize_payload = partial(dumps, default=str)


def insert_plaid_balances_raw(
        conn,
        run_id,
        plaid_item_pk,
        label,
        env,
        payload
):
    sql = f"""insert into {PLAID_BALANCES_RAW_TABLE} (
        run_id, plaid_item_pk, label, env, payload)
        values (%s, %s, %s, %s, %s)"""
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, plaid_item_pk, label, env, Jsonb(payload, dumps=serialize_payload)))

def insert_plaid_transactions_raw(
        conn,
        run_id,
        plaid_item_pk,
        label,
        env,
        page_index,
        payload
):
    sql = f"""insert into {PLAID_TRANSACTIONS_RAW_TABLE} (
        run_id, plaid_item_pk, label, env, page_index, payload)
        values (%s, %s, %s, %s, %s, %s)"""
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, plaid_item_pk, label, env, page_index, Jsonb(payload, dumps=serialize_payload)))
