import json
from config import TABLES

PLAID_BALANCES_RAW_TABLE = TABLES["plaid_balances_raw"]
PLAID_TRANSACTIONS_RAW_TABLE = TABLES["plaid_transactions_raw"]

def insert_plaid_balances_raw(
        conn,
        run_id,
        plaid_item_pk,
        label,
        env,
        payload
):
    payload_json = json.dumps(payload, default=str)
    sql = f"""insert into {PLAID_BALANCES_RAW_TABLE} (
        run_id, plaid_item_pk, label, env, payload)
        values (%s, %s, %s, %s, %s::jsonb)"""
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, plaid_item_pk, label, env, payload_json))

def insert_plaid_transactions_raw(
        conn,
        run_id,
        plaid_item_pk,
        label,
        env,
        page_index,
        payload
):
    payload_json = json.dumps(payload, default=str)
    sql = f"""insert into {PLAID_TRANSACTIONS_RAW_TABLE} (
        run_id, plaid_item_pk, label, env, page_index, payload)
        values (%s, %s, %s, %s, %s, %s::jsonb)"""
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, plaid_item_pk, label, env, page_index, payload_json))