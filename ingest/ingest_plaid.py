from db.db import db_conn
from plaid_src.client import get_plaid_client

from db.repos.runs import create_run, finish_run
from db.repos.items import list_items_for_balances, list_items_for_transactions, get_access_token
from db.repos.accounts import upsert_account
from db.repos.plaid_raw import insert_plaid_balances_raw, insert_plaid_transactions_raw
from db.repos.cursors import get_transactions_cursor, set_transactions_cursor
from config import PLAID_ENV


def to_plain(obj):
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "to_dict") and callable(getattr(obj, "to_dict")):
        return obj.to_dict()
    return obj


def ingest_balances_for_item(conn, client, run_id, plaid_item_pk, label, env):
    access_token = get_access_token(conn, plaid_item_pk)
    if not access_token:
        raise RuntimeError(f"Missing access token for plaid_item_pk={plaid_item_pk} label={label}")
    response = to_plain(client.accounts_get({"access_token": access_token})) or {}
    accounts = response.get("accounts", []) or []
    for account_obj in accounts:
        account = to_plain(account_obj) or {}
        account_id = account.get("account_id")
        if not account_id:
            continue
        bal = to_plain(account.get("balances")) or {}
        upsert_account(
            conn=conn,
            plaid_item_pk=plaid_item_pk,
            account_id=account_id,
            name=account.get("name"),
            official_name=account.get("official_name"),
            account_type=account.get("type"),
            subtype=account.get("subtype"),
            mask=account.get("mask"),
            iso_currency_code=bal.get("iso_currency_code"),
            raw=account,
        )
    insert_plaid_balances_raw(conn, run_id, plaid_item_pk, label, env, response)


def ingest_balances(conn, client, run_id, env):
    items = list_items_for_balances(conn, env_override=env)
    for plaid_item_pk, label in items:
        ingest_balances_for_item(conn, client, run_id, plaid_item_pk, label, env)


def ingest_transactions_sync(conn, client, run_id, plaid_item_pk, label, env):
    access_token = get_access_token(conn, plaid_item_pk)
    if not access_token:
        raise RuntimeError(f"Missing access token for plaid_item_pk={plaid_item_pk} label={label}")
    cursor = get_transactions_cursor(conn, plaid_item_pk)
    has_more = True
    page_index = 0
    next_cursor_value = cursor
    while has_more:
        req = {"access_token": access_token}
        if next_cursor_value:
            req["cursor"] = next_cursor_value
        resp = to_plain(client.transactions_sync(req)) or {}
        insert_plaid_transactions_raw(
            conn, run_id, plaid_item_pk, label, env, page_index, resp
        )
        page_index += 1
        next_cursor_value = resp.get("next_cursor")
        has_more = bool(resp.get("has_more", False))
    set_transactions_cursor(conn, plaid_item_pk, next_cursor_value)


def ingest_transactions(conn, client, run_id, env):
    items = list_items_for_transactions(conn, env_override=env)
    for plaid_item_pk, label in items:
        ingest_transactions_sync(conn, client, run_id, plaid_item_pk, label, env)


def run_ingest(env=None):
    env_value = env or PLAID_ENV
    client = get_plaid_client()
    with db_conn() as conn:
        run_id = create_run(conn, run_type="daily_sync", env=env_value)
    try:
        with db_conn() as conn:
            ingest_balances(conn, client, run_id, env_value)
            ingest_transactions(conn, client, run_id, env_value)
    except Exception as e:
        with db_conn() as conn:
            finish_run(conn, run_id, status="failed", error=str(e))
        raise
    with db_conn() as conn:
        finish_run(conn, run_id, status="success", error=None)
    return run_id




def main():
    run_ingest()


if __name__ == "__main__":
    main()