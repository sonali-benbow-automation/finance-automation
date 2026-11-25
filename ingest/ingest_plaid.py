import logging
from db.db import db_conn
from plaid_src.client import get_plaid_client

from db.repos.runs import create_run, finish_run
from db.repos.items import list_items_for_balances, list_items_for_transactions, get_access_token
from db.repos.plaid_raw import insert_plaid_balances_raw, insert_plaid_transactions_raw
from db.repos.cursors import get_transactions_cursor, set_transactions_cursor
from config import PLAID_ENV
from app_logging import log_stage, run_command

logger = logging.getLogger(__name__)



def to_plain(obj):
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "to_dict") and callable(getattr(obj, "to_dict")):
        return obj.to_dict()
    return obj


def require_access_token(conn, plaid_item_pk, label):
    access_token = get_access_token(conn, plaid_item_pk)
    if not access_token:
        raise RuntimeError(f"Missing access token for plaid_item_pk={plaid_item_pk} label={label}")
    return access_token


def ingest_balances_for_item(conn, client, run_id, plaid_item_pk, label, env):
    access_token = require_access_token(conn, plaid_item_pk, label)
    response = to_plain(client.accounts_get({"access_token": access_token})) or {}
    insert_plaid_balances_raw(conn, run_id, plaid_item_pk, label, env, response)


def ingest_balances(conn, client, run_id, env):
    items = list_items_for_balances(conn, env_override=env)
    for plaid_item_pk, label in items:
        ingest_balances_for_item(conn, client, run_id, plaid_item_pk, label, env)


def ingest_transactions_sync(conn, client, run_id, plaid_item_pk, label, env):
    access_token = require_access_token(conn, plaid_item_pk, label)
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
    with log_stage(logger, "ingestion") as fields:
        env_value = env or PLAID_ENV
        fields["env"] = env_value
        client = get_plaid_client()
        with db_conn() as conn:
            run_id = create_run(conn, run_type="daily_sync", env=env_value)
        fields["run_id"] = run_id
        try:
            with db_conn() as conn:
                with log_stage(logger, "fetch_plaid_balances", run_id=run_id):
                    ingest_balances(conn, client, run_id, env_value)
                with log_stage(logger, "fetch_plaid_transactions", run_id=run_id):
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
    run_command(main)