from db.db import db_conn
from plaid_src.client import get_plaid_client

from db.repos.runs import create_run, finish_run
from db.repos.items import list_items_for_balances
from db.repos.items import list_items_for_transactions
from db.repos.accounts import upsert_account, get_included_account_ids
from db.repos.balances import upsert_balance_snapshot
from db.repos.cursors import get_transactions_cursor, set_transactions_cursor
from db.repos.transactions import upsert_transaction, mark_transaction_removed


def ingest_balances(conn, client, run_id):
    items = list_items_for_balances(conn)
    for label, access_token in items:
        response = client.accounts_balance_get({"access_token": access_token})
        for account in response["accounts"]:
            bal = account.get("balances") or {}
            upsert_account(
                conn=conn,
                label=label,
                account_id=account["account_id"],
                name=account.get("name"),
                official_name=account.get("official_name"),
                account_type=account.get("type"),
                subtype=account.get("subtype"),
                mask=account.get("mask"),
                iso_currency_code=bal.get("iso_currency_code"),
                active=True,
            )
        included = get_included_account_ids(conn, label)
        for account in response["accounts"]:
            if account["account_id"] not in included:
                continue
            bal = account.get("balances") or {}
            upsert_balance_snapshot(
                conn=conn,
                run_id=run_id,
                label=label,
                account_id=account["account_id"],
                account_name=account.get("name"),
                account_type=account.get("type"),
                account_subtype=account.get("subtype"),
                mask=account.get("mask"),
                current=bal.get("current"),
                available=bal.get("available"),
                credit_limit=bal.get("limit"),
                iso_currency_code=bal.get("iso_currency_code"),
            )

def ingest_transactions_sync(conn, client, run_id, label, access_token):
    included = get_included_account_ids(conn, label)
    cursor = get_transactions_cursor(conn, label)
    has_more = True
    while has_more:
        req = {"access_token": access_token}
        if cursor:
            req["cursor"] = cursor
        resp = client.transactions_sync(req)
        for tx in resp.get("added", []):
            if tx["account_id"] not in included:
                continue
            upsert_transaction(conn, run_id, label, tx, sync_status="added")
        for tx in resp.get("modified", []):
            if tx["account_id"] not in included:
                continue
            upsert_transaction(conn, run_id, label, tx, sync_status="modified")
        for r in resp.get("removed", []):
            mark_transaction_removed(conn, run_id, r["transaction_id"])
        cursor = resp["next_cursor"]
        set_transactions_cursor(conn, label, cursor)
        has_more = resp.get("has_more", False)

def ingest_transactions(conn, client, run_id):
    items = list_items_for_transactions(conn)
    for label, access_token in items:
        ingest_transactions_sync(conn, client, run_id, label, access_token)

def main():
    client = get_plaid_client()
    with db_conn() as conn:
        run_id = create_run(conn, run_type="balances")
        try:
            ingest_balances(conn, client, run_id)
            ingest_transactions(conn, client, run_id)
            finish_run(conn, run_id, status="success")
        except Exception as e:
            finish_run(conn, run_id, status="failed", error=str(e))
            raise


if __name__ == "__main__":
    main()