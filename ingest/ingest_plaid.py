from db.db import db_conn
from plaid_src.client import get_plaid_client

from db.repos.runs import create_run, finish_run
from db.repos.items import list_items_for_balances, list_items_for_transactions
from db.repos.accounts import upsert_account, get_included_accounts
from db.repos.balances import upsert_balance_snapshot
from db.repos.cursors import get_transactions_cursor, set_transactions_cursor
from db.repos.transactions import upsert_transaction, mark_transaction_removed


def ingest_balances_for_item(conn, client, run_id, plaid_item_pk, label, access_token):
    response = client.accounts_balance_get({"access_token": access_token})
    for account in response.get("accounts", []):
        bal = account.get("balances") or {}
        upsert_account(
            conn=conn,
            plaid_item_pk=plaid_item_pk,
            account_id=account["account_id"],
            name=account.get("name"),
            official_name=account.get("official_name"),
            account_type=account.get("type"),
            subtype=account.get("subtype"),
            mask=account.get("mask"),
            iso_currency_code=bal.get("iso_currency_code"),
            raw=account,
        )
    included = get_included_accounts(conn, plaid_item_pk)
    for account in response.get("accounts", []):
        account_pk = included.get(account["account_id"])
        if not account_pk:
            continue
        bal = account.get("balances") or {}
        upsert_balance_snapshot(
            conn=conn,
            run_id=run_id,
            account_pk=account_pk,
            current=bal.get("current"),
            available=bal.get("available"),
            credit_limit=bal.get("limit"),
            iso_currency_code=bal.get("iso_currency_code"),
            raw=bal,
        )


def ingest_balances(conn, client, run_id):
    items = list_items_for_balances(conn)
    for plaid_item_pk, label, access_token in items:
        ingest_balances_for_item(conn, client, run_id, plaid_item_pk, label, access_token)


def ingest_transactions_sync(conn, client, run_id, plaid_item_pk, label, access_token):
    included = get_included_accounts(conn, plaid_item_pk)
    cursor = get_transactions_cursor(conn, plaid_item_pk)
    has_more = True
    while has_more:
        req = {"access_token": access_token}
        if cursor:
            req["cursor"] = cursor
        resp = client.transactions_sync(req)
        for tx in resp.get("added", []):
            account_pk = included.get(tx.get("account_id"))
            if not account_pk:
                continue
            upsert_transaction(conn, run_id, account_pk, tx, sync_status="added")
        for tx in resp.get("modified", []):
            account_pk = included.get(tx.get("account_id"))
            if not account_pk:
                continue
            upsert_transaction(conn, run_id, account_pk, tx, sync_status="modified")
        for removed in resp.get("removed", []):
            mark_transaction_removed(conn, run_id, removed["transaction_id"])
        cursor = resp["next_cursor"]
        set_transactions_cursor(conn, plaid_item_pk, cursor)
        has_more = resp.get("has_more", False)


def ingest_transactions(conn, client, run_id):
    items = list_items_for_transactions(conn)
    for plaid_item_pk, label, access_token in items:
        ingest_transactions_sync(conn, client, run_id, plaid_item_pk, label, access_token)


def run_ingest():
    client = get_plaid_client()
    with db_conn() as conn:
        run_id = create_run(conn, run_type="daily_sync")
        try:
            ingest_balances(conn, client, run_id)
            ingest_transactions(conn, client, run_id)
            finish_run(conn, run_id, status="success")
            return run_id
        except Exception as e:
            finish_run(conn, run_id, status="failed", error=str(e))
            raise


def main():
    run_ingest()


if __name__ == "__main__":
    main()