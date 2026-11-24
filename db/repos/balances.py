from config import TABLES

BALANCES_TABLE = TABLES["balance_snapshots"]


def balance_details(balance):
    return (
        balance["run_id"],
        balance["account_pk"],
        balance["current"],
        balance["available"],
        balance["credit_limit"],
        balance["iso_currency_code"],
        balance["snapshot_at"].astimezone(),
    )


def insert_balance(cur, balance):
    sql = f"""
    insert into {BALANCES_TABLE} (
        run_id, account_pk, current, available, credit_limit,
        iso_currency_code, snapshot_at
    ) values (
        %s, %s, %s, %s, %s, %s, %s
    ) on conflict (run_id, account_pk) do nothing
    """
    cur.execute(
        sql,
        balance_details(balance),
    )


def write_balance_rows(conn, processed_balances):
    skipped_duplicates = 0
    with conn.cursor() as cur:
        for balance in processed_balances.toLocalIterator():
            insert_balance(cur, balance)
            if cur.rowcount == 0:
                skipped_duplicates += 1

    return skipped_duplicates
