from config import TABLES

TRANSACTIONS_TABLE = TABLES["transactions"]


def transaction_details(transaction):
    transaction_datetime = transaction["datetime"]
    authorized_datetime = transaction["authorized_datetime"]
    return (
        transaction["account_pk"],
        transaction["amount"],
        transaction["iso_currency_code"],
        transaction["date"],
        transaction["pending"],
        transaction["pending_transaction_id"],
        transaction["name"],
        transaction["merchant_name"],
        transaction["category_id"],
        transaction["category"],
        transaction["personal_finance_category"],
        transaction["payment_channel"],
        transaction["transaction_type"],
        transaction["authorized_date"],
        transaction_datetime.astimezone() if transaction_datetime is not None else None,
        authorized_datetime.astimezone() if authorized_datetime is not None else None,
    )


def insert_transaction(cur, transaction):
    sql = f"""
    insert into {TRANSACTIONS_TABLE} (
        account_pk, amount, iso_currency_code, date, pending,
        pending_transaction_id, name, merchant_name, category_id, category,
        personal_finance_category, payment_channel, transaction_type,
        authorized_date, datetime, authorized_datetime,
        transaction_id, sync_status, first_seen_run_id, last_seen_run_id
    ) values (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s::jsonb, %s, %s, %s, %s, %s, %s, 'added', %s, %s
    ) on conflict (transaction_id) do nothing
    """
    cur.execute(
        sql,
        transaction_details(transaction) + (
            transaction["transaction_id"],
            transaction["first_seen_run_id"],
            transaction["run_id"],
        ),
    )


def update_transaction(cur, transaction):
    sql = f"""
    update {TRANSACTIONS_TABLE}
    set account_pk = %s,
        amount = %s,
        iso_currency_code = %s,
        date = %s,
        pending = %s,
        pending_transaction_id = %s,
        name = %s,
        merchant_name = %s,
        category_id = %s,
        category = %s,
        personal_finance_category = %s::jsonb,
        payment_channel = %s,
        transaction_type = %s,
        authorized_date = %s,
        datetime = %s,
        authorized_datetime = %s,
        sync_status = 'modified',
        last_seen_run_id = %s,
        updated_at = now()
    where transaction_id = %s
    """
    cur.execute(
        sql,
        transaction_details(transaction) + (
            transaction["run_id"],
            transaction["transaction_id"],
        ),
    )


def mark_transaction_removed(cur, transaction):
    sql = f"""
    update {TRANSACTIONS_TABLE}
    set sync_status = 'removed',
        removed = true,
        removed_at = coalesce(removed_at, now()),
        last_seen_run_id = %s,
        updated_at = now()
    where transaction_id = %s
    """
    cur.execute(sql, (transaction["run_id"], transaction["transaction_id"]))


def write_transactions_rows(conn, processed_transactions):
    unmatched_changes = []
    with conn.cursor() as cur:
        for transaction in processed_transactions.toLocalIterator():
            status = transaction["sync_status"]
            if status == "added":
                insert_transaction(cur, transaction)
                continue
            if status == "modified":
                update_transaction(cur, transaction)
            elif status == "removed":
                mark_transaction_removed(cur, transaction)
            else:
                raise ValueError(f"Unsupported transaction sync status: {status}")

            if cur.rowcount == 0:
                unmatched_changes.append({
                    "transaction_id": transaction["transaction_id"],
                    "run_id": transaction["run_id"],
                    "sync_status": status,
                })

    return unmatched_changes
