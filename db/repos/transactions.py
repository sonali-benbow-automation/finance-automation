from config import TABLES

TRANSACTIONS_TABLE = TABLES["transactions"]


def upsert_transaction(conn, run_id, account_pk, tx, sync_status):
    sql = f"""
    insert into {TRANSACTIONS_TABLE}
      (account_pk, transaction_id,
       name, merchant_name, amount, iso_currency_code, date,
       pending, pending_transaction_id,
       category_id, category, personal_finance_category,
       payment_channel, transaction_type,
       authorized_date, datetime, authorized_datetime,
       sync_status, removed, removed_at,
       first_seen_run_id, last_seen_run_id,
       raw, updated_at)
    values
      (%s, %s,
       %s, %s, %s, %s, %s,
       %s, %s,
       %s, %s, %s,
       %s, %s,
       %s, %s, %s,
       %s, false, null,
       %s, %s,
       %s, now())
    on conflict (transaction_id) do update set
      account_pk = excluded.account_pk,
      name = excluded.name,
      merchant_name = excluded.merchant_name,
      amount = excluded.amount,
      iso_currency_code = excluded.iso_currency_code,
      date = excluded.date,
      pending = excluded.pending,
      pending_transaction_id = excluded.pending_transaction_id,
      category_id = excluded.category_id,
      category = excluded.category,
      personal_finance_category = excluded.personal_finance_category,
      payment_channel = excluded.payment_channel,
      transaction_type = excluded.transaction_type,
      authorized_date = excluded.authorized_date,
      datetime = excluded.datetime,
      authorized_datetime = excluded.authorized_datetime,
      sync_status = excluded.sync_status,
      removed = false,
      removed_at = null,
      last_seen_run_id = excluded.last_seen_run_id,
      raw = excluded.raw,
      updated_at = now();
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                account_pk,
                tx["transaction_id"],
                tx.get("name"),
                tx.get("merchant_name"),
                tx.get("amount"),
                tx.get("iso_currency_code"),
                tx.get("date"),
                tx.get("pending", False),
                tx.get("pending_transaction_id"),
                tx.get("category_id"),
                ", ".join(tx.get("category", [])) if isinstance(tx.get("category"), list) else tx.get("category"),
                tx.get("personal_finance_category"),
                tx.get("payment_channel"),
                tx.get("transaction_type"),
                tx.get("authorized_date"),
                tx.get("datetime"),
                tx.get("authorized_datetime"),
                sync_status,
                run_id,
                run_id,
                tx,
            ),
        )


def mark_transaction_removed(conn, run_id, transaction_id):
    sql = f"""
    update {TRANSACTIONS_TABLE}
    set removed = true,
        removed_at = now(),
        sync_status = 'removed',
        last_seen_run_id = %s,
        updated_at = now()
    where transaction_id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, transaction_id))
        return cur.rowcount