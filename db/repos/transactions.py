from config import TABLES

TRANSACTIONS_TABLE = TABLES["transactions"]

def upsert_transaction(conn, run_id, label, tx, sync_status):
    sql = f"""
    insert into {TRANSACTIONS_TABLE}
      (label, transaction_id, account_id,
       name, merchant_name, amount, iso_currency_code, date,
       pending, pending_transaction_id,
       sync_status, removed, removed_at,
       first_seen_run_id, last_seen_run_id, updated_at)
    values
      (%s, %s, %s,
       %s, %s, %s, %s, %s,
       %s, %s,
       %s, false, null,
       %s, %s, now())
    on conflict (transaction_id) do update set
      label = excluded.label,
      account_id = excluded.account_id,
      name = excluded.name,
      merchant_name = excluded.merchant_name,
      amount = excluded.amount,
      iso_currency_code = excluded.iso_currency_code,
      date = excluded.date,
      pending = excluded.pending,
      pending_transaction_id = excluded.pending_transaction_id,
      sync_status = excluded.sync_status,
      removed = false,
      removed_at = null,
      last_seen_run_id = excluded.last_seen_run_id,
      updated_at = now();
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                label,
                tx["transaction_id"],
                tx["account_id"],
                tx.get("name"),
                tx.get("merchant_name"),
                tx.get("amount"),
                tx.get("iso_currency_code"),
                tx.get("date"),
                tx.get("pending"),
                tx.get("pending_transaction_id"),
                sync_status,
                run_id,
                run_id,
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