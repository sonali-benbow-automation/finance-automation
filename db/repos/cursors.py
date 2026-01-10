from config import TABLES

CURSORS_TABLE = TABLES["cursors"]


def get_transactions_cursor(conn, plaid_item_pk):
    sql = f"select transactions_cursor from {CURSORS_TABLE} where plaid_item_pk = %s;"
    with conn.cursor() as cur:
        cur.execute(sql, (plaid_item_pk,))
        row = cur.fetchone()
        return row[0] if row else None


def set_transactions_cursor(conn, plaid_item_pk, cursor_value):
    sql = f"""
    insert into {CURSORS_TABLE} (plaid_item_pk, transactions_cursor, updated_at)
    values (%s, %s, now())
    on conflict (plaid_item_pk) do update set
      transactions_cursor = excluded.transactions_cursor,
      updated_at = now();
    """
    with conn.cursor() as cur:
        cur.execute(sql, (plaid_item_pk, cursor_value))