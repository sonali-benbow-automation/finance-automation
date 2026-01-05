from config import TABLES

CURSORS_TABLE = TABLES["cursors"]

def get_transactions_cursor(conn, label):
    sql = f"select transactions_cursor from {CURSORS_TABLE} where label = %s;"
    with conn.cursor() as cur:
        cur.execute(sql, (label,))
        row = cur.fetchone()
        return row[0] if row else None

def set_transactions_cursor(conn, label, cursor_value):
    sql = f"""
    insert into {CURSORS_TABLE} (label, transactions_cursor, updated_at)
    values (%s, %s, now())
    on conflict (label) do update set
      transactions_cursor = excluded.transactions_cursor,
      updated_at = now();
    """
    with conn.cursor() as cur:
        cur.execute(sql, (label, cursor_value))