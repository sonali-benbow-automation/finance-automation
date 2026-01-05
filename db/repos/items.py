from config import TABLES

TABLE_NAME = TABLES["plaid_items"]

def item_exists(conn, label):
    sql = f"select 1 from {TABLE_NAME} where label = %s limit 1;"
    with conn.cursor() as cur:
        cur.execute(sql, (label,))
        return cur.fetchone() is not None

def upsert_item(
    conn,
    label,
    institution_name,
    institution_id,
    item_id,
    access_token,
    transactions_enabled,
    balances_enabled,
):
    sql = f"""
    insert into {TABLE_NAME}
      (label, institution_name, institution_id, item_id, access_token, transactions_enabled, balances_enabled)
    values
      (%s, %s, %s, %s, %s, %s, %s)
    on conflict (label) do update set
      institution_name = excluded.institution_name,
      institution_id = excluded.institution_id,
      item_id = excluded.item_id,
      access_token = excluded.access_token,
      transactions_enabled = excluded.transactions_enabled,
      balances_enabled = excluded.balances_enabled;
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                label,
                institution_name,
                institution_id,
                item_id,
                access_token,
                transactions_enabled,
                balances_enabled,
            ),
        )

def list_items_for_balances(conn):
    sql = f"""
    select label, access_token
    from {TABLE_NAME}
    where balances_enabled = true;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()

def list_items_for_transactions(conn):
    sql = f"""
    select label, access_token
    from {TABLE_NAME}
    where transactions_enabled = true;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()