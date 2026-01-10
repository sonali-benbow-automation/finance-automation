from config import TABLES, PLAID_ENV

PLAID_ITEMS_TABLE = TABLES["plaid_items"]


def item_exists(conn, label):
    sql = f"select 1 from {PLAID_ITEMS_TABLE} where label = %s limit 1;"
    with conn.cursor() as cur:
        cur.execute(sql, (label,))
        return cur.fetchone() is not None


def get_plaid_item_pk(conn, label):
    sql = f"select id from {PLAID_ITEMS_TABLE} where label = %s;"
    with conn.cursor() as cur:
        cur.execute(sql, (label,))
        row = cur.fetchone()
        return row[0] if row else None


def get_item(conn, plaid_item_pk):
    sql = f"""
    select
      id,
      label,
      env,
      institution_name,
      institution_id,
      item_id,
      access_token,
      transactions_enabled,
      balances_enabled,
      created_at,
      updated_at
    from {PLAID_ITEMS_TABLE}
    where id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (plaid_item_pk,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [desc[0] for desc in cur.description]
        return dict(zip(cols, row))


def upsert_item(
    conn,
    label,
    institution_name,
    institution_id,
    item_id,
    access_token,
    transactions_enabled,
    balances_enabled,
    env=None,
):
    env_value = env or PLAID_ENV
    sql = f"""
    insert into {PLAID_ITEMS_TABLE}
      (label, env, institution_name, institution_id, item_id, access_token,
       transactions_enabled, balances_enabled, updated_at)
    values
      (%s, %s, %s, %s, %s, %s,
       %s, %s, now())
    on conflict (label) do update set
      env = excluded.env,
      institution_name = excluded.institution_name,
      institution_id = excluded.institution_id,
      item_id = excluded.item_id,
      access_token = excluded.access_token,
      transactions_enabled = excluded.transactions_enabled,
      balances_enabled = excluded.balances_enabled,
      updated_at = now()
    returning id;
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                label,
                env_value,
                institution_name,
                institution_id,
                item_id,
                access_token,
                transactions_enabled,
                balances_enabled,
            ),
        )
        return cur.fetchone()[0]


def list_items_for_balances(conn, env=None):
    env_value = env or PLAID_ENV
    sql = f"""
    select id, label, access_token
    from {PLAID_ITEMS_TABLE}
    where balances_enabled = true
      and env = %s
    order by id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (env_value,))
        return cur.fetchall()


def list_items_for_transactions(conn, env=None):
    env_value = env or PLAID_ENV
    sql = f"""
    select id, label, access_token
    from {PLAID_ITEMS_TABLE}
    where transactions_enabled = true
      and env = %s
    order by id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (env_value,))
        return cur.fetchall()