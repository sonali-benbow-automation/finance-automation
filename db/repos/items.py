from config import TABLES, PLAID_ENV, PLAID_TOKEN_KEY

PLAID_ITEMS_TABLE = TABLES["plaid_items"]


def resolve_env(env_override):
    return env_override if env_override is not None else PLAID_ENV


def item_exists(conn, label, env_override=None, active_only=True):
    env_value = resolve_env(env_override)
    active_clause = "and active = true" if active_only else ""
    sql = f"""
    select 1
    from {PLAID_ITEMS_TABLE}
    where env = %s
      and label = %s
      {active_clause}
    limit 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (env_value, label))
        return cur.fetchone() is not None


def get_plaid_item_pk(conn, label, env_override=None, active_only=True):
    env_value = resolve_env(env_override)
    active_clause = "and active = true" if active_only else ""
    sql = f"""
    select id
    from {PLAID_ITEMS_TABLE}
    where env = %s
      and label = %s
      {active_clause}
    order by id desc
    limit 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (env_value, label))
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
      access_token_enc,
      access_token_kid,
      transactions_enabled,
      balances_enabled,
      active,
      archived_at,
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


def get_item_id_by_label(conn, label, env_override=None, active_only=True):
    env_value = resolve_env(env_override)
    active_clause = "and active = true" if active_only else ""
    sql = f"""
    select item_id
    from {PLAID_ITEMS_TABLE}
    where env = %s
      and label = %s
      {active_clause}
    order by id desc
    limit 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (env_value, label))
        row = cur.fetchone()
        return row[0] if row else None


def deactivate_label(conn, label, env_override=None):
    env_value = resolve_env(env_override)
    sql = f"""
    update {PLAID_ITEMS_TABLE}
    set active = false,
        archived_at = now(),
        updated_at = now()
    where env = %s
      and label = %s
      and active = true;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (env_value, label))
        return cur.rowcount


def upsert_item(
    conn,
    label,
    institution_name,
    institution_id,
    item_id,
    access_token_plaintext,
    transactions_enabled,
    balances_enabled,
    env_override=None,
):
    env_value = resolve_env(env_override)
    existing_item_id = get_item_id_by_label(conn, label, env_override=env_value, active_only=True)
    if existing_item_id and existing_item_id != item_id:
        deactivate_label(conn, label, env_override=env_value)
    sql = f"""
    insert into {PLAID_ITEMS_TABLE}
      (label, env, institution_name, institution_id, item_id,
       access_token_enc, access_token_kid,
       transactions_enabled, balances_enabled,
       active, archived_at, updated_at)
    values
      (%s, %s, %s, %s, %s,
       pgp_sym_encrypt(%s::text, %s::text), 'v1',
       %s, %s,
       true, null, now())
    on conflict (env, label) do update set
      institution_name = excluded.institution_name,
      institution_id = excluded.institution_id,
      item_id = excluded.item_id,
      access_token_enc = excluded.access_token_enc,
      access_token_kid = excluded.access_token_kid,
      transactions_enabled = excluded.transactions_enabled,
      balances_enabled = excluded.balances_enabled,
      active = true,
      archived_at = null,
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
                access_token_plaintext,
                PLAID_TOKEN_KEY,
                transactions_enabled,
                balances_enabled,
            ),
        )
        return cur.fetchone()[0]


def get_access_token(conn, plaid_item_pk):
    sql = f"""
    select pgp_sym_decrypt(access_token_enc, %s::text)::text as access_token
    from {PLAID_ITEMS_TABLE}
    where id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (PLAID_TOKEN_KEY, plaid_item_pk))
        row = cur.fetchone()
        return row[0] if row else None


def list_items_for_balances(conn, env_override=None):
    env_value = resolve_env(env_override)
    sql = f"""
    select id, label
    from {PLAID_ITEMS_TABLE}
    where balances_enabled = true
      and env = %s
      and active = true
    order by id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (env_value,))
        return cur.fetchall()


def list_items_for_transactions(conn, env_override=None):
    env_value = resolve_env(env_override)
    sql = f"""
    select id, label
    from {PLAID_ITEMS_TABLE}
    where transactions_enabled = true
      and env = %s
      and active = true
    order by id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (env_value,))
        return cur.fetchall()