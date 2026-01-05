from config import TABLES

ACCOUNTS_TABLE = TABLES["accounts"]

def _to_text(v):
    return None if v is None else str(v)

def upsert_account(
    conn,
    label,
    account_id,
    name=None,
    official_name=None,
    account_type=None,
    subtype=None,
    mask=None,
    iso_currency_code=None,
    include_in_app=True,
    active=True,
):
    sql = f"""
    insert into {ACCOUNTS_TABLE}
      (label, account_id, name, official_name, type, subtype, mask, iso_currency_code,
       include_in_app, active, updated_at)
    values
      (%s, %s, %s, %s, %s, %s, %s, %s,
       %s, %s, now())
    on conflict (label, account_id) do update set
      name = excluded.name,
      official_name = excluded.official_name,
      type = excluded.type,
      subtype = excluded.subtype,
      mask = excluded.mask,
      iso_currency_code = excluded.iso_currency_code,
      include_in_app = excluded.include_in_app,
      active = excluded.active,
      updated_at = now();
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                label,
                account_id,
                name,
                official_name,
                _to_text(account_type),
                _to_text(subtype),
                mask,
                iso_currency_code,
                include_in_app,
                active,
            ),
        )

def get_included_account_ids(conn, label):
    sql = f"""
    select account_id
    from {ACCOUNTS_TABLE}
    where label = %s
      and include_in_app = true
      and active = true;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (label,))
        return {r[0] for r in cur.fetchall()}