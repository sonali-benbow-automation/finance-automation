from psycopg.types.json import Json
from config import TABLES

ACCOUNTS_TABLE = TABLES["accounts"]


def to_text(v):
    return None if v is None else str(v)


def to_json(v):
    if v is None:
        return None
    return Json(v)


def upsert_account(
    conn,
    plaid_item_pk,
    account_id,
    name=None,
    official_name=None,
    account_type=None,
    subtype=None,
    mask=None,
    iso_currency_code=None,
    include_in_app=None,
    active=None,
    raw=None,
):
    sql = f"""
    insert into {ACCOUNTS_TABLE}
      (plaid_item_pk, account_id, name, official_name, type, subtype, mask, iso_currency_code,
       include_in_app, active, raw, updated_at)
    values
      (%s, %s, %s, %s, %s, %s, %s, %s,
       coalesce(%s, true), coalesce(%s, true), %s, now())
    on conflict (plaid_item_pk, account_id) do update set
      name = excluded.name,
      official_name = excluded.official_name,
      type = excluded.type,
      subtype = excluded.subtype,
      mask = excluded.mask,
      iso_currency_code = excluded.iso_currency_code,
      raw = excluded.raw,
      include_in_app = coalesce(%s, {ACCOUNTS_TABLE}.include_in_app),
      active = coalesce(%s, {ACCOUNTS_TABLE}.active),
      updated_at = now()
    returning id;
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                plaid_item_pk,
                account_id,
                name,
                official_name,
                to_text(account_type),
                to_text(subtype),
                mask,
                iso_currency_code,
                include_in_app,
                active,
                to_json(raw),
                include_in_app,
                active,
            ),
        )
        return cur.fetchone()[0]


def get_included_accounts(conn, plaid_item_pk):
    sql = f"""
    select id, account_id
    from {ACCOUNTS_TABLE}
    where plaid_item_pk = %s
      and include_in_app = true
      and active = true;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (plaid_item_pk,))
        return {account_id: account_pk for (account_pk, account_id) in cur.fetchall()}
