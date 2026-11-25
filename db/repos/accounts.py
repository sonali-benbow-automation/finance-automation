from psycopg.types.json import Jsonb
from config import TABLES

ACCOUNTS_TABLE = TABLES["accounts"]


def to_text(v):
    return None if v is None else str(v)


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
                Jsonb(raw) if raw is not None else None,
                include_in_app,
                active,
            ),
        )
        return cur.fetchone()[0]


def write_account_rows(conn, processed_accounts):
    for account in processed_accounts.toLocalIterator():
        if not account["account_id"]:
            continue
        upsert_account(conn, **account.asDict(), raw=None)
