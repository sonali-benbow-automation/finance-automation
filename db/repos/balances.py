from config import TABLES

BALANCE_SNAPSHOTS_TABLE = TABLES["balance_snapshots"]

def _to_text(v):
    return None if v is None else str(v)

def upsert_balance_snapshot(
    conn,
    run_id,
    label,
    account_id,
    account_name=None,
    account_type=None,
    account_subtype=None,
    mask=None,
    current=None,
    available=None,
    credit_limit=None,
    iso_currency_code=None,
):
    sql = f"""
    insert into {BALANCE_SNAPSHOTS_TABLE}
      (run_id, label, account_id, account_name, account_type, account_subtype, mask,
       current, available, credit_limit, iso_currency_code, snapshot_at)
    values
      (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
    on conflict (run_id, account_id) do update set
      label = excluded.label,
      account_name = excluded.account_name,
      account_type = excluded.account_type,
      account_subtype = excluded.account_subtype,
      mask = excluded.mask,
      current = excluded.current,
      available = excluded.available,
      credit_limit = excluded.credit_limit,
      iso_currency_code = excluded.iso_currency_code,
      snapshot_at = excluded.snapshot_at;
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                run_id,
                label,
                account_id,
                account_name,
                _to_text(account_type),
                _to_text(account_subtype),
                mask,
                current,
                available,
                credit_limit,
                iso_currency_code,
            ),
        )