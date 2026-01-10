from config import TABLES

BALANCE_SNAPSHOTS_TABLE = TABLES["balance_snapshots"]


def upsert_balance_snapshot(
    conn,
    run_id,
    account_pk,
    current=None,
    available=None,
    credit_limit=None,
    iso_currency_code=None,
    snapshot_at=None,
    raw=None,
):
    if snapshot_at is None:
        sql = f"""
        insert into {BALANCE_SNAPSHOTS_TABLE}
          (run_id, account_pk, current, available, credit_limit, iso_currency_code, snapshot_at, raw)
        values
          (%s, %s, %s, %s, %s, %s, now(), %s)
        on conflict (run_id, account_pk) do update set
          current = excluded.current,
          available = excluded.available,
          credit_limit = excluded.credit_limit,
          iso_currency_code = excluded.iso_currency_code,
          snapshot_at = excluded.snapshot_at,
          raw = excluded.raw;
        """
        params = (run_id, account_pk, current, available, credit_limit, iso_currency_code, raw)
    else:
        sql = f"""
        insert into {BALANCE_SNAPSHOTS_TABLE}
          (run_id, account_pk, current, available, credit_limit, iso_currency_code, snapshot_at, raw)
        values
          (%s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (run_id, account_pk) do update set
          current = excluded.current,
          available = excluded.available,
          credit_limit = excluded.credit_limit,
          iso_currency_code = excluded.iso_currency_code,
          snapshot_at = excluded.snapshot_at,
          raw = excluded.raw;
        """
        params = (run_id, account_pk, current, available, credit_limit, iso_currency_code, snapshot_at, raw)
    with conn.cursor() as cur:
        cur.execute(sql, params)