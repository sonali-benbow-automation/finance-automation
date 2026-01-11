from config import TABLES

NOTIFICATIONS_TABLE = TABLES["notifications"]


def upsert_notification(conn, run_id, channel, status, message=None, error=None):
    sql = f"""
    insert into {NOTIFICATIONS_TABLE}
      (run_id, channel, status, message, error, created_at)
    values
      (%s, %s, %s, %s, %s, now())
    on conflict (run_id, channel) do update set
      status = excluded.status,
      message = excluded.message,
      error = excluded.error,
      created_at = now()
    returning id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, channel, status, message, error))
        return cur.fetchone()[0]


def list_retryable_run_ids(conn, channel, limit=25):
    sql = f"""
    with latest_per_run as (
      select
        run_id,
        max(created_at) as max_created_at
      from {NOTIFICATIONS_TABLE}
      where channel = %s
      group by run_id
    )
    select n.run_id
    from {NOTIFICATIONS_TABLE} n
    join latest_per_run l
      on l.run_id = n.run_id
     and l.max_created_at = n.created_at
    where n.channel = %s
      and n.status in ('failed')
      and n.run_id is not null
    order by n.run_id asc
    limit %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (channel, channel, limit))
        return [r[0] for r in cur.fetchall()]