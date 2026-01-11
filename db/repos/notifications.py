from config import TABLES

NOTIFICATIONS_TABLE = TABLES["notifications"]

def upsert_notification(
    conn,
    run_id,
    channel,
    status,
    message=None,
    error=None,
):
    sql = f"""
    insert into {NOTIFICATIONS_TABLE}
      (run_id, channel, status, message, error, created_at)
    values
      (%s, %s, %s, %s, %s, now())
    on conflict (run_id, channel) do update set
      status = excluded.status,
      message = excluded.message,
      error = excluded.error,
      created_at = now();
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, channel, status, message, error))
