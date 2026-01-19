from psycopg.types.json import Jsonb
from config import TABLES

PLAID_WEBHOOK_EVENTS_TABLE = TABLES["plaid_webhook_events"]


def insert_event(conn, payload):
    sql = f"""
    insert into {PLAID_WEBHOOK_EVENTS_TABLE}
      (webhook_type, webhook_code, link_session_id, link_token, status, environment, raw)
    values
      (%s, %s, %s, %s, %s, %s, %s)
    returning id;
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                payload.get("webhook_type"),
                payload.get("webhook_code"),
                payload.get("link_session_id"),
                payload.get("link_token"),
                payload.get("status"),
                payload.get("environment"),
                Jsonb(payload),
            ),
        )
        return cur.fetchone()[0]