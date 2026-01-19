from config import TABLES, PLAID_ENV

HOSTED_LINK_SESSIONS_TABLE = TABLES["hosted_link_sessions"]


def create_session(conn, label, link_token, hosted_link_url, webhook_url, env=None):
    env_value = env or PLAID_ENV
    sql = f"""
    insert into {HOSTED_LINK_SESSIONS_TABLE}
      (label, env, link_token, hosted_link_url, webhook_url, status, updated_at)
    values
      (%s, %s, %s, %s, %s, 'created', now())
    returning id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (label, env_value, link_token, hosted_link_url, webhook_url))
        return cur.fetchone()[0]


def get_by_link_token(conn, link_token):
    sql = f"""
    select id, label, env, link_token, hosted_link_url, webhook_url, status, error, created_at, updated_at
    from {HOSTED_LINK_SESSIONS_TABLE}
    where link_token = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (link_token,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


def mark_success(conn, link_token):
    sql = f"""
    update {HOSTED_LINK_SESSIONS_TABLE}
    set status = 'success',
        error = null,
        updated_at = now()
    where link_token = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (link_token,))
        return cur.rowcount


def mark_failed(conn, link_token, error):
    sql = f"""
    update {HOSTED_LINK_SESSIONS_TABLE}
    set status = 'failed',
        error = %s,
        updated_at = now()
    where link_token = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (error, link_token))
        return cur.rowcount