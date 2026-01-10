from config import TABLES, PLAID_ENV

RUNS_TABLE = TABLES["runs"]


def create_run(conn, run_type, env=None):
    env_value = env or PLAID_ENV
    sql = f"""
    insert into {RUNS_TABLE} (run_type, env, status)
    values (%s, %s, 'running')
    returning id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_type, env_value))
        return cur.fetchone()[0]


def finish_run(conn, run_id, status, error=None):
    sql = f"""
    update {RUNS_TABLE}
    set status = %s,
        error = %s,
        finished_at = now()
    where id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (status, error, run_id))