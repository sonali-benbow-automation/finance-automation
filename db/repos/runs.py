from config import TABLES
RUNS_TABLE = TABLES["runs"]

def create_run(conn, run_type):
    sql = f"""
        insert into {RUNS_TABLE} (run_type, status)
        values (%s, 'running')
        returning id;
        """
    with conn.cursor() as cur:
        cur.execute(sql, (run_type,))
        return cur.fetchone()[0]

def finish_run(conn, run_id, status, error=None):
    sql = f""" update {RUNS_TABLE}
    set status = %s,
        error = %s,
        finished_at = now()
    where id = %s;"""
    with conn.cursor() as cur:
        cur.execute(sql, (status, error, run_id))