import psycopg
from contextlib import contextmanager
from config import DATABASE_URL

@contextmanager
def db_conn():
    conn = psycopg.connect(DATABASE_URL, sslmode="require")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()