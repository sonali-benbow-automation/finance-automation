from string import Template
import psycopg
from config import DATABASE_URL, TABLES


def main():
    with open("db/schema.sql", "r") as f:
        raw_sql = f.read()
    sql = Template(raw_sql).safe_substitute({
        "PLAID_ITEMS_TABLE": TABLES["plaid_items"],
        "BALANCE_SNAPSHOTS_TABLE": TABLES["balance_snapshots"],
        "RUNS_TABLE": TABLES["runs"],
        "ACCOUNTS_TABLE": TABLES["accounts"],
        "CURSORS_TABLE": TABLES["cursors"],
        "TRANSACTIONS_TABLE": TABLES["transactions"],
        "NOTIFICATIONS_TABLE": TABLES["notifications"],
    })
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    print("Schema applied successfully.")


if __name__ == "__main__":
    main()
