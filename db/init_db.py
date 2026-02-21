from string import Template
import psycopg
from config import DATABASE_URL, TABLES


def main():
    with open("db/schema.sql", "r") as f:
        raw_sql = f.read()
    sql = Template(raw_sql).safe_substitute({
        "PLAID_ITEMS_TABLE": TABLES["plaid_items"],
        "RUNS_TABLE": TABLES["runs"],
        "ACCOUNTS_TABLE": TABLES["accounts"],
        "CURSORS_TABLE": TABLES["cursors"],
        "PLAID_BALANCES_RAW_TABLE": TABLES["plaid_balances_raw"],
        "PLAID_TRANSACTIONS_RAW_TABLE": TABLES["plaid_transactions_raw"],
        "NOTIFICATIONS_TABLE": TABLES["notifications"],
        "HOSTED_LINK_SESSIONS_TABLE": TABLES["hosted_link_sessions"],
        "PLAID_WEBHOOK_EVENTS_TABLE": TABLES["plaid_webhook_events"],
        "MERCHANT_RULES_TABLE": TABLES["merchant_rules"],
        "MANUAL_BALANCES_TABLE": TABLES["manual_balances"],
        "MANUAL_BALANCE_HISTORY_TABLE": TABLES["manual_balance_history"],
    })
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    print("Schema applied successfully.")


if __name__ == "__main__":
    main()