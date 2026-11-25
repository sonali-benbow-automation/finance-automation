import logging
from string import Template
import psycopg
from config import DATABASE_URL, TABLES
from app_logging import log_stage, run_command

logger = logging.getLogger(__name__)



def main():
    with log_stage(logger, "schema_init"):
        with open("db/schema.sql", "r") as f:
            raw_sql = f.read()
        sql = Template(raw_sql).safe_substitute({
            "PLAID_ITEMS_TABLE": TABLES["plaid_items"],
            "RUNS_TABLE": TABLES["runs"],
            "ACCOUNTS_TABLE": TABLES["accounts"],
            "CURSORS_TABLE": TABLES["cursors"],
            "PLAID_BALANCES_RAW_TABLE": TABLES["plaid_balances_raw"],
            "PLAID_TRANSACTIONS_RAW_TABLE": TABLES["plaid_transactions_raw"],
            "BALANCE_SNAPSHOTS_TABLE": TABLES["balance_snapshots"],
            "TRANSACTIONS_TABLE": TABLES["transactions"],
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


if __name__ == "__main__":
    run_command(main)
