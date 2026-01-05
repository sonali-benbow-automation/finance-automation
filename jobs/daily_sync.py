from db.db import db_tx
from ingest.ingest_plaid import ingest_plaid
from notify.send_sms import send_daily_digest
from config import NOTIFICATIONS_ENABLED

def main():
    with db_tx() as conn:
        ingest_plaid(conn)

        if NOTIFICATIONS_ENABLED:
            send_daily_digest(conn)

if __name__ == "__main__":
    main()