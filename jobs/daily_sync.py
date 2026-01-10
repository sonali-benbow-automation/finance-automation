from config import NOTIFICATIONS_ENABLED
from ingest.ingest_plaid import main as run_ingest
from notify.send_email import send_daily_digest_email


def main():
    run_ingest()
    if NOTIFICATIONS_ENABLED:
        send_daily_digest_email()


if __name__ == "__main__":
    main()