from config import NOTIFICATIONS_ENABLED
from ingest.ingest_plaid import main as run_ingest
from notify.send_sms import send_summary_text


def main():
    run_ingest()
    if NOTIFICATIONS_ENABLED:
        send_summary_text()


if __name__ == "__main__":
    main()