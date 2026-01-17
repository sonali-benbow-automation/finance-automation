from ingest.ingest_plaid import run_ingest
from notify.send_email import send_daily_digest_email


def main():
    run_id = run_ingest()
    send_daily_digest_email(run_id=run_id)


if __name__ == "__main__":
    main()
