from db.db import db_conn
from db.repos.notifications import list_retryable_run_ids
from notify.send_email import send_daily_digest_email


def main():
    with db_conn() as conn:
        run_ids = list_retryable_run_ids(conn, channel="email", limit=25)
    for run_id in run_ids:
        send_daily_digest_email(run_id=run_id)


if __name__ == "__main__":
    main()