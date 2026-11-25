import logging
from db.db import db_conn
from db.repos.notifications import list_retryable_run_ids
from notify.send_email import send_daily_digest_email
from app_logging import log_stage, run_command

logger = logging.getLogger(__name__)



def main():
    with log_stage(logger, "retry_notifications") as fields:
        with db_conn() as conn:
            run_ids = list_retryable_run_ids(conn, channel="email", limit=25)
        fields["notifications_selected"] = len(run_ids)
        for run_id in run_ids:
            send_daily_digest_email(run_id=run_id)


if __name__ == "__main__":
    run_command(main)
