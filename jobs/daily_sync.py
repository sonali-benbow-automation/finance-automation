import logging
from ingest.ingest_plaid import run_ingest
from notify.send_email import send_daily_digest_email
from jobs.process_transactions import apply_plaid_transaction_changes
from jobs.process_balances import apply_plaid_balance_snapshots
from jobs.process_accounts import apply_plaid_accounts
from app_logging import log_stage, run_command

logger = logging.getLogger(__name__)



def main():
    with log_stage(logger, "daily_sync") as fields:
        run_id = run_ingest()
        fields["run_id"] = run_id
        apply_plaid_accounts(run_id)
        apply_plaid_transaction_changes(run_id)
        apply_plaid_balance_snapshots(run_id)
        send_daily_digest_email(run_id=run_id)


if __name__ == "__main__":
    run_command(main)
