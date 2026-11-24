from ingest.ingest_plaid import run_ingest
from notify.send_email import send_daily_digest_email
from jobs.process_transactions import apply_plaid_transaction_changes
from jobs.process_balances import apply_plaid_balance_snapshots


def main():
    run_id = run_ingest()
    apply_plaid_transaction_changes(run_id)
    apply_plaid_balance_snapshots(run_id)
    send_daily_digest_email(run_id=run_id)


if __name__ == "__main__":
    main()
