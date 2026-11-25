import logging
from pyspark.sql import functions as F

from config import DATABASE_URL, TABLES
from db.db import db_conn
from db.repos.balances import write_balance_rows
from jobs.spark import create_spark_session, read_postgres_table, match_accounts
from app_logging import log_stage

logger = logging.getLogger(__name__)



ACCOUNT_LIST_SCHEMA = (
    "ARRAY<STRUCT<"
    "account_id: STRING, "
    "balances: STRUCT<"
    "current: DECIMAL(38,10), available: DECIMAL(38,10), "
    "limit: DECIMAL(38,10), iso_currency_code: STRING, "
    "unofficial_currency_code: STRING"
    ">"
    ">>"
)


def extract_balance_details(raw_responses):
    account_list = F.from_json(
        F.get_json_object("payload", "$.accounts"),
        ACCOUNT_LIST_SCHEMA,
    )
    balances = raw_responses.select(
        "plaid_item_pk",
        "run_id",
        "fetched_at",
        F.explode(account_list).alias("account"),
    )
    return balances.select(
        "plaid_item_pk",
        "run_id",
        F.col("fetched_at").alias("snapshot_at"),
        "account.account_id",
        "account.balances.current",
        "account.balances.available",
        F.col("account.balances.limit").alias("credit_limit"),
        "account.balances.iso_currency_code"
    )


def prepare_balances(raw_responses, accounts):
    balances = extract_balance_details(raw_responses)
    matched_balances = match_accounts(balances, accounts)
    return matched_balances


def apply_plaid_balance_snapshots(run_id):
    with log_stage(logger, "balance_snapshots", run_id=run_id) as fields:
        spark = create_spark_session("process_balances")
        try:
            raw_responses = read_postgres_table(spark, DATABASE_URL, TABLES["plaid_balances_raw"])
            raw_responses = raw_responses.filter(F.col("run_id") == run_id)
            accounts = read_postgres_table(spark, DATABASE_URL, TABLES["accounts"])
            processed_balances = prepare_balances(raw_responses, accounts)
            with db_conn() as conn:
                skipped_duplicates = write_balance_rows(conn, processed_balances)
            fields["duplicates_skipped"] = skipped_duplicates
        finally:
            spark.stop()
