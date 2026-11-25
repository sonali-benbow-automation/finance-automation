import logging
from pyspark.sql import functions as F

from config import DATABASE_URL, TABLES
from db.db import db_conn
from db.repos.transactions import write_transactions_rows
from jobs.spark import create_spark_session, read_postgres_table, match_accounts
from app_logging import log_stage

logger = logging.getLogger(__name__)



TRANSACTION_LIST_SCHEMA = (
    "ARRAY<STRUCT<"
    "transaction_id: STRING, account_id: STRING, "
    "date: DATE, name: STRING, amount: DECIMAL(18,2), "
    "merchant_name: STRING, pending: BOOLEAN, "
    "iso_currency_code: STRING, pending_transaction_id: STRING, "
    "category_id: STRING, category: ARRAY<STRING>, "
    "personal_finance_category: STRUCT<primary: STRING, detailed: STRING, "
    "confidence_level: STRING, version: STRING>, "
    "payment_channel: STRING, transaction_type: STRING, "
    "authorized_date: DATE, datetime: TIMESTAMP, authorized_datetime: TIMESTAMP"
    ">>"
)


def extract_transaction_details(raw_responses, list_name):
    transaction_list = F.from_json(
        F.get_json_object("payload", f"$.{list_name}"),
        TRANSACTION_LIST_SCHEMA,
    )
    transactions = raw_responses.select(
        "plaid_item_pk",
        "run_id",
        "page_index",
        F.explode(transaction_list).alias("transaction"),
    )
    return transactions.select(
        "plaid_item_pk",
        "run_id",
        "page_index",
        "transaction.transaction_id",
        "transaction.account_id",
        "transaction.date",
        "transaction.name",
        "transaction.amount",
        "transaction.merchant_name",
        "transaction.pending",
        "transaction.iso_currency_code",
        "transaction.pending_transaction_id",
        "transaction.category_id",
        F.array_join("transaction.category", ", ").alias("category"),
        F.to_json("transaction.personal_finance_category").alias("personal_finance_category"),
        "transaction.payment_channel",
        "transaction.transaction_type",
        "transaction.authorized_date",
        "transaction.datetime",
        "transaction.authorized_datetime",
        F.lit(list_name).alias("sync_status"),
    )


def prepare_transactions(raw_responses, accounts):
    added_transactions = extract_transaction_details(raw_responses, "added")
    modified_transactions = extract_transaction_details(raw_responses, "modified")
    removed_transactions = extract_transaction_details(raw_responses, "removed")
    combined_transactions = (added_transactions.unionByName(modified_transactions).unionByName(removed_transactions))
    matched_transactions = match_accounts(combined_transactions, accounts)
    return matched_transactions.withColumn("first_seen_run_id", F.col("run_id"))


def apply_plaid_transaction_changes(run_id):
    with log_stage(logger, "transactions", run_id=run_id) as fields:
        spark = create_spark_session("process_transactions")
        try:
            raw_responses = read_postgres_table(spark, DATABASE_URL, TABLES["plaid_transactions_raw"])
            raw_responses = raw_responses.filter(F.col("run_id") == run_id)
            accounts = read_postgres_table(spark, DATABASE_URL, TABLES["accounts"])
            ordered_transactions = prepare_transactions(raw_responses, accounts).orderBy(
                "plaid_item_pk", "page_index"
            )
            with db_conn() as conn:
                unmatched_changes = write_transactions_rows(conn, ordered_transactions)
            fields["unmatched_changes"] = len(unmatched_changes)
            if unmatched_changes:
                logger.warning("stage=transactions run_id=%s unmatched_changes=%s", run_id, len(unmatched_changes))
        finally:
            spark.stop()
