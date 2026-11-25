import logging
from pyspark.sql import functions as F

from config import DATABASE_URL, TABLES
from db.db import db_conn
from db.repos.accounts import write_account_rows
from jobs.spark import create_spark_session, read_postgres_table
from app_logging import log_stage

logger = logging.getLogger(__name__)



ACCOUNT_SCHEMA = (
    "ARRAY<STRUCT<"
    "account_id: STRING, "
    "name: STRING, "
    "official_name: STRING, "
    "type: STRING, "
    "subtype: STRING, "
    "mask: STRING, "
    "balances: STRUCT<"
    "iso_currency_code: STRING"
    ">"
    ">>"
)


def extract_account_details(raw_responses):
    account_list = F.from_json(
        F.get_json_object("payload", "$.accounts"),
        ACCOUNT_SCHEMA,
    )
    accounts = raw_responses.select(
        "plaid_item_pk",
        F.explode(account_list).alias("account"),
    )
    return accounts.select(
        "plaid_item_pk",
        "account.account_id",
        "account.name",
        "account.official_name",
        F.col("account.type").alias("account_type"),
        "account.subtype",
        "account.mask",
        F.col("account.balances.iso_currency_code").alias("iso_currency_code"),
    )


def apply_plaid_accounts(run_id):
    with log_stage(logger, "accounts", run_id=run_id):
        spark = create_spark_session("process_accounts")
        try:
            raw_responses = read_postgres_table(spark, DATABASE_URL, TABLES["plaid_balances_raw"])
            raw_responses = raw_responses.filter(F.col("run_id") == run_id)
            processed_accounts = extract_account_details(raw_responses)
            with db_conn() as conn:
                write_account_rows(conn, processed_accounts)
        finally:
            spark.stop()
