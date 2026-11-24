from urllib.parse import urlparse
from pyspark.sql import SparkSession, functions as F
from config import DATABASE_URL, TABLES
from db.db import db_conn
from db.repos.balances import write_balance_rows



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

def read_postgres_table(spark, database_url, table_name):
    parsed_url = urlparse(database_url)
    jdbc_url = f"jdbc:postgresql://{parsed_url.hostname}:{parsed_url.port}{parsed_url.path}"
    properties = {
        "user": parsed_url.username,
        "password": parsed_url.password,
        "driver": "org.postgresql.Driver",
        "sslmode": "require",
    }
    return spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)


def extract_balance_details(raw_responses):
    account_list = F.from_json(
        F.get_json_object("payload", f"$.accounts"),
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


def match_balance_accounts(balance_details, accounts):
    account_lookup = accounts.select(
        "plaid_item_pk",
        "account_id",
        F.col("id").alias("account_pk"),
    )
    return balance_details.join(
        account_lookup,
        on=["plaid_item_pk", "account_id"],
        how="left",
    )

def prepare_balances(raw_responses, accounts):
    balances = extract_balance_details(raw_responses)
    matched_balances = match_balance_accounts(balances, accounts)
    return matched_balances


def apply_plaid_balance_snapshots(run_id):
    spark = (
        SparkSession.builder
        .appName("process_balances")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    try:
        raw_responses = read_postgres_table(spark, DATABASE_URL, TABLES["plaid_balances_raw"])
        raw_responses = raw_responses.filter(F.col("run_id") == run_id)
        accounts = read_postgres_table(spark, DATABASE_URL, TABLES["accounts"])
        processed_balances = prepare_balances(raw_responses, accounts)
        with db_conn() as conn:
            skipped_duplicates = write_balance_rows(conn, processed_balances)
        print(f"Run {run_id} balance snapshots committed. Duplicates skipped: {skipped_duplicates}")
    finally:
        spark.stop()
