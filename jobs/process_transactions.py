from urllib.parse import urlparse
from pyspark.sql import SparkSession, functions as F


TRANSACTION_LIST_SCHEMA = (
    "ARRAY<STRUCT<"
    "transaction_id: STRING, account_id: STRING, "
    "date: STRING, name: STRING, amount: DECIMAL(18,2), "
    "merchant_name: STRING, pending: BOOLEAN, "
    "iso_currency_code: STRING, pending_transaction_id: STRING, "
    "category_id: STRING, category: ARRAY<STRING>, "
    "personal_finance_category: STRUCT<primary: STRING, detailed: STRING, "
    "confidence_level: STRING, version: STRING>, "
    "payment_channel: STRING, transaction_type: STRING, "
    "authorized_date: DATE, datetime: TIMESTAMP, authorized_datetime: TIMESTAMP"
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


def extract_transaction_details(raw_responses, list_name):
    transaction_list = F.from_json(
        F.get_json_object("payload", f"$.{list_name}"),
        TRANSACTION_LIST_SCHEMA,
    )
    transactions = raw_responses.select(
        "plaid_item_pk",
        "run_id",
        F.explode(transaction_list).alias("transaction"),
    )
    return transactions.select(
        "plaid_item_pk",
        "run_id",
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


def match_transaction_accounts(transaction_details, accounts):
    account_lookup = accounts.select(
        "plaid_item_pk",
        "account_id",
        F.col("id").alias("account_pk"),
    )
    return transaction_details.join(
        account_lookup,
        on=["plaid_item_pk", "account_id"],
        how="left",
    )


def prepare_transaction_changes(raw_responses, accounts, list_name):
    transaction_details = extract_transaction_details(raw_responses, list_name)
    matched_transactions = match_transaction_accounts(transaction_details, accounts)
    return matched_transactions.withColumn("last_seen_run_id", F.col("run_id"))


def prepare_new_transactions(raw_responses, accounts):
    added_transactions = prepare_transaction_changes(raw_responses, accounts, "added")
    return added_transactions.withColumn("first_seen_run_id", F.col("run_id"))


def prepare_modified_transactions(raw_responses, accounts, existing_transactions):
    modified_transactions = prepare_transaction_changes(raw_responses, accounts, "modified")
    first_seen_runs = existing_transactions.select(
        "transaction_id", "first_seen_run_id"
    )
    return modified_transactions.join(
        first_seen_runs, on="transaction_id", how="inner"
    )

def prepare_removed_transactions(raw_responses):
    return extract_transaction_details(raw_responses, "removed").select(
        "transaction_id", "run_id"
    )



def main(run_id):
    from config import DATABASE_URL, TABLES

    spark = (
        SparkSession.builder
        .appName("process_transactions")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    try:
        raw_responses = read_postgres_table(spark, DATABASE_URL, TABLES["plaid_transactions_raw"])
        raw_responses = raw_responses.filter(F.col("run_id") == run_id)
        accounts = read_postgres_table(spark, DATABASE_URL, TABLES["accounts"])
        existing_transactions = read_postgres_table(spark, DATABASE_URL, TABLES["transactions"])
        prepare_new_transactions(raw_responses, accounts).show()
        prepare_modified_transactions(raw_responses, accounts, existing_transactions)
        prepare_removed_transactions(raw_responses)
    finally:
        spark.stop()
