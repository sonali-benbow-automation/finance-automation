from urllib.parse import urlparse

from pyspark.sql import SparkSession, functions as F


def create_spark_session(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
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


def match_accounts(records, accounts):
    account_lookup = accounts.select(
        "plaid_item_pk",
        "account_id",
        F.col("id").alias("account_pk"),
    )
    return records.join(
        account_lookup,
        on=["plaid_item_pk", "account_id"],
        how="left",
    )
