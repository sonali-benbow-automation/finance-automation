from config import DATABASE_URL
from pyspark.sql import SparkSession
from urllib.parse import urlparse

parsed_url = urlparse(DATABASE_URL)
jdbc_url = f"jdbc:postgresql://{parsed_url.hostname}:{parsed_url.port}{parsed_url.path}"

props = {
    "user": parsed_url.username,
    "password": parsed_url.password,
    "driver": "org.postgresql.Driver",
}

spark = (
    SparkSession.builder
    .appName("read_raw")
    .master("local[*]")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)

df = spark.read.jdbc(url=jdbc_url, table="plaid_transactions_raw", properties=props)
df.show(5)
df.printSchema()
print(df.count())