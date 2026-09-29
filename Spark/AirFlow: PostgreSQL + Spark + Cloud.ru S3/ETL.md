~~~
#-------------SPARK ENGINE-------------
#-----------------------------PostgreSQL + Spark + Cloud.ru S3-------------------
import os
import boto3
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"


#────────────────────────────────────────────
# POSTGRESQL
#────────────────────────────────────────────

load_dotenv(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\1.env"
)

pg_host = os.getenv("DB_HOST")
pg_port = os.getenv("DB_PORT")
pg_database = os.getenv("DB_NAME")
pg_user = os.getenv("DB_USER")
pg_password = os.getenv("DB_PASSWORD")

pg_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"
pg_driver = "org.postgresql.Driver"


#────────────────────────────────────────────
# CLOUD.RU S3
#────────────────────────────────────────────

load_dotenv(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\cloud_s3.env"
)

s3_endpoint = os.getenv("S3_ENDPOINT")
s3_region = os.getenv("AWS_DEFAULT_REGION")
s3_access_key = os.getenv("AWS_ACCESS_KEY_ID")
s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
s3_bucket = os.getenv("S3_BUCKET")


#────────────────────────────────────────────
# BOTO3 → CLOUD.RU S3
#────────────────────────────────────────────

s3 = boto3.client(
    "s3",
    endpoint_url=s3_endpoint,
    region_name=s3_region,
    aws_access_key_id=s3_access_key,
    aws_secret_access_key=s3_secret_key
)


#────────────────────────────────────────────
# SPARK
#────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("SparkApp")
    .config(
        "spark.jars",
        r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\jars\PostgreSQL JDBC Driver\postgresql-42.7.12.jar"
    )
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.5.0"
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        os.getenv("S3_ENDPOINT")
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        os.getenv("AWS_ACCESS_KEY_ID")
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint.region",
        os.getenv("AWS_DEFAULT_REGION")
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    )
    .config(
        "spark.hadoop.fs.s3a.buffer.dir",
        "C:/spark_tmp/s3a"
    )
    .config(
        "spark.hadoop.hadoop.tmp.dir",
        "C:/spark_tmp"
    )
    .config(
        "spark.local.dir",
        "C:/spark_tmp"
    )
    .master("local[4]")
    .getOrCreate()
)

print("Spark started")


#-------------------------------------------------------------------------------


print("PostgreSQL:", pg_host, pg_port, pg_database)
print("User:", pg_user)
print("JDBC driver:", pg_driver)


test_df = spark.read \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "(SELECT 1 AS test) AS t") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .load()

print(test_df.toPandas().to_string(index=False))





df = (
    spark.read
    .format("jdbc")
    .option("url", pg_url)
    .option("dbtable", "orders_source")
    .option("user", pg_user)
    .option("password", pg_password)
    .option("driver", pg_driver)
    .load()
)

df.show()
df.printSchema()


df.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save(r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\output\orders")



load_date = datetime.now().strftime("%Y-%m-%d")

df.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save(f"s3a://{s3_bucket}/from_PostgreSQL/dt={load_date}/")





~~~
