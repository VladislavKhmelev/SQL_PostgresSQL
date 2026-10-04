~~~
# ────────────────────────────────────────────
# SPARK ENGINE
# PostgreSQL + Spark + Cloud.ru S3 + Iceberg
# ────────────────────────────────────────────

import os
import boto3

from dotenv import load_dotenv
from pyspark.sql import SparkSession


# ────────────────────────────────────────────
# PYSPARK
# ────────────────────────────────────────────

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"


# ────────────────────────────────────────────
# POSTGRESQL
# ────────────────────────────────────────────

load_dotenv(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\1.env"
)

pg_host = os.getenv("DB_HOST")
pg_port = os.getenv("DB_PORT")
pg_database = os.getenv("DB_NAME")
pg_user = os.getenv("DB_USER")
pg_password = os.getenv("DB_PASSWORD")

pg_url = (
    f"jdbc:postgresql://"
    f"{pg_host}:{pg_port}/{pg_database}"
)

pg_driver = "org.postgresql.Driver"


# ────────────────────────────────────────────
# CLOUD.RU S3
# ────────────────────────────────────────────

load_dotenv(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\cloud_s3.env"
)

s3_endpoint = os.getenv("S3_ENDPOINT")
s3_region = os.getenv("AWS_DEFAULT_REGION")
s3_access_key = os.getenv("AWS_ACCESS_KEY_ID")
s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
s3_bucket = os.getenv("S3_BUCKET")


# ────────────────────────────────────────────
# BOTO3 → CLOUD.RU S3
# ────────────────────────────────────────────

s3 = boto3.client(
    "s3",
    endpoint_url=s3_endpoint,
    region_name=s3_region,
    aws_access_key_id=s3_access_key,
    aws_secret_access_key=s3_secret_key
)


# ────────────────────────────────────────────
# SPARK
# ────────────────────────────────────────────

spark = (
    SparkSession.builder

    .appName("SparkApp")

    # PostgreSQL JDBC Driver
    .config(
        "spark.jars",
        r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\jars\PostgreSQL JDBC Driver\postgresql-42.7.12.jar"
    )

    # Hadoop AWS + Iceberg
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.5.0,"
        "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
    )

    # Iceberg
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.iceberg",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.iceberg.type",
        "hadoop"
    )
    .config(
        "spark.sql.catalog.iceberg.warehouse",
        f"s3a://{s3_bucket}/data/core/"
    )

    # Cloud.ru S3
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        s3_endpoint
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        s3_access_key
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        s3_secret_key
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint.region",
        s3_region
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    )

    # Spark temporary directories
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


# ────────────────────────────────────────────
# LOG LEVEL
# ────────────────────────────────────────────

spark.sparkContext.setLogLevel("ERROR")


# ────────────────────────────────────────────
# CHECK SETTINGS
# ────────────────────────────────────────────

print("PostgreSQL:", pg_host, pg_port, pg_database)
print("JDBC driver:", pg_driver)

print("S3 endpoint:", s3_endpoint)
print("S3 region:", s3_region)
print("S3 bucket:", s3_bucket)

print("Spark:", spark.version)

print("Iceberg catalog:")
print(
    spark.conf.get(
        "spark.sql.catalog.iceberg",
        "NOT FOUND"
    )
)

print("Iceberg warehouse:")
print(
    spark.conf.get(
        "spark.sql.catalog.iceberg.warehouse",
        "NOT FOUND"
    )
)

#------------------------------START PIPELINE-------------------------------


df_customers  = (
    spark.read
    .format("jdbc")
    .option("url", pg_url)
    .option("dbtable", "public.customers")
    .option("user", pg_user)
    .option("password", pg_password)
    .option("driver", pg_driver)
    .load()
)

df_customers.show(2)




df_orders  = (
    spark.read
    .format("jdbc")
    .option("url", pg_url)
    .option("dbtable", "public.orders")
    .option("user", pg_user)
    .option("password", pg_password)
    .option("driver", pg_driver)
    .load()
)

df_orders.show(2)


df_products = spark.read.csv(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\products.csv",
    header=True,
    inferSchema=True
)


df_products.show(2)


df_json = spark.read.json(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\df.json",
    multiLine=True
)


df_json.show(2)


df_customers.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"s3a://{s3_bucket}/ecommerce-data/PostgresSQl/customers/") \
    .option("header", "true") \
    .saveAsTable("customers")


df_orders.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"s3a://{s3_bucket}/ecommerce-data/PostgresSQl/orders/") \
    .option("header", "true") \
    .saveAsTable("orders")



df_products.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"s3a://{s3_bucket}/ecommerce-data/Location_file/") \
    .option("header", "true") \
    .saveAsTable("products")

df_json.write \
    .format("json") \
    .mode("overwrite") \
    .option("path", f"s3a://{s3_bucket}/ecommerce-data/API/") \
    .saveAsTable("body_json")

~~~
