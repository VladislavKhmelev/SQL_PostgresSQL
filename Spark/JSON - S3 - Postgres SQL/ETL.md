~~~
#-------------SPARK ENGINE-------------
#-----------------------------PostgreSQL + Spark + Cloud.ru S3-------------------
import os
import boto3
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from datetime import datetime



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
        "org.apache.hadoop:hadoop-aws:3.5.0,"
        "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
    )
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
#--------------------------------------------------------------
print("ICEBERG CONFIG:")
print(spark.conf.get("spark.sql.catalog.iceberg", "NOT FOUND"))

print("ICEBERG TYPE:")
print(spark.conf.get("spark.sql.catalog.iceberg.type", "NOT FOUND"))

print("ICEBERG WAREHOUSE:")
print(spark.conf.get("spark.sql.catalog.iceberg.warehouse", "NOT FOUND"))

#-------------------------------------------------------------------


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


print("Spark started")

print("Каталоги:")
spark.sql("SHOW CATALOGS").show()


import pandas as pd
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)


#=========================================START PIPELINE==============================================
from datetime import datetime


df = spark.read.json(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\events.json",
    multiLine=True
)

# print(df.toPandas(),'\n')
# print(df.toPandas().dtypes)

load_date = datetime.now().strftime("%Y-%m-%d")

s3.upload_file(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\events.json",
    s3_bucket,
    f"data/raw/events/dt={load_date}/raw_events.json"
)


df1 = spark.read.json(
    f"s3a://{s3_bucket}/data/raw/events/dt={load_date}/raw_events.json",
    multiLine=True
)

# print(df1.toPandas(), '\n')


df1.createOrReplaceTempView("tt")


df2 = spark.sql("""
SELECT

event_id,           
cast(event_time as TIMESTAMP) as event_time,   
event_type,       
page,  
user_id,

case when count(*) over(PARTITION BY event_id) > 1 then 'event_id is dub'
when page is null then 'page is null'
when event_type not in ('page_view','purchase','add_to_cart') then 'event_type error format'
end as error_reason


from tt;

""")


df2.write \
    .format("json") \
    .mode("overwrite") \
    .option("path", f"s3a://{s3_bucket}/data/validated/events/dt={load_date}/") \
    .saveAsTable("validated_events")


# df2.show()

# print(df2.toPandas().dtypes)


df2.createOrReplaceTempView("df2")

df3=spark.sql("""
SELECT

event_id,
event_time,
event_type,
page,
user_id,
error_reason

from df2

where error_reason is not null;

""")


df3.write \
    .format("json") \
    .mode("overwrite") \
    .option("path", f"s3a://{s3_bucket}/data/error/error_event/dt={load_date}/") \
    .saveAsTable("error_event")




df4 = spark.read.json(
    f"s3a://{s3_bucket}/data/validated/events/dt={load_date}/",
)

print(df4.toPandas(), '\n')


df4.createOrReplaceTempView("df4")


df4_today = spark.sql("""
SELECT
event_id,          
cast(event_time as timestamp) as event_time,   
event_type,       
page,  
user_id

from df4

where error_reason is null;

""")


df4_today.createOrReplaceTempView("df4_today")



spark.sql("""
CREATE table if not EXISTS iceberg.core_event (
event_id  int,         
event_time TIMESTAMP,  
event_type string,      
page string, 
user_id int

)

USING iceberg

""")




spark.sql("""
MERGE into iceberg.core_event as t
USING df4_today as s
on s.event_id = t.event_id

when MATCHED THEN 
UPDATE SET
t.event_time = s.event_time,
t.event_type = s.event_type,
t.page = s.page,
t.user_id = s.user_id

when not MATCHED THEN
INSERT (
    event_id,           
    event_time,   
    event_type,       
    page,  
    user_id
)
VALUES (
    s.event_id,           
    s.event_time,   
    s.event_type,       
    s.page,  
    s.user_id
)

""")


df_products = spark.sql("""
    SELECT *
    FROM iceberg.core_event
""").toPandas()

print("Таблица в Iceberg:")
print(df_products)



data_mart_1 = spark.sql("""
SELECT

date(event_time) as day,

    COUNT(DISTINCT user_id) AS unique_users,

    COUNT(CASE
        WHEN event_type = 'page_view'
        THEN 1
    END) AS page_views,

    COUNT(CASE
        WHEN event_type = 'add_to_cart'
        THEN 1
    END) AS add_to_cart,

    COUNT(CASE
        WHEN event_type = 'purchases'
        THEN 1
    END) AS purchases,

    COUNT(CASE
        WHEN event_type = 'purchases'
        THEN 1
    END)
    / NULLIF(COUNT(DISTINCT user_id), 0) * 100 AS conversion_rate

FROM iceberg.core_event

GROUP BY date(event_time);
""")


data_mart_1.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", f"s3a://{s3_bucket}/data/data_mart/data_mart_1/dt={load_date}/") \
    .saveAsTable("data_mart_1")






data_mart_2 = spark.sql("""
SELECT
user_id,
count(case when event_type = 'page_view' then 1 end ) as page_view,
count(case when event_type = 'add_to_cart' then 1 end ) as cart_events,
count(case when event_type = 'purchases' then 1 end ) as purchases


from iceberg.core_event

GROUP BY user_id;

""")

data_mart_2.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", f"s3a://{s3_bucket}/data/data_mart/data_mart_2/dt={load_date}/") \
    .saveAsTable("data_mart_2")





data_mart_1.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "data_mart_1") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .mode("overwrite") \
    .save()




data_mart_2.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "data_mart_2") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .mode("overwrite") \
    .save()


~~~
