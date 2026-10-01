~~~
import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from dotenv import load_dotenv


import os
import sys

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

load_dotenv(r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\1.env")

pg_host = os.getenv("DB_HOST")
pg_port = os.getenv("DB_PORT")
pg_database = os.getenv("DB_NAME")
pg_user = os.getenv("DB_USER")
pg_password = os.getenv("DB_PASSWORD")

pg_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"
pg_driver = "org.postgresql.Driver"



os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"



spark = (
    SparkSession.builder
    .appName("SparkApp")

    # PostgreSQL JDBC
    .config(
        "spark.jars",
        r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\jars\PostgreSQL JDBC Driver\postgresql-42.7.12.jar"
    )

    # Iceberg
    .config(
        "spark.jars.packages",
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
        "hdfs:///data/core/"
    )

    # HDFS
    .config(
        "spark.sql.warehouse.dir",
        "hdfs:///user/hive/warehouse"
    )
    .config(
        "spark.hadoop.fs.defaultFS",
        "hdfs://localhost:9000"
    )
    .config(
        "spark.hadoop.dfs.client.use.datanode.hostname",
        "true"
    )

    .master("local[4]")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")



# показывает путь куда будет сохранять физически таблицы по умолчанию если Managed Table
warehouse = spark.conf.get("spark.sql.warehouse.dir")

print(
    "Warehouse:",
    warehouse.replace("hdfs://localhost:9000", "hdfs://")
)

import pandas as pd
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)


print("Базы Hive:")
spark.sql("SHOW DATABASES").show()

print("Catalog:", spark.conf.get("spark.sql.catalogImplementation"))

#=================START COD==================================================

from datetime import datetime

df = spark.read.csv(
    "hdfs:///data/raw/raw_events/events.csv",
    header=True,
    inferSchema=True
)

print("Данные из HDFS:")
print(df.toPandas(), "\n")

print("Типы данных:")
print(df.toPandas().dtypes)



df.createOrReplaceTempView("tt")



validated = spark.sql("""

SELECT
event_id,          
event_time,   
event_type,       
page,  
user_id,

case when event_id is null then 'when event_id is null'
when event_type not in ('page_view','add_to_cart','purchase') then 'event_type error format'
when page is null then 'page is null '
when user_id is null then 'user_id is null'
end as error_reason

from tt;

""")


load_date = datetime.now().strftime("%Y-%m-%d")

validated.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"hdfs:///data/validated/dt={load_date}/") \
     .option("header", "true") \
    .saveAsTable("validated_event")

spark.sql("SHOW TABLES").show()


spark.sql("""
    SELECT *
    FROM validated_event
""").show()




error = spark.sql("""
SELECT
    event_id,
    event_time,
    event_type,
    page,
    user_id,
    error_reason
FROM validated_event
WHERE error_reason IS NOT NULL
""")








error.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"hdfs:///data/error/dt={load_date}/") \
    .option("header", "true") \
    .saveAsTable("error_event")


spark.sql("""
    SELECT *
    FROM error_event
""").show()





core_today= spark.sql("""
SELECT
event_id,
event_time,
event_type,
page,
user_id


from validated_event

where error_reason is null;

""")


core_today.createOrReplaceTempView("core_today")




spark.sql("""
CREATE table if not EXISTS iceberg.core_event (
event_id int,
event_time TIMESTAMP,
event_type string,
page string,
user_id int

)

USING iceberg;

""")




spark.sql("""
merge into iceberg.core_event as t
USING core_today as s
on s.event_id = t.event_id

when MATCHed then
UPDATE SET
t.event_time = s.event_time,
t.event_type = s.event_type,
t.page = s.page,
t.user_id = s.user_id

when not MATCHed then 
insert (
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
);

""")


spark.sql("""
    SELECT *
    FROM iceberg.core_event
""").show()






data_mart = spark.sql("""


SELECT
event_type,

count(event_type) as event_count

from iceberg.core_event

GROUP BY
event_type;
""")


data_mart.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", "hdfs:///data/data_mart/") \
    .saveAsTable("data_mart_event")


spark.sql("""
    SELECT *
    FROM data_mart_event
""").show()


~~~
