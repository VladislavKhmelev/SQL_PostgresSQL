~~~
#────────────────────────────────────────────
# SPARK + HDFS + ICEBERG + POSTGRESQL
#────────────────────────────────────────────

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from sqlalchemy import Select

#────────────────────────────────────────────
# PYSPARK
#────────────────────────────────────────────

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
# SPARK + HDFS + ICEBERG
#────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("SparkApp")

    #────────────────────────────────────────
    # PostgreSQL JDBC
    #────────────────────────────────────────

    .config(
        "spark.jars",
        r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\jars\PostgreSQL JDBC Driver\postgresql-42.7.12.jar"
    )

    #────────────────────────────────────────
    # ICEBERG
    #────────────────────────────────────────

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

    #────────────────────────────────────────
    # HDFS
    #────────────────────────────────────────

    .config(
        "spark.hadoop.fs.defaultFS",
        "hdfs://localhost:9000"
    )

    .config(
        "spark.hadoop.dfs.client.use.datanode.hostname",
        "true"
    )

    #────────────────────────────────────────
    # SPARK
    #────────────────────────────────────────

    .master("local[4]")

    .getOrCreate()
)


#────────────────────────────────────────────
# LOG LEVEL
#────────────────────────────────────────────

spark.sparkContext.setLogLevel("ERROR")


#────────────────────────────────────────────
# CHECK HDFS
#────────────────────────────────────────────

print(
    "HDFS:",
    spark.conf.get("spark.hadoop.fs.defaultFS")
)


#────────────────────────────────────────────
# CHECK ICEBERG
#────────────────────────────────────────────

print(
    "Iceberg Catalog:",
    spark.conf.get("spark.sql.catalog.iceberg")
)

print(
    "Iceberg Type:",
    spark.conf.get("spark.sql.catalog.iceberg.type")
)

print(
    "Iceberg Warehouse:",
    spark.conf.get("spark.sql.catalog.iceberg.warehouse")
)


#────────────────────────────────────────────
# PANDAS
#────────────────────────────────────────────

import pandas as pd

pd.set_option(
    "display.max_columns",
    None
)

pd.set_option(
    "display.width",
    None
)


#============================================
# START CODE
#============================================

print("HDFS:", spark.conf.get("spark.hadoop.fs.defaultFS"))

print(
    "Catalog:",
    spark.conf.get("spark.sql.catalog.iceberg")
)

print(
    "Type:",
    spark.conf.get("spark.sql.catalog.iceberg.type")
)

print(
    "Warehouse:",
    spark.conf.get("spark.sql.catalog.iceberg.warehouse")
)


# ============================================================
# СОЗДАНИЕ ICEBERG ТАБЛИЦЫ
# ============================================================





df = (
    spark.read
    .format("jdbc")
    .option("url", pg_url)
    .option("dbtable", "orders")
    .option("user", pg_user)
    .option("password", pg_password)
    .option("driver", pg_driver)
    .load()
)

print(df.toPandas(), '\n')

df.printSchema()


df.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", f"hdfs:///data/raw/") \
    .saveAsTable("raw_orders")




# 3. ПРОВЕРЯЕМ TARGET CORE

table_exists = spark.catalog.tableExists(
    "iceberg.core_orders"
)

# ПЕРВЫЙ ЗАПУСК — INITIAL LOAD

if not table_exists:

    print("CORE не существует. Выполняем INITIAL LOAD.")



    spark.sql("""
    CREATE table iceberg.core_orders (
    order_id int,
    customer_id int, 
    order_date date,     
    status string,                  
    amount DOUBLE,         
    updated_at TIMESTAMP
    )
    
    USING iceberg;
    
    """)



    spark.sql("""
    INSERT INTO iceberg.core_orders (
    order_id, 
    customer_id,
    order_date,     
    status,                  
    amount,         
    updated_at   
    )
    SELECT
    order_id, 
    customer_id,
    order_date,     
    status,                  
    amount,         
    updated_at  
    
    
    from raw_orders;
    
    
    """)


# ПОСЛЕДУЮЩИЙ ЗАПУСК — INCREMENTAL LOAD

else:

    print("CORE существует. Выполняем INCREMENTAL LOAD.")


# Получаем последний watermark из CORE
    watermark = spark.sql("""
        SELECT
            MAX(updated_at) AS watermark
        FROM iceberg.core_orders
    """).collect()[0]["watermark"]


    print("Watermark:", watermark)



 # Читаем актуальную таблицу из PostgreSQL


# 2. Читаем источник
    df = (
        spark.read
        .format("jdbc")
        .option("url", pg_url)
        .option("dbtable", "orders")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", pg_driver)
        .load()
    )

    df.createOrReplaceTempView("orders")


    # Берём только новые/изменённые записи
    spark.sql(f"""
        CREATE TEMP VIEW increment AS
        SELECT
            order_id,
            customer_id,
            order_date,
            status,
            amount,
            updated_at
        FROM orders
        WHERE updated_at > TIMESTAMP '{watermark}'
    """)



# MERGE В ICEBERG CORE

    spark.sql("""
        MERGE INTO iceberg.core_orders AS t

        USING increment AS s

        ON t.order_id = s.order_id

        WHEN MATCHED THEN
            UPDATE SET
                t.customer_id = s.customer_id,
                t.order_date = s.order_date,
                t.status = s.status,
                t.amount = s.amount,
                t.updated_at = s.updated_at

        WHEN NOT MATCHED THEN
            INSERT (
                order_id,
                customer_id,
                order_date,
                status,
                amount,
                updated_at
            )
            VALUES (
                s.order_id,
                s.customer_id,
                s.order_date,
                s.status,
                s.amount,
                s.updated_at
            )
    """)


# ПРОВЕРЯЕМ CORE




print("Целевая таблица на сегодня:")

spark.sql("""
    SELECT *
    FROM iceberg.core_orders
    ORDER BY order_id
""").show(n=100, truncate=False)


print("Строки, NEW обработанные через incremental load:")
spark.sql(f"""
    SELECT *
    FROM iceberg.core_orders
    WHERE updated_at > TIMESTAMP '{watermark}'
    ORDER BY order_id
""").show()



watermark = spark.sql("""
    SELECT
        MAX(updated_at) AS watermark
    FROM iceberg.core_orders
""").collect()[0]["watermark"]


print("Новый Watermark:", watermark)


~~~
