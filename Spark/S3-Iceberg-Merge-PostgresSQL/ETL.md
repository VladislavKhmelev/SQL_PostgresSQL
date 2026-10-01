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
#=========================================START PIPELINE==============================================
from datetime import datetime

load_date = datetime.now().strftime("%Y-%m-%d")

s3.upload_file(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\products.csv",
    s3_bucket,
    f"data/raw/products/dt={load_date}/products.csv"
)

df_products = spark.read.csv(
    f"s3a://{s3_bucket}/data/raw/products/dt={load_date}/products.csv",
    header=True,
    inferSchema=True
)
print("загружено в S3 csv")
print(df_products.toPandas(),'\n')





s3.upload_file(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\sales.json",
    s3_bucket,
    f"data/raw/sales/dt={load_date}/sales.json"
)

df_sales = spark.read.json(
    f"s3a://{s3_bucket}/data/raw/sales/dt={load_date}/sales.json",
    multiLine=True
)
print("загружено в S3 json")
print(df_sales.toPandas(), '\n')


df_products.createOrReplaceTempView("tt")

df_sales.createOrReplaceTempView("tt1")




validated_products = spark.sql("""
SELECT
product_id,      
name,     
category,  
price,

case when product_id is null then 'product_id is null'
when name is null then 'name is null'
when category is null then 'category is null'
when price <= 0 then 'price <= 0'
end as error_reason

from tt;

""")


validated_products.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"s3a://{s3_bucket}/data/validated/products/dt={load_date}/") \
    .option("header", "true") \
    .saveAsTable("validated_products")




error_products=spark.sql("""
 SELECT
product_id,      
name,     
category,  
price,
error_reason

from validated_products

where error_reason is not null;

""")




error_products.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"s3a://{s3_bucket}/data/error/error_products/dt={load_date}/") \
    .option("header", "true") \
    .saveAsTable("error_products")





core_products_today= spark.sql("""

SELECT
product_id,      
name,     
category,  
price


from validated_products

where error_reason is null;

""")


# нужна временная регистрация для Merge
core_products_today.createOrReplaceTempView("core_products_today")


print("какие строки сегодня будем добавлять в CORE")
print(core_products_today.toPandas(),'\n')



#-----Теперь создаём постоянную Iceberg-таблицу core_products------
# .config(
#     "spark.sql.catalog.iceberg.warehouse",
#     f"s3a://{s3_bucket}/data/core/"
# )
# здесь локацию не пишем, будет ругаться, поэтому настоено в конфиге


spark.sql(f"""
CREATE TABLE IF NOT EXISTS iceberg.core_products (
    product_id INT,
    name STRING,
    category STRING,
    price DOUBLE
)
USING iceberg

""")

#-----------Теперь соединяем сегодняшнюю порцию с постоянным Core через MERGE.------




spark.sql("""
MERGE INTO iceberg.core_products AS target
USING core_products_today AS source
ON target.product_id = source.product_id

WHEN MATCHED THEN
    UPDATE SET
        target.name = source.name,
        target.category = source.category,
        target.price = source.price

WHEN NOT MATCHED THEN
    INSERT (
        product_id,
        name,
        category,
        price
    )
    VALUES (
        source.product_id,
        source.name,
        source.category,
        source.price
    )
""")




core_products = spark.sql("""
SELECT *
FROM iceberg.core_products
ORDER BY product_id
""")

print("накопленное состояние в CORE")
print(core_products.toPandas(),'\n')


#------------------------------SALES----------------------------------------


validated_sales=spark.sql("""
 SELECT
product_id,
quantity,
sale_date,
sale_id,
status,

case when sale_id is null then 'sale_id is null'

when product_id is null then 'product_id is null'

when quantity <= 0 then 'quantity <= 0 '

when status not in ('completed','cancelled') then 'status error format'

when not EXISTS (select 1
    from tt 
    where tt.product_id = tt1.product_id) then 'product_id is orphan'
end as error_reason

from tt1;


""")


validated_sales.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"s3a://{s3_bucket}/data/validated/sales/dt={load_date}/") \
    .option("header", "true") \
    .saveAsTable("validated_sales")


error_sales=spark.sql("""
 SELECT
product_id,  
quantity,   
sale_date,  
sale_id,     
status,
error_reason

from validated_sales

where error_reason is not null;


""")



error_sales.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"s3a://{s3_bucket}/data/error/error_sales/dt={load_date}/") \
    .option("header", "true") \
    .saveAsTable("error_sales")




core_sales_today =spark.sql("""
 SELECT
product_id,  
quantity,   
cast(sale_date as date) as sale_date,  
sale_id,     
status


from validated_sales

where error_reason is null;

""")

core_sales_today.createOrReplaceTempView("core_sales_today")


spark.sql(f"""
CREATE table if not EXISTS iceberg.core_sales (
 product_id int,  
 quantity int,   
 sale_date date, 
 sale_id int,    
 status string
)

USING iceberg
""")




spark.sql("""

MERGE INTO iceberg.core_sales AS t

USING core_sales_today AS s

ON t.sale_id = s.sale_id

WHEN MATCHED THEN

    UPDATE SET
        t.product_id = s.product_id,
        t.quantity = s.quantity,
        t.sale_date = s.sale_date,
        t.status = s.status

WHEN NOT MATCHED THEN

    INSERT (
        product_id,
        quantity,
        sale_date,
        sale_id,
        status
    )

    VALUES (
        s.product_id,
        s.quantity,
        s.sale_date,
        s.sale_id,
        s.status
    )
""")



core_sales = spark.sql("""
select
*
from iceberg.core_sales

""")

print(core_sales.toPandas(),'\n')








print("таблицы зарегестрированные в Spark Catalog:")
spark.sql("SHOW TABLES").show()


df_products = spark.sql("""
    SELECT *
    FROM iceberg.core_products
""").toPandas()

print("Таблица core_products в Iceberg:")
print(df_products)


df_sales = spark.sql("""
    SELECT *
    FROM iceberg.core_sales
""").toPandas()

print("Таблица core_sales в Iceberg:")
print(df_sales)



data_mart = spark.sql("""
    SELECT
        p.product_id,
        p.name,
        p.category,

        COUNT(
            CASE
                WHEN s.status = 'completed'
                THEN s.sale_id
            END
        ) AS sales_count,

        COALESCE(
            SUM(
                CASE
                    WHEN s.status = 'completed'
                    THEN s.quantity
                END
            ), 0
        ) AS total_quantity,

        p.price * COALESCE(
            SUM(
                CASE
                    WHEN s.status = 'completed'
                    THEN s.quantity
                END
            ), 0
        ) AS total_revenue

    FROM iceberg.core_products AS p

    INNER JOIN iceberg.core_sales AS s
        ON s.product_id = p.product_id

    GROUP BY
        p.product_id,
        p.name,
        p.category,
        p.price

    ORDER BY sales_count DESC
""")

data_mart.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", f"s3a://{s3_bucket}/data/data_mart/dt={load_date}/") \
    .saveAsTable("data_mart")




data_mart.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "data_mart") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .mode("overwrite") \
    .save()



data_mart_s3 = spark.read.parquet(
    f"s3a://{s3_bucket}/data/data_mart/dt={load_date}/"
)

print(
    "Количество строк Data Mart в S3:",
    data_mart_s3.count()
)


check_df = spark.read \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "data_mart") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .load()

print("PostgreSQL rows загружено:", check_df.count())

~~~
