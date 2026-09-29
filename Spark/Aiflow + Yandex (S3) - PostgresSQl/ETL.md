~~~

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from dotenv import load_dotenv
import boto3
import os

load_dotenv(r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\1.env")

pg_host = os.getenv("DB_HOST")
pg_port = os.getenv("DB_PORT")
pg_database = os.getenv("DB_NAME")
pg_user = os.getenv("DB_USER")
pg_password = os.getenv("DB_PASSWORD")

pg_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"
pg_driver = "org.postgresql.Driver"



#----------настройки ключа внутри env + подключение к сервисному аккаунту------------------------------------------------
load_dotenv("yandex_key.env")

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT"),
    region_name=os.getenv("AWS_DEFAULT_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

response = s3.list_buckets()

for bucket in response["Buckets"]:
    print("bucket: ", bucket["Name"],'\n')

#------------------------------------------------------------------



# ---------- настройки bucket ----------
bucket = os.getenv("S3_BUCKET")


# ---------- загрузка файла ----------
s3.upload_file(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\customers.csv",
    bucket,
    "raw/customers/customers.csv"
)

# путь будет: data-data/raw/customers/customers.csv
print("csv загрузили")





s3.upload_file(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\orders.json",
    bucket,
    "raw/orders/orders.json"
)

# путь будет: data-data/raw/orders/orders.csv
print("json загрузили")



#-------------------------Теперь RAW-слой в Object Storage готов:------------------------



spark = (
    SparkSession.builder
    .appName("YandexS3Test")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.5.0"
    )

    .config(
        "spark.jars",
        r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\jars\PostgreSQL JDBC Driver\postgresql-42.7.12.jar"
    )
    .config("spark.local.dir", "C:/spark_tmp")
    .config("spark.hadoop.tmp.dir", "C:/spark_tmp")
    .config("spark.hadoop.fs.s3a.buffer.dir", "C:/spark_tmp")
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint.region", os.getenv("AWS_DEFAULT_REGION"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)
print("Spark started")

#------------------CSV-------------------------


df = spark.read.csv(
    "s3a://data-data/raw/customers/customers.csv",
    header=True,
    inferSchema=True
)

print(df.toPandas(),'\n')
print(df.toPandas().dtypes)


#--------------createOrReplaceTempView-----------------------------
df.createOrReplaceTempView("tt")


#--------------validated_customers-----------------------------
validated_customers = spark.sql("""

SELECT

customer_id,     
name,    
city,
CASE
    WHEN customer_id IS NULL THEN 'customer_id is NULL'
    WHEN name IS NULL THEN 'name is NULL'
    WHEN city IS NULL THEN 'city IS NULL'
END 

as error_reason


from tt

""")

print(validated_customers.toPandas(),'\n')


validated_customers.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "s3a://data-data/validated/customers/") \
    .saveAsTable("validated_customers")




#--------------error_customers-----------------------------

spark.sql("""
CREATE TABLE error_customers (
    customer_id INT,
    name STRING,
    city STRING,
    error_reason STRING
)
USING CSV
OPTIONS (
    header 'true'
)
LOCATION 's3a://data-data/error/error_customers/';
""")


spark.sql("""
insert overwrite error_customers (
customer_id, 
name, 
city,
error_reason
)
SELECT
customer_id, 
name, 
city,
error_reason

from validated_customers
where error_reason is NOT NULL;

""")



error_customers=spark.sql("""
SELECT
*
from error_customers;

""")

print(error_customers.toPandas(),'\n')


#--------------core_customers-----------------------------

spark.sql("""
CREATE table core_customers (
customer_id int,     
name string,   
city string
)

USING parquet
LOCATION 's3a://data-data/core/core_customers/';
""")


spark.sql("""
insert overwrite core_customers (
customer_id, 
name, 
city
)
SELECT
customer_id, 
name, 
city

from validated_customers
where error_reason is  NULL;

""")



core_customers=spark.sql("""
SELECT
*
from core_customers;

""")

print(core_customers.toPandas(),'\n')





#-----------------JSON--------------------------

df: DataFrame = spark.read.json(
    "s3a://data-data/raw/orders/orders.json",
    multiLine=True
)

print(df.toPandas(),'\n')
print(df.toPandas().dtypes)


df.createOrReplaceTempView("tt")


validated_orders= spark.sql("""

SELECT
 amount,  
 customer_id,  
 order_id,     
 status,

 case when order_id is null then 'order_id is null'

 when  customer_id is null then 'customer_id is null'

 when amount <= 0 then 'amount <= 0'

 when status not in ('completed','cancelled') then 'status error format'

end as error_reason


from tt

""")


validated_orders.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "s3a://data-data/validated/orders/") \
    .saveAsTable("validated_orders")


spark.sql("""
CREATE table core_orders (
amount int, 
customer_id int, 
order_id int,    
status string

)

USING parquet
LOCATION 's3a://data-data/core/orders/core_orders';

""")



spark.sql("""
INSERT OVERWRITE core_orders (
amount,
customer_id, 
order_id,     
status 
)
SELECT
amount,
customer_id, 
order_id,     
status 


from validated_orders

where error_reason is null;

""")



spark.sql("""
CREATE table error_orders (
amount int, 
customer_id int, 
order_id int,    
status string,
error_reason string

)

USING parquet
LOCATION 's3a://data-data/error/orders/error_orders';

""")




spark.sql("""
INSERT OVERWRITE error_orders (
amount,
customer_id, 
order_id,     
status,
error_reason
)
SELECT
amount,
customer_id, 
order_id,     
status,
error_reason


from validated_orders

where error_reason is not null;

""")



core_orders=spark.sql("""

SELECT
*
from core_orders

""")

print(core_orders.toPandas(),'\n')



error_orders= spark.sql("""
SELECT
*
from error_orders

""")

print(error_orders.toPandas(),'\n')




#-------------DATA MART-------------


data_mart = spark.sql("""
SELECT
    core_customers.customer_id,
    core_customers.name,
    core_customers.city,
    COUNT(core_orders.order_id) AS orders_count,
    COALESCE(SUM(core_orders.amount), 0) AS total_amount
FROM core_customers
LEFT JOIN core_orders
    ON core_orders.customer_id = core_customers.customer_id
GROUP BY
    core_customers.customer_id,
    core_customers.name,
    core_customers.city;
""")



print(data_mart.toPandas(),'\n')
print(data_mart.toPandas().dtypes)


data_mart.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", "s3a://data-data/data_mart/") \
    .saveAsTable("data_mart")




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


data_mart.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "data_mart") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .mode("overwrite") \
    .save()




#-------------POST LOAD CHECK-------------

print("Количество строк таблицы Data Mart в S3:", data_mart.count())


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
