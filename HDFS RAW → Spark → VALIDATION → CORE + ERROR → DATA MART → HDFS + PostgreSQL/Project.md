~~~
import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from dotenv import load_dotenv


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



spark = SparkSession.builder \
    .appName("SparkApp") \
    .config(
        "spark.jars",
        r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\jars\PostgreSQL JDBC Driver\postgresql-42.7.12.jar"
    ) \
    .config(
        "spark.sql.warehouse.dir",
        "hdfs:///"
    ) \
    .config(
        "spark.hadoop.fs.defaultFS",
        "hdfs://localhost:9000"
    ) \
    .master("local[4]") \
    .enableHiveSupport() \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")



# показывает путь куда будет сохранять физически таблицы по умолчанию если Managed Table
warehouse = spark.conf.get("spark.sql.warehouse.dir")

print(
    "Warehouse:",
    warehouse.replace("hdfs://localhost:9000", "hdfs://")
)





#==================================START COD==================================================

spark.sql("CREATE DATABASE IF NOT EXISTS public")
spark.sql("USE public")

print("*"*80)

df: DataFrame = spark.read.csv(
    "hdfs:///data/raw/raw_customers.csv",
    header=True,
    inferSchema=True
)

# df.show()
# df.printSchema()

df.createOrReplaceTempView("tt")




validated_customers: DataFrame = spark.sql("""

SELECT

customer_id,
name,
amount,
year,
month,
CASE
    WHEN customer_id IS NULL THEN 'customer_id is NULL'
    WHEN name IS NULL THEN 'name is NULL'
    WHEN amount <= 0 THEN 'amount <= 0'
END 

as error_reason


from tt

""")






validated_customers.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "hdfs:///data/validated/") \
    .saveAsTable("validated_customers")



validated_customers.createOrReplaceTempView("tt1")

# spark.sql("""
#
# CREATE TABLE core_customers (
#     customer_id INT,
#     name STRING,
#     amount INT,
#     year INT,
#     month INT
# )
# USING csv
# LOCATION 'hdfs:///data/core/';
# """)

print(
    "Таблица core_customers существует в Spark Catalog:",
    spark.catalog.tableExists("core_customers"),'\n'
)

spark.sql("""
INSERT OVERWRITE core_customers (
    customer_id,
    name,
    amount,
    year,
    month
)
SELECT
    customer_id,
    name,
    amount,
    year,
    month
FROM tt1
WHERE error_reason IS NULL
""")

spark.sql("DROP TABLE error_customers")
print(
    "Таблица error_customers существует в Spark Catalog после DROP TABLE:",
    spark.catalog.tableExists("error_customers"),'\n'
)


spark.sql("""

CREATE TABLE error_customers (
    customer_id INT,
    name STRING,
    amount INT,
    year INT,
    month INT,
    error_reason string
)
USING csv
LOCATION 'hdfs:///data/error/';
""")

print(
    "Таблица error_customers существует в Spark Catalog после CREATE TABLE:",
    spark.catalog.tableExists("error_customers"),'\n'
)



spark.sql("""
INSERT OVERWRITE error_customers (
    customer_id,
    name,
    amount,
    year,
    month,
    error_reason
)
SELECT
    customer_id,
    name,
    amount,
    year,
    month,
    error_reason
FROM tt1
WHERE error_reason IS NOT NULL

""")

print(
    spark.sql("""
        SELECT *
        FROM error_customers
    """).toPandas(),'\n'
)


print(
    spark.sql("""
        SELECT *
        FROM core_customers
    """).toPandas(),'\n'
)



data_mart: DataFrame = spark.sql("""
SELECT

year,
month,
count(customer_id) as customers_count,
COALESCE(sum(amount),0) as total_amount

from core_customers

GROUP BY 
year,
month;

""")

print(data_mart.toPandas(),'\n')



df.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "hdfs:///data/mart/data_mart") \
    .saveAsTable("data_mart")


print(
    "Таблица data_mart существует в Spark Catalog после записи:",
    spark.catalog.tableExists("data_mart"),'\n'
)




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
~~~
