~~~
# ────────────────────────────────────────────
# HDFS + SPARK + GREENPLUM + ICEBERG
# ────────────────────────────────────────────

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession


# ────────────────────────────────────────────
# PYSPARK
# ────────────────────────────────────────────

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"


# ────────────────────────────────────────────
# GREENPLUM
# ────────────────────────────────────────────

load_dotenv(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\Greenplum_config.env"
)

gp_host = os.getenv("GP_HOST")
gp_port = os.getenv("GP_PORT")
gp_database = os.getenv("GP_NAME")
gp_user = os.getenv("GP_USER")
gp_password = os.getenv("GP_PASSWORD")

gp_url = (
    f"jdbc:postgresql://"
    f"{gp_host}:{gp_port}/{gp_database}"
)

gp_driver = "org.postgresql.Driver"


# ────────────────────────────────────────────
# HDFS
# ────────────────────────────────────────────

hdfs_name_node = "hdfs://localhost:9000"

hdfs_raw_path = "hdfs:///data/raw"
hdfs_staging_path = "hdfs:///data/staging"
hdfs_core_path = "hdfs:///data/core"
hdfs_dwh_path = "hdfs:///data/dwh"


# ────────────────────────────────────────────
# SPARK
# ────────────────────────────────────────────

spark = (
    SparkSession.builder

    .appName("Spark_HDFS_Greenplum_Iceberg")

    # ───────────── HDFS ─────────────

    .config(
        "spark.hadoop.fs.defaultFS",
        hdfs_name_node
    )
    .config(
        "spark.hadoop.dfs.client.use.datanode.hostname",
        "true"
    )

    # ───────────── Greenplum / JDBC ─────────────

    .config(
        "spark.jars",
        r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\jars\PostgreSQL JDBC Driver\postgresql-42.7.12.jar"
    )

    # ───────────── ICEBERG ─────────────

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
        "hdfs:///data/project_1/core/"
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

print("Greenplum:", gp_url)

print("HDFS NameNode:", hdfs_name_node)
print("HDFS RAW:", hdfs_raw_path)
print("HDFS STAGING:", hdfs_staging_path)
print("HDFS CORE:", hdfs_core_path)
print("HDFS DWH:", hdfs_dwh_path)

print("Iceberg warehouse:", "hdfs:///data/project_1/core/")

print("Spark:", spark.version)

#------------------------START PIPELINE-----------------------------

customers_path = "file:///C:/Users/VladK/OneDrive/Desktop/proj/ver10/data/customers.csv"
products_path = "file:///C:/Users/VladK/OneDrive/Desktop/proj/ver10/data/products.csv"
orders_path = "file:///C:/Users/VladK/OneDrive/Desktop/proj/ver10/data/orders.csv"




df_customers = spark.read.csv(
    customers_path,
    header=True,
    inferSchema=True
)

df_customers.show(2)
df_customers.printSchema()


df_orders = spark.read.csv(
    orders_path,
    header=True,
    inferSchema=True
)

df_orders.show(2)
df_orders.printSchema()


df_products = spark.read.csv(
    products_path,
    header=True,
    inferSchema=True
)

df_products.show(2)
df_products.printSchema()


df_customers.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"hdfs:///data/project_1/raw/raw_customers") \
     .option("header", "true") \
    .saveAsTable("raw_customers")



df_orders.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"hdfs:///data/project_1/raw/raw_orders") \
     .option("header", "true") \
    .saveAsTable("raw_orders")




df_products.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"hdfs:///data/project_1/raw/raw_products") \
     .option("header", "true") \
    .saveAsTable("raw_products")


df_customers.createOrReplaceTempView("df_customers")
df_orders.createOrReplaceTempView("df_orders")
df_products.createOrReplaceTempView("df_products")



df_valid_customers = spark.sql("""

SELECT
    customer_id,
    first_name,
    last_name,
    city,

    CASE
        WHEN customer_id IS NULL THEN 'customer_id is null'
        WHEN customer_id <= 0 THEN 'customer_id <= 0'
        WHEN first_name IS NULL THEN 'first_name is null'
        WHEN last_name IS NULL THEN 'last_name is null'
        WHEN city IS NULL THEN 'city is null'
        WHEN COUNT(*) OVER (
            PARTITION BY customer_id
        ) > 1 THEN 'customer_id is dub'
    END AS error_reason

FROM df_customers;

""")




df_valid_products  =spark.sql("""

SELECT
    product_id,
    product_name,
    category,
    price,

    CASE
        WHEN product_id IS NULL THEN 'product_id is null'
        WHEN product_id <= 0 THEN 'product_id <= 0'
        WHEN product_name IS NULL THEN 'product_name is null'
        WHEN category IS NULL THEN 'category is null'
        WHEN price < 0 THEN 'price < 0'
        WHEN COUNT(*) OVER (
            PARTITION BY product_id
        ) > 1 THEN 'product_id is dub'
    END AS error_reason

FROM df_products;

""")





df_valid_orders = spark.sql("""

SELECT
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    amount,
    status,

    CASE
        WHEN order_id IS NULL THEN 'order_id is null'
        WHEN order_id <= 0 THEN 'order_id <= 0'

        WHEN customer_id IS NULL THEN 'customer_id is null'
        WHEN customer_id <= 0 THEN 'customer_id <= 0'

        WHEN product_id IS NULL THEN 'product_id is null'
        WHEN product_id <= 0 THEN 'product_id <= 0'

        WHEN order_date IS NULL THEN 'order_date is null'

        WHEN quantity <= 0 THEN 'quantity <= 0'

        WHEN amount < 0 THEN 'amount < 0'

        WHEN status NOT IN (
            'completed',
            'pending',
            'cancelled'
        )
        THEN 'status is invalid'

        WHEN NOT EXISTS (
            SELECT 1
            FROM df_customers
            WHERE df_orders.customer_id = df_customers.customer_id
        )
        THEN 'customer_id is orphan'

        WHEN NOT EXISTS (
            SELECT 1
            FROM df_products
            WHERE df_orders.product_id = df_products.product_id
        )
        THEN 'product_id is orphan'
        
        when COUNT(*) OVER (
                PARTITION BY order_id
            ) > 1 then 'order_id is dub'

    END AS error_reason

FROM df_orders;

""")


df_valid_customers.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"hdfs:///data/project_1/validated/validated_customers/") \
     .option("header", "true") \
    .saveAsTable("validated_customers")


df_valid_orders.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"hdfs:///data/project_1/validated/validated_orders/") \
     .option("header", "true") \
    .saveAsTable("validated_orders")



df_valid_products.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"hdfs:///data/project_1/validated/validated_products/") \
     .option("header", "true") \
    .saveAsTable("validated_products")


df_valid_customers.createOrReplaceTempView("df_valid_customers")
df_valid_orders.createOrReplaceTempView("df_valid_orders")
df_valid_products.createOrReplaceTempView("df_valid_products")






df_error_customers = spark.sql("""
SELECT
customer_id,
first_name,
last_name,
city,
error_reason



from df_valid_customers

where error_reason is not null;

""")




df_error_products = spark.sql("""
SELECT
product_id,
product_name,
category,
price,
error_reason



from df_valid_products

where error_reason is not null;

""")




df_error_orders = spark.sql("""
SELECT
order_id,
customer_id,
product_id,
order_date,
quantity,
amount,
status,
error_reason



from df_valid_orders

where error_reason is not null;

""")



df_error_customers.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"hdfs:///data/project_1/error/error_customers/") \
     .option("header", "true") \
    .saveAsTable("error_customers")


df_error_orders.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"hdfs:///data/project_1/error/error_orders/") \
     .option("header", "true") \
    .saveAsTable("error_orders")


df_error_products.write \
    .format("csv") \
    .mode("overwrite") \
    .option("path", f"hdfs:///data/project_1/error/error_products/") \
     .option("header", "true") \
    .saveAsTable("error_products")







df_core_customers_today = spark.sql("""

SELECT
customer_id,
first_name,
last_name,
city




from df_valid_customers

where error_reason is null;

""")




df_core_products_today = spark.sql("""

SELECT
product_id,
product_name,
category,
price




from df_valid_products

where error_reason is null;


""")





df_core_orders_today = spark.sql("""
SELECT
order_id,
customer_id,
product_id,
order_date,
quantity,
amount,
status




from df_valid_orders

where error_reason is null;

""")




spark.sql("""
CREATE table if not EXISTS iceberg.core_customers (
    customer_id int,
    first_name string,
    last_name string,
    city string
)

USING iceberg


""")




spark.sql("""
CREATE table if not EXISTS iceberg.core_products (
    product_id int,
    product_name string,
    category string,
    price DECIMAL(10,2)
)

USING iceberg


""")





spark.sql("""
CREATE table if not EXISTS iceberg.core_orders (
    order_id int,
    customer_id int,
    product_id int,
    order_date date,
    quantity int,
    amount DECIMAL(10,2),
    status STRING
)

USING iceberg

""")



df_core_customers_today.createOrReplaceTempView("df_core_customers_today")

spark.sql("""
MERGE INTO iceberg.core_customers AS t
USING df_core_customers_today AS s
ON t.customer_id = s.customer_id

WHEN MATCHED THEN
    UPDATE SET
        t.first_name = s.first_name,
        t.last_name = s.last_name,
        t.city = s.city

WHEN NOT MATCHED THEN
    INSERT (
        customer_id,
        first_name,
        last_name,
        city
    )
    VALUES (
        s.customer_id,
        s.first_name,
        s.last_name,
        s.city
    )
""")



df_core_products_today.createOrReplaceTempView(
    "df_core_products_today"
)



spark.sql("""
MERGE INTO iceberg.core_products AS t
USING df_core_products_today AS s
ON t.product_id = s.product_id

WHEN MATCHED THEN
    UPDATE SET
        t.product_name = s.product_name,
        t.category = s.category,
        t.price = s.price

WHEN NOT MATCHED THEN
    INSERT (
        product_id,
        product_name,
        category,
        price
    )
    VALUES (
        s.product_id,
        s.product_name,
        s.category,
        s.price
    )
""")



df_core_orders_today.createOrReplaceTempView(
    "df_core_orders_today"
)


spark.sql("""
MERGE INTO iceberg.core_orders AS t
USING df_core_orders_today AS s
ON t.order_id = s.order_id

WHEN MATCHED THEN
    UPDATE SET
        t.customer_id = s.customer_id,
        t.product_id = s.product_id,
        t.order_date = s.order_date,
        t.quantity = s.quantity,
        t.amount = s.amount,
        t.status = s.status

WHEN NOT MATCHED THEN
    INSERT (
        order_id,
        customer_id,
        product_id,
        order_date,
        quantity,
        amount,
        status
    )
    VALUES (
        s.order_id,
        s.customer_id,
        s.product_id,
        s.order_date,
        s.quantity,
        s.amount,
        s.status
    )
""")


df_customers_iceberg = spark.sql(""" 
    SELECT *
    FROM iceberg.core_customers
""")

df_customers_iceberg.show()

df_products_iceberg = spark.sql(""" 
    SELECT *
    FROM iceberg.core_products
""")

df_products_iceberg.show()

df_orders_iceberg = spark.sql(""" 
    SELECT *
    FROM iceberg.core_orders
""")

df_orders_iceberg.show()




import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv("Greenplum_config.env")

GP_USER = os.getenv("GP_USER")
GP_PASSWORD = os.getenv("GP_PASSWORD")
GP_HOST = os.getenv("GP_HOST")
GP_PORT = os.getenv("GP_PORT")
GP_DB = os.getenv("GP_NAME")

engine = create_engine(
    f"postgresql+psycopg2://"
    f"{GP_USER}:{GP_PASSWORD}"
    f"@{GP_HOST}:{GP_PORT}/{GP_DB}"
)




with engine.begin() as conn:



    conn.execute(text("""
    CREATE SCHEMA if not exists dwh;

    """))

    conn.execute(text("""
    CREATE table if not EXISTS dwh.dwh_customers (
    customer_id int PRIMARY KEY,
    first_name text,
    last_name text,
    city text
    );
    
        """))

    conn.execute(text("""
    CREATE table if not EXISTS dwh.dwh_products (
    product_id int PRIMARY KEY,
    product_name text,
    category text,
    price NUMERIC(10,2)
    );
    
        """))

    conn.execute(text("""
    CREATE table if not EXISTS dwh.dwh_orders (
    order_id int PRIMARY KEY,
    customer_id int,
    product_id int,
    order_date date,
    quantity int,
    amount NUMERIC(10,2),
    status text
    );
    
        """))


df_dwh_customers = spark.sql("""
    SELECT
        customer_id,
        first_name, 
        last_name,
        city
    FROM iceberg.core_customers
""")


pdf_dwh_customers = df_dwh_customers.toPandas()


from sqlalchemy import text

rows = pdf_dwh_customers.to_dict(orient="records")

with engine.begin() as conn:
    conn.execute(
        text("""
            INSERT INTO dwh.dwh_customers (
                customer_id,
                first_name,
                last_name,
                city
            )
            VALUES (
                :customer_id,
                :first_name,
                :last_name,
                :city
            )
            ON CONFLICT (customer_id)
            DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                city = EXCLUDED.city
        """),
        rows
    )



df_core_products  = spark.sql(""" 
    SELECT *
    FROM iceberg.core_products
""")



pandas_df = df_core_products.toPandas()


rows = pandas_df.to_dict(orient="records")

with engine.begin() as conn:
    conn.execute(
        text("""
            INSERT INTO dwh.dwh_products (
                product_id, 
                product_name, 
                category, 
                price 
            )
            VALUES (
                :product_id,
                :product_name,
                :category,
                :price
            )
            ON CONFLICT (product_id)
            DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category,
                price = EXCLUDED.price
        """),
        rows
    )




# ────────────────────────────────────────────
# ICEBERG → SPARK DATAFRAME
# ────────────────────────────────────────────

df_iceberg_core_orders = spark.sql("""
    SELECT
        *
    FROM iceberg.core_orders
""")


# ────────────────────────────────────────────
# STAGING TABLE
# ────────────────────────────────────────────

with engine.begin() as conn:

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dwh.staging_orders (
            order_id INT,
            customer_id INT,
            product_id INT,
            order_date DATE,
            quantity INT,
            amount NUMERIC(10,2),
            status TEXT
        )
    """))


# ────────────────────────────────────────────
# SPARK → GREENPLUM STAGING
# ────────────────────────────────────────────

df_iceberg_core_orders.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "dwh.staging_orders") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()


# ────────────────────────────────────────────
# STAGING → DWH
# ────────────────────────────────────────────

with engine.begin() as conn:

    # UPDATE существующих заказов
    conn.execute(text("""
        UPDATE dwh.dwh_orders AS target
        SET
            customer_id = staging.customer_id,
            product_id = staging.product_id,
            order_date = staging.order_date,
            quantity = staging.quantity,
            amount = staging.amount,
            status = staging.status
        FROM dwh.staging_orders AS staging
        WHERE target.order_id = staging.order_id
    """))

    # INSERT новых заказов
    conn.execute(text("""
        INSERT INTO dwh.dwh_orders (
            order_id,
            customer_id,
            product_id,
            order_date,
            quantity,
            amount,
            status
        )
        SELECT
            staging.order_id,
            staging.customer_id,
            staging.product_id,
            staging.order_date,
            staging.quantity,
            staging.amount,
            staging.status
        FROM dwh.staging_orders AS staging
        LEFT JOIN dwh.dwh_orders AS target
            ON target.order_id = staging.order_id
        WHERE target.order_id IS NULL
    """))

    conn.execute(text("""
    TRUNCATE TABLE dwh.staging_orders

       """))





with engine.begin() as conn:


    conn.execute(text("""
         DROP VIEW IF EXISTS dwh.dm_customer_sales
       """))


    conn.execute(text("""
        CREATE view dwh.dm_customer_sales AS

        SELECT
            c.customer_id,
            COUNT(o.order_id)  AS orders_count,
            COUNT(CASE WHEN o.status = 'completed' THEN 1 END) AS completed_orders,
            COALESCE(SUM(o.amount), 0) AS total_amount
        FROM dwh.dwh_customers AS c
        LEFT JOIN dwh.dwh_orders AS o
            ON o.customer_id = c.customer_id
        GROUP BY c.customer_id;

    """))


#
# # 1. Проверяем последние snapshots
# spark.sql("""
#     SELECT
#         committed_at,
#         snapshot_id,
#         parent_id,
#         operation
#     FROM iceberg.core_customers.snapshots
#     ORDER BY committed_at DESC
#     LIMIT 5
# """).show(truncate=False)
#
#
# # 2. Проверяем текущий snapshot
# spark.sql("""
#     SELECT
#         made_current_at,
#         snapshot_id,
#         parent_id
#     FROM iceberg.core_customers.history
#     ORDER BY made_current_at DESC
#     LIMIT 1
# """).show(truncate=False)
#
#
# # 3. Проверяем количество строк
# spark.sql("""
#     SELECT COUNT(*) AS cnt
#     FROM iceberg.core_customers
# """).show()
#
#
# # 4. Удаляем старые snapshots
# spark.sql("""
# CALL iceberg.system.expire_snapshots(
#     table => 'core_customers',
#     retain_last => 2
# )
# """).show(truncate=False)
#
#
# # 5. Сначала проверяем orphan-файлы
# spark.sql("""
# CALL iceberg.system.remove_orphan_files(
#     table => 'core_customers',
#     dry_run => true
# )
# """).show(truncate=False)
#
#
# # 6. Удаляем orphan-файлы
# spark.sql("""
# CALL iceberg.system.remove_orphan_files(
#     table => 'core_customers'
# )
# """).show(truncate=False)

~~~
