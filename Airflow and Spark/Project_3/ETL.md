~~~
#────────────────────────────────────────────
# SPARK + HDFS + ICEBERG + POSTGRESQL + SQLALCHEMY
#────────────────────────────────────────────

import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"


os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"


load_dotenv("/opt/airflow/.env", override=True)

pg_host = os.getenv("DB_HOST")
pg_port = os.getenv("DB_PORT")
pg_database = os.getenv("DB_NAME")
pg_user = os.getenv("DB_USER")
pg_password = os.getenv("DB_PASSWORD")

# JDBC
pg_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"
pg_driver = "org.postgresql.Driver"


engine = create_engine(
    f"postgresql+psycopg2://"
    f"{pg_user}:{pg_password}"
    f"@{pg_host}:{pg_port}/{pg_database}"
)


#============================================
# START CODE
#============================================

#----------def(1)
def read_database_and_create_table():
    spark = (
        SparkSession.builder
        .appName("ProductPipeline")
        .config(
            "spark.jars",
            "/opt/airflow/jars/postgresql-42.7.12.jar"
        )
        .master("local[4]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    df_customers = spark.read.csv(
        str(DATA_DIR / "customers.csv"),
        header=True,
        inferSchema=True
    )

    df_products = spark.read.csv(
        str(DATA_DIR / "products.csv"),
        header=True,
        inferSchema=True
    )

    with engine.begin() as conn:

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS raw.customers (
        customer_id INT PRIMARY KEY,
        name TEXT,
        city TEXT,
        email TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
        );
        
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS raw.products (
        product_id INT PRIMARY KEY,
        product_name TEXT,
        category TEXT,
        price NUMERIC(10,2),
        updated_at TIMESTAMP
        );
        
        """))

    df_customers.write \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", "raw.staging_customers") \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("driver", pg_driver) \
        .mode("overwrite") \
        .save()

    df_products.write \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", "raw.staging_products") \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("driver", pg_driver) \
        .mode("overwrite") \
        .save()

    with engine.begin() as conn:
        conn.execute(text("""
        MERGE INTO raw.customers AS t
        USING raw.staging_customers AS s
        ON s.customer_id = t.customer_id
        
        WHEN MATCHED THEN
        UPDATE SET
        name = s.name,
        city = s.city,
        email = s.email,
        created_at = s.created_at,
        updated_at = s.updated_at
        
        WHEN NOT MATCHED THEN
        INSERT (
        customer_id,
        name,
        city,
        email,
        created_at,
        updated_at
        )
        VALUES (
        s.customer_id,
        s.name,
        s.city,
        s.email,
        s.created_at,
        s.updated_at
        );
        
        """))

        conn.execute(text("""
        MERGE INTO raw.products AS t
        USING raw.staging_products AS s
        ON s.product_id = t.product_id
        
        WHEN MATCHED THEN
        UPDATE SET
        product_name = s.product_name,
        category = s.category,
        price = s.price,
        updated_at = s.updated_at
        
        WHEN NOT MATCHED THEN
        INSERT (
        product_id,
        product_name,
        category,
        price,
        updated_at
        )
        VALUES (
        s.product_id,
        s.product_name,
        s.category,
        s.price,
        s.updated_at
        );
        
        """))





#----------def(2)

def save_raw_hdfs():
    spark = (
        SparkSession.builder
        .appName("ProductPipeline")

        .config(
            "spark.jars",
            "/opt/airflow/jars/postgresql-42.7.12.jar"
        )

        # HDFS
        .config(
            "spark.hadoop.fs.defaultFS",
            "hdfs://host.docker.internal:9000"
        )
        .config(
            "spark.hadoop.dfs.client.use.datanode.hostname",
            "true"
        )

        .master("local[4]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")



    df_orders = spark.read.json(
        (DATA_DIR / "orders.json").as_uri(),
        multiLine=True
    )

    df_orders.createOrReplaceTempView("df_orders")

    df_orders_good = spark.sql("""
    SELECT
    customer_id,
    order_id,
    cast(order_ts as TIMESTAMP) as order_ts,
    product_id,
    quantity,
    status,
    unit_price,
    cast(updated_at as TIMESTAMP) as updated_at
    
    from df_orders;
    
    """)

    df_payments = spark.read.csv(
        (DATA_DIR / "payments.csv").as_uri(),
        header=True,
        inferSchema=True
    )

    df_customers = (
        spark.read
        .format("jdbc")
        .option("url", pg_url)
        .option("dbtable", "raw.customers")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", pg_driver)
        .load()
    )

    df_products = (
        spark.read
        .format("jdbc")
        .option("url", pg_url)
        .option("dbtable", "raw.products")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", pg_driver)
        .load()
    )

    df_customers.write \
        .format("csv") \
        .mode("overwrite") \
        .option("path", f"hdfs:///data/project_2/raw/raw_customers/") \
        .option("header", "true") \
        .saveAsTable("raw_customers")

    df_products.write \
        .format("csv") \
        .mode("overwrite") \
        .option("path", f"hdfs:///data/project_2/raw/raw_products/") \
        .option("header", "true") \
        .saveAsTable("raw_products")

    df_orders_good.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "hdfs:///data/project_2/raw/raw_orders/") \
        .saveAsTable("raw_orders")

    df_payments.write \
        .format("csv") \
        .mode("overwrite") \
        .option("path", f"hdfs:///data/project_2/raw/raw_payments/") \
        .option("header", "true") \
        .saveAsTable("raw_payments")





# #----------def(3)

def validated_table_and_save_hdfs():


    spark = (
        SparkSession.builder
        .appName("ProductPipeline")

        .config(
            "spark.jars",
            "/opt/airflow/jars/postgresql-42.7.12.jar"
        )

        # HDFS
        .config(
            "spark.hadoop.fs.defaultFS",
            "hdfs://host.docker.internal:9000"
        )
        .config(
            "spark.hadoop.dfs.client.use.datanode.hostname",
            "true"
        )

        .master("local[4]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    raw_customers = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("hdfs:///data/project_2/raw/raw_customers/")
    )

    raw_customers.createOrReplaceTempView("raw_customers")


    df_valid_customers = spark.sql("""
    
    SELECT
    customer_id,
    name,
    city,
    email,
    created_at,
    updated_at,
    
    case when customer_id is null then 'customer_id is null'
    
    when count(*) over(PARTITION BY customer_id) > 1 then 'customer_id is dub'
    
    when name is null then 'name is null'
    
    when email is null then 'email is null'
    
    when email not like '%@%' then 'email error format'
    
    when created_at is null then 'created_at is null'
    
    when updated_at is null then 'updated_at is null'
    
    when updated_at < created_at then 'updated_at < created_at'
    
    end as error_reason
    
    from raw_customers;
    
    """)

    df_valid_customers.write \
        .format("csv") \
        .mode("overwrite") \
        .option("path", f"hdfs:///data/project_2/validated/valid_customers/") \
        .option("header", "true") \
        .saveAsTable("valid_customers")



    raw_products = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("hdfs:///data/project_2/raw/raw_products/")
    )

    raw_products.createOrReplaceTempView("raw_products")


    df_valid_products = spark.sql("""
    SELECT
    product_id,
    product_name,
    category,
    price,
    updated_at,
    
    case when product_id is null then 'product_id is null'
    
    when count(*) over(PARTITION BY product_id) > 1 then 'product_id is dub'
    
    when product_name is null then 'product_name is null'
    
    when category is null then 'category is null'
    
    when price is null then 'price is null'
    
    when price < 0 then 'price <0'
    
    when updated_at is null then 'updated_at is null'
    
    end as error_reason
    
    from raw_products;
    
    """)



    df_valid_products.write \
        .format("csv") \
        .mode("overwrite") \
        .option("path", f"hdfs:///data/project_2/validated/valid_products/") \
        .option("header", "true") \
        .saveAsTable("valid_products")



    raw_orders = (
        spark.read
        .format("json")
        .option("multiLine", "true")
        .load("hdfs:///data/project_2/raw/raw_orders/")
    )

    raw_orders.createOrReplaceTempView("raw_orders")


    df_valid_orders = spark.sql("""
    
    SELECT
    customer_id,
    order_id,
    order_ts,
    product_id,
    quantity,
    status,
    unit_price,
    updated_at,
    
    case when order_id is null then 'order_id is null'
    
    when count(*) over(PARTITION BY order_id)>1  then 'order_id is dub'
    
    when customer_id is null then 'customer_id is null'
    
    when not EXISTS (select 1
    from raw_customers
    where raw_customers.customer_id = raw_orders.customer_id) then 'customer_id is orphan'
    
    when product_id is null then 'product_id is null'
    
    when not EXISTS (select 1
    from raw_products
    where raw_products.product_id = raw_orders.product_id) then 'product_id is orphan'
    
    when quantity is null then 'quantity is null'
    
    when quantity <= 0 then 'quantity <= 0'
    
    when unit_price is null then 'unit_price is null'
    
    when unit_price <0  then 'unit_price <0 '
    
    when status is null then 'status is null'
    
    when status not in ('completed' ) then 'status error format'
    
    when order_ts is null then 'order_ts is null'
    
    when updated_at is null then 'updated_at is null'
    
    end as error_reason
    
    
    from raw_orders;
    
    
    
    """)

    df_valid_orders.write \
        .format("csv") \
        .mode("overwrite") \
        .option("path", "hdfs:///data/project_2/validated/valid_orders/") \
        .option("header", "true") \
        .saveAsTable("valid_orders")




    raw_payments = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("hdfs:///data/project_2/raw/raw_payments/")
    )

    raw_payments.createOrReplaceTempView("raw_payments")


    df_valid_payments = spark.sql("""
    SELECT
    payment_id,
    order_id,
    payment_method,
    payment_status,
    amount,
    payment_ts,
    updated_at,
    
    case when payment_id is null then 'payment_id is null'
    
    when count(*) over(PARTITION BY payment_id) > 1 then 'payment_id is dub'
    
    when order_id is null then 'order_id is null'
    
    when not EXISTS (select 1
    from raw_orders
    where raw_orders.order_id =raw_payments.order_id ) then 'order_id is orphan'
    
    when payment_method is null then 'payment_method is null'
    
    when payment_method not in ('card','sbp','transfer') then 'payment_method error format'
    
    when payment_status is null then 'payment_status is null'
    
    when payment_status not in ('paid') then 'payment_status error format'
    
    when amount is null then 'amount is null'
    
    when amount <0 then 'amount <0 '
    
    when payment_ts is null then 'payment_ts is null'
    
    when updated_at is null then 'updated_at is null'
    
    when (
    select quantity * unit_price
    from raw_orders
    where raw_orders.order_id = raw_payments.order_id
    ) <> raw_payments.amount
    then 'amount error quantity × unit_price'
    
    end as error_reason
    
    from raw_payments;
    
    """)

    df_valid_payments.write \
        .format("csv") \
        .mode("overwrite") \
        .option("path", f"hdfs:///data/project_2/validated/valid_payments/") \
        .option("header", "true") \
        .saveAsTable("valid_payments")




#
# #----------def(4)

def error_table_hdfs():

    spark = (
        SparkSession.builder
        .appName("ProductPipeline")

        .config(
            "spark.jars",
            "/opt/airflow/jars/postgresql-42.7.12.jar"
        )

        # HDFS
        .config(
            "spark.hadoop.fs.defaultFS",
            "hdfs://host.docker.internal:9000"
        )
        .config(
            "spark.hadoop.dfs.client.use.datanode.hostname",
            "true"
        )

        .master("local[4]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    valid_customers = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("hdfs:///data/project_2/validated/valid_customers")
    )

    valid_customers.createOrReplaceTempView("valid_customers")

    df_error_customers = spark.sql("""
    
    SELECT
    customer_id,
    name,
    city,
    email,
    created_at,
    updated_at,
    error_reason
    
    from valid_customers
    
    where error_reason is not null;
    
    """)

    valid_products = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("hdfs:///data/project_2/validated/valid_products")
    )

    valid_products.createOrReplaceTempView("valid_products")

    df_error_products = spark.sql("""
    SELECT
    product_id,
    product_name,
    category,
    price,
    updated_at,
    error_reason
    
    from valid_products
    
    where error_reason is not null;
    
    """)

    valid_orders = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("hdfs:///data/project_2/validated/valid_orders")
    )


    valid_orders.createOrReplaceTempView("valid_orders")

    df_error_orders = spark.sql("""
    SELECT
    customer_id,
    order_id,
    order_ts,
    product_id,
    quantity,
    status,
    unit_price,
    updated_at,
    error_reason
    
    from valid_orders
    
    where error_reason is not null;
    
    """)

    valid_payments = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("hdfs:///data/project_2/validated/valid_payments")
    )

    valid_payments.createOrReplaceTempView("valid_payments")

    df_error_payments = spark.sql("""
    SELECT
    payment_id,
    order_id,
    payment_method,
    payment_status,
    amount,
    payment_ts,
    updated_at,
    error_reason
    
    from valid_payments
    
    where error_reason is not null;
    
    
    """)

    df_error_customers.write \
        .format("csv") \
        .mode("overwrite") \
        .option("path", f"hdfs:///data/project_2/error/error_customers/") \
        .option("header", "true") \
        .saveAsTable("error_customers")

    df_error_products.write \
        .format("csv") \
        .mode("overwrite") \
        .option("path", f"hdfs:///data/project_2/error/error_products/") \
        .option("header", "true") \
        .saveAsTable("error_products")

    df_error_orders.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "hdfs:///data/project_2/error/error_orders/") \
        .saveAsTable("error_orders")

    df_error_payments.write \
        .format("csv") \
        .mode("overwrite") \
        .option("path", f"hdfs:///data/project_2/error/error_payments/") \
        .option("header", "true") \
        .saveAsTable("error_payments")








#
# #----------def(5)

def core_today_and_iceberg_table():
    spark = (
        SparkSession.builder
        .appName("ProductPipeline")

        # PostgreSQL JDBC
        .config(
            "spark.jars",
            "/opt/airflow/jars/postgresql-42.7.12.jar"
        )

        # Iceberg runtime
        .config(
            "spark.jars.packages",
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
            "hdfs:///data/project_2/core/"
        )

        # HDFS
        .config(
            "spark.hadoop.fs.defaultFS",
            "hdfs://host.docker.internal:9000"
        )
        .config(
            "spark.hadoop.dfs.client.use.datanode.hostname",
            "true"
        )

        .master("local[4]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")


    valid_customers = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("hdfs:///data/project_2/validated/valid_customers")
        )

    valid_customers.createOrReplaceTempView("valid_customers")

    df_core_customers_today = spark.sql("""
    SELECT
    customer_id,
    name,
    city,
    email,
    created_at,
    updated_at
    
    
    from valid_customers
    
    where error_reason is null;
    
    """)

    valid_products = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("hdfs:///data/project_2/validated/valid_products")
    )

    valid_products.createOrReplaceTempView("valid_products")

    df_core_products_today = spark.sql("""
    SELECT
    product_id,
    product_name,
    category,
    price,
    updated_at
    
    
    from valid_products
    
    where error_reason is null;
    
    """)

    valid_orders = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("hdfs:///data/project_2/validated/valid_orders")
    )

    valid_orders.createOrReplaceTempView("valid_orders")

    df_core_orders_today = spark.sql("""
    SELECT
    customer_id,
    order_id,
    order_ts,
    product_id,
    quantity,
    status,
    unit_price,
    updated_at
    
    
    from valid_orders
    
    where error_reason is null;
    
    """)

    valid_payments = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("hdfs:///data/project_2/validated/valid_payments")
    )

    valid_payments.createOrReplaceTempView("valid_payments")

    df_core_payments_today = spark.sql("""
    SELECT
    payment_id,
    order_id,
    payment_method,
    payment_status,
    amount,
    payment_ts,
    updated_at
    
    
    from valid_payments
    
    where error_reason is null;
    
    """)

    spark.sql("""
    CREATE table if not EXISTS iceberg.core_customers (
    customer_id int,
    name string,
    city string,
    email string,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
    )
    
    USING iceberg;
    
    
    """)

    df_core_customers_today.createOrReplaceTempView("df_core_customers_today")

    spark.sql("""
    
    merge into iceberg.core_customers as t
    USING df_core_customers_today as s
    on s.customer_id = t.customer_id
    
    when MATCHed then
    UPDATE SET
    t.name = s.name,
    t.city = s.city,
    t.email = s.email,
    t.created_at = s.created_at,
    t.updated_at = s.updated_at
    
    when not MATCHed THEN
    INSERT (
    customer_id,
    name,
    city,
    email,
    created_at,
    updated_at
    )
    VALUES (
    s.customer_id,
    s.name,
    s.city,
    s.email,
    s.created_at,
    s.updated_at
    );
    
    
    
    """)

    # spark.sql("SELECT * FROM iceberg.core_customers").show()

    spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.core_products (
    product_id INT,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    updated_at TIMESTAMP
    )
    USING iceberg
    
    """)

    df_core_products_today.createOrReplaceTempView("df_core_products_today")

    spark.sql("""
    
    MERGE INTO iceberg.core_products AS t
    USING df_core_products_today AS s
    ON s.product_id = t.product_id
    
    WHEN MATCHED THEN
    UPDATE SET
    t.product_name = s.product_name,
    t.category = s.category,
    t.price = s.price,
    t.updated_at = s.updated_at
    
    WHEN NOT MATCHED THEN
    INSERT (
    product_id,
    product_name,
    category,
    price,
    updated_at
    )
    VALUES (
    s.product_id,
    s.product_name,
    s.category,
    s.price,
    s.updated_at
    )
    
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.core_orders (
    customer_id BIGINT,
    order_id BIGINT,
    order_ts TIMESTAMP,
    product_id BIGINT,
    quantity BIGINT,
    status STRING,
    unit_price DECIMAL(10,2),
    updated_at TIMESTAMP
    )
    USING iceberg
    """)

    df_core_orders_today.createOrReplaceTempView("df_core_orders_today")

    spark.sql("""
    MERGE INTO iceberg.core_orders AS t
    USING df_core_orders_today AS s
    ON s.order_id = t.order_id
    
    WHEN MATCHED THEN
    UPDATE SET
    t.customer_id = s.customer_id,
    t.order_ts = s.order_ts,
    t.product_id = s.product_id,
    t.quantity = s.quantity,
    t.status = s.status,
    t.unit_price = s.unit_price,
    t.updated_at = s.updated_at
    
    WHEN NOT MATCHED THEN
    INSERT (
    customer_id,
    order_id,
    order_ts,
    product_id,
    quantity,
    status,
    unit_price,
    updated_at
    )
    VALUES (
    s.customer_id,
    s.order_id,
    s.order_ts,
    s.product_id,
    s.quantity,
    s.status,
    s.unit_price,
    s.updated_at
    )
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.core_payments (
    payment_id BIGINT,
    order_id BIGINT,
    payment_method STRING,
    payment_status STRING,
    amount DECIMAL(10,2),
    payment_ts TIMESTAMP,
    updated_at TIMESTAMP
    )
    USING iceberg
    """)

    df_core_payments_today.createOrReplaceTempView("df_core_payments_today")

    spark.sql("""
    MERGE INTO iceberg.core_payments AS t
    USING df_core_payments_today AS s
    ON s.payment_id = t.payment_id
    
    WHEN MATCHED THEN
    UPDATE SET
    t.order_id = s.order_id,
    t.payment_method = s.payment_method,
    t.payment_status = s.payment_status,
    t.amount = s.amount,
    t.payment_ts = s.payment_ts,
    t.updated_at = s.updated_at
    
    WHEN NOT MATCHED THEN
    INSERT (
    payment_id,
    order_id,
    payment_method,
    payment_status,
    amount,
    payment_ts,
    updated_at
    )
    VALUES (
    s.payment_id,
    s.order_id,
    s.payment_method,
    s.payment_status,
    s.amount,
    s.payment_ts,
    s.updated_at
    )
    """)




# #----------def(6)

def create_view_data_mart():
    spark = (
        SparkSession.builder
        .appName("ProductPipeline")

        # PostgreSQL JDBC
        .config(
            "spark.jars",
            "/opt/airflow/jars/postgresql-42.7.12.jar"
        )

        # Iceberg runtime
        .config(
            "spark.jars.packages",
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
            "hdfs:///data/project_2/core/"
        )

        # HDFS
        .config(
            "spark.hadoop.fs.defaultFS",
            "hdfs://host.docker.internal:9000"
        )
        .config(
            "spark.hadoop.dfs.client.use.datanode.hostname",
            "true"
        )

        .master("local[4]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")


    spark.sql("""
    CREATE OR REPLACE VIEW mart_sales_daily_view AS
    
    SELECT
    DATE(o.order_ts) AS date_order_ts,
    
    COUNT(DISTINCT o.order_id) AS orders_count,
    
    COALESCE(SUM(o.quantity), 0) AS items_count,
    
    COALESCE(SUM(o.quantity * o.unit_price), 0) AS revenue,
    
    COALESCE(SUM(p.paid_amount), 0) AS paid_amount,
    
    ROUND(
    COALESCE(SUM(o.quantity * o.unit_price), 0)
    / COUNT(DISTINCT o.order_id),
    2
    ) AS average_order_value
    
    FROM iceberg.core_orders AS o
    
    LEFT JOIN (
    SELECT
    order_id,
    SUM(amount) AS paid_amount
    FROM iceberg.core_payments
    GROUP BY order_id
    ) AS p
    ON p.order_id = o.order_id
    
    GROUP BY DATE(o.order_ts);
    
    
    """)

    mart_sales_daily_view = spark.table("mart_sales_daily_view")

    mart_sales_daily_view.write \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", "mart.mart_sales_daily_view") \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("driver", pg_driver) \
        .mode("overwrite") \
        .save()

    spark.sql("""
    CREATE OR REPLACE VIEW mart_customer_sales_view AS
    
    SELECT
    customer_id,
    
    COUNT(DISTINCT order_id) AS orders_count,
    
    COALESCE(SUM(quantity), 0) AS items_count,
    
    COALESCE(SUM(quantity * unit_price), 0) AS revenue,
    
    COALESCE(SUM(quantity * unit_price), 0)
    / NULLIF(COUNT(DISTINCT order_id), 0)
    AS average_order_value
    
    FROM iceberg.core_orders
    
    GROUP BY customer_id;
    
    """)

    mart_customer_sales_view = spark.table("mart_customer_sales_view")

    mart_customer_sales_view.write \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", "mart.mart_customer_sales_view") \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("driver", pg_driver) \
        .mode("overwrite") \
        .save()

    spark.sql("""
    
    CREATE OR REPLACE VIEW mart_product_sales_view AS
    
    SELECT
    product_id,
    
    COUNT(DISTINCT order_id) AS orders_count,
    
    SUM(quantity) AS items_count,
    
    SUM(quantity * unit_price) AS revenue
    
    FROM iceberg.core_orders
    
    GROUP BY product_id;
    
    """)

    mart_product_sales_view = spark.table("mart_product_sales_view")

    mart_product_sales_view.write \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", "mart.mart_product_sales_view") \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("driver", pg_driver) \
        .mode("overwrite") \
        .save()


~~~
