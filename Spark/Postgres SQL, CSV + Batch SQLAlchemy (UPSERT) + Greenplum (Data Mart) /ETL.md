~~~
# ────────────────────────────────────────────
# SPARK + POSTGRESQL + GREENPLUM
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
# ENV — POSTGRESQL
# ────────────────────────────────────────────

load_dotenv(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\1.env"
)


# ────────────────────────────────────────────
# ENV — GREENPLUM
# ────────────────────────────────────────────

load_dotenv(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\Greenplum_config.env"
)


# ────────────────────────────────────────────
# POSTGRESQL
# ────────────────────────────────────────────

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
# GREENPLUM
# ────────────────────────────────────────────

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
# SPARK
# ────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("AdvertisingEvents")

    # PostgreSQL / Greenplum JDBC Driver
    .config(
        "spark.jars",
        r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\jars\PostgreSQL JDBC Driver\postgresql-42.7.12.jar"
    )

    .master("local[4]")

    .getOrCreate()
)


# ────────────────────────────────────────────
# LOG LEVEL
# ────────────────────────────────────────────

spark.sparkContext.setLogLevel("ERROR")


# ────────────────────────────────────────────
# CHECK CONNECTION SETTINGS
# ────────────────────────────────────────────

print("PostgreSQL:", pg_url)
print("Greenplum:", gp_url)
print("Spark:", spark.version)
spark.sparkContext.setLogLevel("ERROR")

#----------------------------------------------START Pipeline---------------

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\Greenplum_config.env")

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


#-----------------------------------------------------------------



df_orders = spark.read.csv(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\orders.csv",
    header=True,
    inferSchema=True
)

df_orders.show()

df_orders.printSchema()



df_customers = (
    spark.read
    .format("jdbc")
    .option("url", pg_url)
    .option("dbtable", "customers")
    .option("user", pg_user)
    .option("password", pg_password)
    .option("driver", pg_driver)
    .load()
)

df_customers.show()

df_customers.printSchema()


df_customers.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "raw.raw_customers ") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()


df_orders.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "raw.raw_orders ") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()



df_customers.createOrReplaceTempView("df_customers")
df_orders.createOrReplaceTempView("df_orders")


df_valid_customers = spark.sql("""
    SELECT
        customer_id,
        name,
        email,
        created_at,

        CASE
            WHEN customer_id IS NULL
                THEN 'customer_id is null'

            WHEN name IS NULL
                THEN 'name is null'

            WHEN email IS NULL
                THEN 'email is null'

            WHEN email NOT LIKE '%@%'
                THEN 'email error format'

            WHEN created_at IS NULL
                THEN 'created_at is null'

            WHEN COUNT(*) OVER (
                PARTITION BY customer_id
            ) > 1
                THEN 'customer_id is dub'

        END AS error_reason

    FROM df_customers
""")

df_valid_customers.show(truncate=False)


df_valid_customers.show()


df_valid_customers.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "raw.validated_customers") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()


df_valid_customers.createOrReplaceTempView("df_valid_customers")

df_valid_orders = spark.sql("""
    SELECT
        order_id,
        customer_id,
        order_date, 
        amount,
        status,

        CASE
            WHEN order_id IS NULL
                THEN 'order_id is null'

            WHEN COUNT(*) OVER (
                PARTITION BY order_id
            ) > 1
                THEN 'order_id is dub'

            WHEN customer_id IS NULL
                THEN 'customer_id is null'

            WHEN order_date IS NULL
                THEN 'order_date is null'

            WHEN amount IS NULL
                THEN 'amount is null'

            WHEN amount < 0
                THEN 'amount <0'

            WHEN status NOT IN ('completed', 'cancelled')
                THEN 'status error format'

            WHEN order_date > CURRENT_TIMESTAMP()
                THEN 'order_date is future'
                
WHEN NOT EXISTS (
    SELECT 1
    FROM df_valid_customers vc
    WHERE df_orders.customer_id = vc.customer_id
)
    THEN 'customer_id is orphan'
 
        END AS error_reason

    FROM df_orders
    
""")


df_valid_orders.show()



df_valid_orders.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "raw.validated_orders") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()



df_valid_orders.createOrReplaceTempView("df_valid_orders")

df_error_orders = spark.sql("""
 SELECT
order_id,
customer_id,
order_date,
amount,
status,
error_reason

 from df_valid_orders
 where error_reason is not null;

""")

df_error_orders.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "raw.error_orders") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()

df_error_orders.show()




df_valid_customers.createOrReplaceTempView("df_valid_customers")

df_error_customers = spark.sql("""
 SELECT
customer_id,
name,
email,
created_at,
error_reason

 from df_valid_customers
 where error_reason is not null; 

""")

df_error_customers.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "raw.error_customers") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()

df_error_customers.show()




core_customers_stage = spark.sql("""
SELECT
customer_id,
name,
email,
created_at
 
from df_valid_customers

where error_reason is null;

""")


# тут спецаильно делаем core.core_customers_stage потому что позже будет FOREIGN KEY у core_orders и будет ссылаться на таблицу core_customers и он будет ругаться.

#техничская таблица

core_customers_stage.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "core.core_customers_stage") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()


with engine.begin() as conn:


    conn.execute(text("""
          CREATE TABLE IF NOT EXISTS core.core_customers (
              customer_id INT,
              name TEXT,
              email TEXT,
              created_at TIMESTAMP
          )
         """))


    # UPDATE существующих
    conn.execute(text("""
        UPDATE core.core_customers c
        SET
            name = s.name,
            email = s.email,
            created_at = s.created_at
        FROM core.core_customers_stage s
        WHERE c.customer_id = s.customer_id
    """))

    # INSERT новых
    conn.execute(text("""
           INSERT INTO core.core_customers (
               customer_id,
               name,
               email,
               created_at
           )
           SELECT
               s.customer_id,
               s.name,
               s.email,
               s.created_at
           FROM core.core_customers_stage s
           WHERE NOT EXISTS (
               SELECT 1
               FROM core.core_customers c
               WHERE c.customer_id = s.customer_id
           )
       """))

    conn.execute(text("""
           DROP TABLE core.core_customers_stage
       """))








    df_core_orders_stage = spark.sql("""
    SELECT
    order_id,
    customer_id,
    order_date,
    amount, 
    status
    
    from df_valid_orders
    
    where error_reason is null;
    
    """)


    df_core_orders_stage.write \
        .format("jdbc") \
        .option("url", gp_url) \
        .option("dbtable", "core.core_orders_stage") \
        .option("user", gp_user) \
        .option("password", gp_password) \
        .option("driver", gp_driver) \
        .mode("overwrite") \
        .save()

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS core.core_orders (
            order_id INT,
            customer_id INT,
            order_date TIMESTAMP,
            amount DOUBLE PRECISION,
            status TEXT
        )
    """))

    # UPDATE существующих
    conn.execute(text("""
        UPDATE core.core_orders c
        SET
            customer_id = s.customer_id,
            order_date = s.order_date,
            amount = s.amount,
            status = s.status
        FROM core.core_orders_stage s
        WHERE c.order_id = s.order_id
    """))

    # INSERT новых
    conn.execute(text("""
        INSERT INTO core.core_orders (
            order_id,
            customer_id,
            order_date,
            amount,
            status
        )
        SELECT
            s.order_id,
            s.customer_id,
            s.order_date,
            s.amount,
            s.status
        FROM core.core_orders_stage s
        WHERE NOT EXISTS (
            SELECT 1
            FROM core.core_orders c
            WHERE c.order_id = s.order_id
        )
    """))

    # Удаляем техническую stage-таблицу
    conn.execute(text("""
        DROP TABLE core.core_orders_stage
    """))




# ТУТ ВАЖЕН ПОРЯДОК УДАЛЕНИЯ

with engine.begin() as conn:
    # FK orders -> customers
    conn.execute(text("""
        ALTER TABLE core.core_orders
        DROP CONSTRAINT IF EXISTS fk1
    """))


    # PK customers
    conn.execute(text("""
        ALTER TABLE core.core_customers
        DROP CONSTRAINT IF EXISTS pk_core_customers
    """))

    # PK orders
    conn.execute(text("""
        ALTER TABLE core.core_orders
        DROP CONSTRAINT IF EXISTS pk_core_orders
    """))


    # PK customers
    conn.execute(text("""
        ALTER TABLE core.core_customers
        ADD CONSTRAINT pk_core_customers
        PRIMARY KEY (customer_id)
    """))



    conn.execute(text("""
        ALTER TABLE core.core_orders
        ADD CONSTRAINT pk_core_orders
        PRIMARY KEY (order_id)
    """))

    # FK orders -> customers
    conn.execute(text("""
        ALTER TABLE core.core_orders
        ADD CONSTRAINT fk1
        FOREIGN KEY (customer_id)
        REFERENCES core.core_customers(customer_id)
    """))


    # MART
    conn.execute(text("""
        CREATE SCHEMA IF NOT EXISTS mart
    """))





# ----------------data mart-----------------------



core_orders = (
    spark.read
    .format("jdbc")
    .option("url", gp_url)
    .option("dbtable", "core.core_orders")
    .option("user", gp_user)
    .option("password", gp_password)
    .option("driver", gp_driver)
    .load()
)



core_customers = (
    spark.read
    .format("jdbc")
    .option("url", gp_url)
    .option("dbtable", "core.core_customers")
    .option("user", gp_user)
    .option("password", gp_password)
    .option("driver", gp_driver)
    .load()
)

core_orders.createOrReplaceTempView("core_orders")
core_customers.createOrReplaceTempView("core_customers")


data_mart = spark.sql("""
    SELECT
        c.customer_id,
        c.name,

        COUNT(o.order_id) AS orders_count,

        COALESCE(
            SUM(o.amount),
            0
        ) AS total_amount,

        COALESCE(
            SUM(
                CASE
                    WHEN o.status = 'completed'
                    THEN o.amount
                END
            ),
            0
        ) AS completed_amount,

        COUNT(
            CASE
                WHEN o.status = 'cancelled'
                THEN 1
            END
        ) AS cancelled_count

    FROM core_customers c

    LEFT JOIN core_orders o
        ON o.customer_id = c.customer_id

    GROUP BY
        c.customer_id,
        c.name
""")

data_mart.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "mart.data_mart_new") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()


data_mart.show()


~~~
