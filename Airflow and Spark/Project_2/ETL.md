~~~
 # ────────────────────────────────────────────
# SPARK + POSTGRESQL + GREENPLUM
# ────────────────────────────────────────────

import os

from pyspark.sql import SparkSession

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ────────────────────────────────────────────
# PYSPARK
# ────────────────────────────────────────────

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"


load_dotenv("/opt/airflow/.env", override=True)


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


#----------------------------------------------START Pipeline---------------

#----------def(1)

def read_database():

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


    df = (
        spark.read
        .format("jdbc")
        .option("url", pg_url)
        .option("dbtable", "public.customers")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", pg_driver)
        .load()
    )



    df.show(2)


#----------def(2)

def create_tables():


    gp_engine = create_engine(
        f"postgresql+psycopg2://"
        f"{gp_user}:{gp_password}"
        f"@{gp_host}:{gp_port}/{gp_database}"
    )


    with gp_engine.begin() as conn:


        conn.execute(text("""
    
        CREATE table if not EXISTS raw_customer (
        customer_id int PRIMARY KEY,
        name text,
        city text,
        email text,
        created_at TIMESTAMP
        );
    
        """))


        conn.execute(text("""
        CREATE Table if not EXISTS staging_customer (
        customer_id int,
        name text,
        city text,
        email text,
        created_at TIMESTAMP
        );
    
        """))

# #----------def(3)
def write_database():


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

    df = (
        spark.read
        .format("jdbc")
        .option("url", pg_url)
        .option("dbtable", "public.customers")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", pg_driver)
        .load()
    )

    df.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "staging_customer") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()


# #----------def(4)
def upsert_sqlalhemy():


    gp_engine = create_engine(
        f"postgresql+psycopg2://"
        f"{gp_user}:{gp_password}"
        f"@{gp_host}:{gp_port}/{gp_database}"
    )

    with gp_engine.begin() as conn:
        conn.execute(text("""
    
            UPDATE raw_customer AS t
            SET
                name = s.name,
                city = s.city,
                email = s.email,
                created_at = s.created_at
            FROM staging_customer AS s
            WHERE t.customer_id = s.customer_id;
    
        """))

        conn.execute(text("""
        INSERT INTO raw_customer (
            customer_id,
            name,
            city,
            email,
            created_at
        )
        SELECT
            s.customer_id,
            s.name,
            s.city,
            s.email,
            s.created_at
        FROM staging_customer AS s
        WHERE NOT EXISTS (
            SELECT 1
            FROM raw_customer AS t
            WHERE t.customer_id = s.customer_id
        );
    
        """))







~~~
