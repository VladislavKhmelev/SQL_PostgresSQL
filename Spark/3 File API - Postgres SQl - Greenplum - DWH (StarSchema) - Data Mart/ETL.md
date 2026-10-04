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

# ============================================================
# DF
# ============================================================

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv("1.env")

PG_USER = os.getenv("DB_USER")
PG_PASSWORD = os.getenv("DB_PASSWORD")
PG_HOST = os.getenv("DB_HOST")
PG_PORT = os.getenv("DB_PORT")
PG_DB = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql+psycopg2://"
    f"{PG_USER}:{PG_PASSWORD}"
    f"@{PG_HOST}:{PG_PORT}/{PG_DB}"
)








df = spark.read.csv(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\df.csv",
    header=True,
    inferSchema=True
)


df.write \
    .format("json") \
    .mode("overwrite") \
    .save(
        r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\df"
    )


df = spark.read.json(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\df"
)

df.show()
print(df.count())
df.printSchema()


# ============================================================
# DF2
# ============================================================

df2 = spark.read.csv(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\df2.csv",
    header=True,
    inferSchema=True
)


df2.write \
    .format("json") \
    .mode("overwrite") \
    .save(
        r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\df2"
    )


df2 = spark.read.json(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\df2"
)

df2.show()
print(df2.count())
df2.printSchema()


# ============================================================
# DF3
# ============================================================

df3 = spark.read.parquet(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\df3.parquet"
)


df3.write \
    .format("json") \
    .mode("overwrite") \
    .save(
        r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\df3"
    )


df3 = spark.read.json(
    r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\df3"
)

df3.show()
print(df3.count())
df3.printSchema()


df.createOrReplaceTempView("products")
df2.createOrReplaceTempView("users")
df3.createOrReplaceTempView("carts")



with engine.begin() as conn:

    conn.execute(text("""
     CREATE SCHEMA if not EXISTS raw;
    """))



df.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "raw.raw_products") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .mode("overwrite") \
    .save()


df2.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "raw.raw_users") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .mode("overwrite") \
    .save()



carts = spark.sql("""
    SELECT
        cart_id,
        user_id,
        total,
        discountedTotal,
        totalProducts,
        totalQuantity,
        TO_JSON(products) AS products
    FROM carts
""")




carts.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "raw.raw_carts") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .mode("overwrite") \
    .save()






df_valid_products= spark.sql("""

SELECT
    products_id,
    title,
    category,
    price,
    stock,
    brand,
    rating,
    discountPercentage,
    sku,

    CASE
        WHEN products_id IS NULL
            THEN 'products_id is null'

        WHEN price < 0
            THEN 'price < 0'

        WHEN stock < 0
            THEN 'stock < 0'

        WHEN rating < 0 OR rating > 5
            THEN 'rating error between 0 - 5'

        WHEN discountPercentage < 0
             OR discountPercentage > 100
            THEN 'discountPercentage error between 0 - 100'

        WHEN title IS NULL
            THEN 'title is null'

        WHEN category IS NULL
            THEN 'category is null'

        WHEN sku IS NULL
            THEN 'sku is null'
    END AS error_reason

FROM products;

""")




df_valid_products.show(2)

df_valid_products.createOrReplaceTempView("df_valid_products")


df_valid_products_error = spark.sql("""
SELECT
products_id,
title,
category,
price,
stock,
brand,
rating,
discountPercentage,
sku,
error_reason

from df_valid_products

where error_reason is not null;

""")

df_valid_products_error.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "raw.products_error") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .mode("overwrite") \
    .save()




df_valid_products_core = spark.sql("""
SELECT
products_id,
title,
category,
price,
stock,
brand,
rating,
discountPercentage,
sku


from df_valid_products

where error_reason is  null;

""")


df_valid_products_core .write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "core.products_core") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()



df_valid_users = spark.sql("""

SELECT
    users_id,
    firstName,
    lastName,
    age,
    gender,
    email,
    city,

    CASE
        WHEN users_id IS NULL THEN 'users_id us null'
        WHEN firstName IS NULL THEN 'firstName us null'
        WHEN lastName IS NULL THEN 'lastName us null'
        WHEN age < 18 OR age > 100 THEN 'age < 18 or age > 100'
        WHEN gender NOT IN ('male', 'female') THEN 'gender error format'
        WHEN email IS NULL THEN 'email us null'
        WHEN email NOT LIKE '%@%' THEN 'email error format'
        WHEN city IS NULL THEN 'city us null'
    END AS error_reason

FROM users;

""")

df_valid_users.show( 2)



df_valid_users.createOrReplaceTempView("df_valid_users")


df_valid_users_error = spark.sql("""

SELECT
users_id,
firstName,
lastName,
age,
gender,
email,
city,
error_reason

from df_valid_users

where error_reason is not null;

""")

df_valid_users_error.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "raw.users_error") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .mode("overwrite") \
    .save()





df_valid_users_core = spark.sql("""
SELECT
users_id,
firstName,
lastName,
age,
gender,
email,
city


from df_valid_users

where error_reason is  null;

""")




df_valid_users_core.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "core.users_core") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()











df_valid_carts_array = spark.sql("""
SELECT
    cart_id,
    user_id,
    total,
    discountedTotal,
    totalProducts,
    totalQuantity,
    products,

    CASE
        WHEN cart_id IS NULL
            THEN 'cart_id is null'

        WHEN user_id IS NULL
            THEN 'user_id is null'

        WHEN total < 0
            THEN 'total < 0'

        WHEN discountedTotal < 0
            THEN 'discountedTotal < 0'

        WHEN totalProducts < 0
            THEN 'totalProducts < 0'

        WHEN totalQuantity < 0
            THEN 'totalQuantity < 0'

        WHEN products IS NULL
            THEN 'products is null'

        WHEN discountedTotal > total
            THEN 'discountedTotal > total'

        WHEN totalQuantity < totalProducts
            THEN 'totalQuantity < totalProducts'

    END AS error_reason

FROM carts;

""")



df_valid_carts_string = spark.sql("""
SELECT
    cart_id,
    user_id,
    total,
    discountedTotal,
    totalProducts,
    totalQuantity,
    to_json(products) as products ,

    CASE
        WHEN cart_id IS NULL
            THEN 'cart_id is null'

        WHEN user_id IS NULL
            THEN 'user_id is null'

        WHEN total < 0
            THEN 'total < 0'

        WHEN discountedTotal < 0
            THEN 'discountedTotal < 0'

        WHEN totalProducts < 0
            THEN 'totalProducts < 0'

        WHEN totalQuantity < 0
            THEN 'totalQuantity < 0'

        WHEN products IS NULL
            THEN 'products is null'

        WHEN discountedTotal > total
            THEN 'discountedTotal > total'

        WHEN totalQuantity < totalProducts
            THEN 'totalQuantity < totalProducts'

    END AS error_reason

FROM carts;

""")





df_valid_products.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "raw.validated_products") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .mode("overwrite") \
    .save()


df_valid_users.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "raw.validated_users") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .mode("overwrite") \
    .save()



df_valid_carts_string.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "raw.validated_carts") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .mode("overwrite") \
    .save()





df_valid_carts_array.createOrReplaceTempView("df_valid_carts_array")

df_cart_products_explode = spark.sql("""
SELECT
    cart_id,
    user_id,
    explode(products) AS product
FROM df_valid_carts_array
""")



df_cart_products_explode.show()
df_cart_products_explode.printSchema()


df_cart_products_explode.createOrReplaceTempView("df_cart_products_explode")



df_carts_products_table = spark.sql("""
SELECT
    cart_id,
    user_id,
    product.id AS product_id,
    product.title AS title,
    product.price AS price,
    product.quantity AS quantity,
    product.total AS total,
    product.discountPercentage AS discount_percentage,
    product.discountedTotal AS discounted_total,
    product.thumbnail AS thumbnail

FROM df_cart_products_explode
""")


df_carts_products_table.show(2)


df_carts_products_table.createOrReplaceTempView("df_carts_products_table")




# регестрируем чтобы дальше сделать проверку с orphan

df_valid_products.createOrReplaceTempView("df_valid_products")
df_carts_products_table.createOrReplaceTempView("df_carts_products_table")


df_valid_users.createOrReplaceTempView("df_valid_users")
df_valid_products.createOrReplaceTempView("df_valid_products")

#в error_reason проверяем orphan на (product_id и user_id ) тут берем валидированную таблицу которюу записали в PostgresSQL с колонкой error_reason и ищем между ними orphan

# Orphan:
# 1. ( в df_carts_products_table_valid есть user_id и user_id есть в df_valid_users --->>> ищем orphan)
#   2. в df_valid_products есть product_id и product_id есть в df_carts_products_table_valid  ---->>> ищем)
#



df_carts_products_table_valid = spark.sql("""
SELECT
    c.cart_id,
    c.user_id,
    c.product_id,
    c.title,
    c.price,
    c.quantity,
    c.total,
    c.discount_percentage,
    c.discounted_total,
    c.thumbnail,

    CASE
        WHEN c.product_id IS NULL
            THEN 'product_id is null'

        WHEN c.price < 0
            THEN 'price <0'

        WHEN c.quantity < 0
            THEN 'quantity <0'

        WHEN c.total < 0
            THEN 'total <0'

        WHEN c.discount_percentage < 0
             OR c.discount_percentage > 100
            THEN 'discount_percentage error 0 - 100'

        WHEN c.discounted_total < 0
            THEN 'discounted_total <0'

        WHEN c.discounted_total > c.total
            THEN 'discounted_total > total'

        WHEN c.title IS NULL
            THEN 'title is null'

      WHEN NOT EXISTS (
    SELECT 1
    FROM df_valid_products p
    WHERE c.product_id = p.products_id
)
    THEN 'products_id is orphan'
            
                
      WHEN NOT EXISTS (
    SELECT 1
    FROM df_valid_users u
    WHERE c.user_id = u.users_id
)
    THEN 'user_id is orphan'

    END AS error_reason

FROM df_carts_products_table c
""")


df_carts_products_table_valid.show(2)



#
df_carts_products_table_valid.createOrReplaceTempView("df_carts_products_table_valid")



df_carts_products_table_error = spark.sql("""
SELECT
cart_id,
user_id,
product_id,
title,
price,
quantity,
total,
discount_percentage,
discounted_total,
thumbnail,
error_reason

from df_carts_products_table_valid

where error_reason is not null;

""")



df_carts_products_table_core = spark.sql("""
SELECT
cart_id,
user_id,
product_id,
title,
price,
quantity,
total,
discount_percentage,
discounted_total,
thumbnail

from df_carts_products_table_valid

where error_reason is null;

""")


df_carts_products_table_error.show(2)
df_carts_products_table_core.show(2)


df_carts_products_table_core.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "core.carts_products_table_core") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()



df_carts_products_table_error.write \
    .format("jdbc") \
    .option("url", pg_url) \
    .option("dbtable", "raw.carts_products_table_error") \
    .option("user", pg_user) \
    .option("password", pg_password) \
    .option("driver", pg_driver) \
    .mode("overwrite") \
    .save()




df_open = (
    spark.read
    .format("jdbc")
    .option("url", gp_url)
    .option("dbtable", "core.carts_products_table_core")
    .option("user", gp_user)
    .option("password", gp_password)
    .option("driver", gp_driver)
    .load()
)


print("URL:", gp_url)
print("TABLE:", "core.carts_products_table_core")



#-------------------------DWH (dim + fact)-----------------------

df_carts_products_table_core.show(2)
df_valid_products_core.show(2)
df_valid_users_core.show(2)


df_carts_products_table_core.createOrReplaceTempView(
    "df_carts_products_table_core"
)

df_fact_cart_products = spark.sql("""
    SELECT
        cart_id,
        user_id,
        product_id,
        quantity,
        price,
        discount_percentage
    FROM df_carts_products_table_core
""")










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
    CREATE SCHEMA if not EXISTS dwh;
    """))





    conn.execute(text("""
        
    
    CREATE table if not exists dwh.dim_users (
    users_id int PRIMARY KEY,
    firstName text,
    lastName text,
    age int,
    gender text,
    email text,
    city text
    
    );
    
    
        """))


    conn.execute(text("""
    CREATE table if not exists dwh.dim_products (
    products_id int PRIMARY KEY,
    title text,
    category text,
    price NUMERIC(10,2),
    stock int,
    brand text,
    rating numeric(3,2),
    discountPercentage NUMERIC(4,2),
    sku text
    
    );

    """))



    conn.execute(text("""
    
    
    CREATE table if NOT EXISTS dwh.fact_table_carts_product (
    cart_id int,
    user_id int,
    product_id int,
    quantity int,
    price numeric(10,2), 
    discount_percentage NUMERIC(4,2),
    
    CONSTRAINT fk1 FOREIGN KEY (user_id) REFERENCES dwh.dim_users(users_id),
    CONSTRAINT fk2 FOREIGN KEY (product_id) REFERENCES dwh.dim_products(products_id)
    );
    
    """))



    conn.execute(text("""
        TRUNCATE TABLE
            dwh.fact_table_carts_product,
            dwh.dim_products,
            dwh.dim_users
        CASCADE;
    """))


df_valid_users_core.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "dwh.dim_users") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()



df_valid_products_core.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "dwh.dim_products") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()


df_fact_cart_products.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "dwh.fact_table_carts_product") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()



df_fact_cart_products.createOrReplaceTempView("df_fact_cart_products")
df_valid_products_core.createOrReplaceTempView("df_valid_products_core")
df_valid_users_core.createOrReplaceTempView("df_valid_users_core")




data_mart = spark.sql("""
SELECT
    user_id,
    COUNT(DISTINCT product_id) AS total_products,
    COALESCE(SUM(quantity), 0) AS total_quantity,
    SUM(quantity * price) AS total_amount,
    ROUND(COALESCE(AVG(price), 0), 2) AS avg_price
FROM df_fact_cart_products
GROUP BY user_id
""")



data_mart.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "mart.data_mart") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()




data_mart_2 = spark.sql("""

SELECT
category,

count(DISTINCT products_id) as total_products,
COALESCE(sum(quantity),0) as total_quantity,
sum(quantity *price) as total_amount,
ROUND(COALESCE(avg(price),0),2) as   avg_price

from df_valid_products_core
LEFT join df_fact_cart_products
on df_fact_cart_products.product_id = df_valid_products_core.products_id


GROUP BY category;

""")




data_mart_2.write \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("dbtable", "mart.data_mart_2") \
    .option("user", gp_user) \
    .option("password", gp_password) \
    .option("driver", gp_driver) \
    .mode("overwrite") \
    .save()


~~~
