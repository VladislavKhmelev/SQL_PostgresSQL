~~~
import os
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text

load_dotenv(r"C:\Users\Vladislav X\Desktop\proj\ver7_API\1.env")

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
)

file = r"C:\Users\Vladislav X\Desktop\proj\psql_copy\data\products.csv"

connection = psycopg2.connect(
    user=user,
    password=password,
    host=host,
    port=port,
    dbname=database
)

try:
    with connection.cursor() as cursor:
        with open(file, "r", encoding="utf-8") as f:
            cursor.execute("TRUNCATE TABLE raw_products")

            cursor.copy_expert(
                """
                COPY raw_products (
                    product_id,
                    product_name,
                    category,
                    supplier_id,
                    unit_price,
                    stock_quantity,
                    updated_at
                )
                FROM STDIN
                WITH CSV HEADER
                """,
                f
            )

    connection.commit()

except Exception:
    connection.rollback()
    raise

finally:
    connection.close()

# После RAW переходим к VALIDATED. Здесь уже удобно использовать SQLAlchemy:
try:
    with engine.begin() as connection:
        connection.execute(text("""
            DROP TABLE IF EXISTS validated_products;

            CREATE TABLE validated_products AS
            SELECT
                NULLIF(TRIM(product_id), '')::int AS product_id,
                NULLIF(INITCAP(TRIM(product_name)), '')::text AS product_name,
                NULLIF(INITCAP(TRIM(category)), '')::text AS category,
                NULLIF(TRIM(supplier_id), '')::NUMERIC::int AS supplier_id,
                NULLIF(TRIM(unit_price), '')::NUMERIC(10,2) AS unit_price,
                NULLIF(TRIM(stock_quantity), '')::int AS stock_quantity,

                CASE
                    WHEN updated_at ~ '^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$'
                        THEN updated_at::TIMESTAMP

                    WHEN updated_at ~ '^\\d{2}\\.\\d{2}\\.\\d{4} \\d{2}:\\d{2}:\\d{2}$'
                        THEN to_timestamp(updated_at, 'DD.MM.YYYY HH24:MI:SS')

                    WHEN updated_at ~ '^\\d{4}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2}$'
                        THEN to_timestamp(updated_at, 'YYYY/MM/DD HH24:MI:SS')

                    ELSE NULL
                END AS updated_at,

                NULLIF(
                    CONCAT_WS(
                        '; ',

                        CASE
                            WHEN NULLIF(TRIM(product_id), '') IS NULL
                                THEN 'product_id is NULL'
                        END,

                        CASE
                            WHEN NULLIF(INITCAP(TRIM(product_name)), '') IS NULL
                                THEN 'product_name is NULL'
                        END,

                        CASE
                            WHEN NULLIF(INITCAP(TRIM(category)), '') IS NULL
                                THEN 'category is NULL'
                        END,

                        CASE
                            WHEN NULLIF(TRIM(supplier_id), '') IS NULL
                                THEN 'supplier_id is NULL'
                        END,

                        CASE
                            WHEN NULLIF(TRIM(unit_price), '')::NUMERIC(10,2) <= 0
                                 OR NULLIF(TRIM(unit_price), '') IS NULL
                                THEN 'unit_price <= 0 or NULL'
                        END,

                        CASE
                            WHEN NULLIF(TRIM(stock_quantity), '')::int < 0
                                 OR NULLIF(TRIM(stock_quantity), '') IS NULL
                                THEN 'stock_quantity < 0 or NULL'
                        END,

                        CASE
                            WHEN NULLIF(TRIM(updated_at), '') IS NULL
                                THEN 'updated_at is NULL'
                        END
                    ),
                    ''
                ) AS error_reason

            FROM raw_products;
        """))

        connection.execute(text("""
            INSERT INTO error_products (
                product_id,
                product_name,
                category,
                supplier_id,
                unit_price,
                stock_quantity,
                updated_at,
                error_reason
            )
            SELECT
                product_id,
                product_name,
                category,
                supplier_id,
                unit_price,
                stock_quantity,
                updated_at,
                error_reason
            FROM validated_products
            WHERE error_reason IS NOT NULL
            ON CONFLICT (product_id)
            DO NOTHING;
        """))

        connection.execute(text("""
            INSERT INTO products (
                product_id,
                product_name,
                category,
                supplier_id,
                unit_price,
                stock_quantity,
                updated_at
            )
            SELECT
                product_id,
                product_name,
                category,
                supplier_id,
                unit_price,
                stock_quantity,
                updated_at
            FROM validated_products
            WHERE error_reason IS NULL
            ON CONFLICT (product_id)
            DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category,
                supplier_id = EXCLUDED.supplier_id,
                unit_price = EXCLUDED.unit_price,
                stock_quantity = EXCLUDED.stock_quantity,
                updated_at = EXCLUDED.updated_at;
        """))

        # Теперь строим products_mart:

        connection.execute(text("""
            DROP TABLE IF EXISTS products_mart;

            CREATE TABLE products_mart AS
            SELECT
                product_id,
                product_name,
                category,
                supplier_id,
                unit_price,
                stock_quantity,
                unit_price * stock_quantity AS stock_value,
                updated_at
            FROM products;
        """))


except Exception as e:
    print(f"Ошибка ETL: {e}")
    raise

with engine.connect() as connection:
    result = connection.execute(text("""
        SELECT
            (SELECT COUNT(*) FROM raw_products) AS raw_count,
            (SELECT COUNT(*) FROM validated_products) AS validated_count,
            (SELECT COUNT(*) FROM products) AS core_count,
            (SELECT COUNT(*) FROM error_products) AS error_count
    """))

    print(result.fetchone())


~~~
