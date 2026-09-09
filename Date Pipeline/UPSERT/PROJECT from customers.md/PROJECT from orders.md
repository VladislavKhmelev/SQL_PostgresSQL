~~~
CSV → Python (pandas) → PostgreSQL

COPY_Staging

orders.csv


id,customer_id,product_id,amount,status,created_at
1,1,10,5000,completed,2026-09-01
2,2,11,7000,completed,2026-09-01
3,1,12,3000,cancelled,2026-09-01
4,3,10,8000,completed,2026-09-02
5,2,11,6000,completed,2026-09-02
6,5,13,4500,completed,2026-09-03
7,8,14,6200,completed,2026-09-03

-----------------------------------------------------------------
# #todo ---- CSV → Python → PostgreSQL
# ----------------------------------------


# #todo Будем использовать два CSV:
#
# customers.csv
# orders.csv
#
# ----------------------------------------------
#

# #todo  и построим:
#
# CSV
#  ↓
# Pandas
#  ↓
# Проверка
#  ↓
# Очистка
#  ↓
# PostgreSQL
#  ↓
# INSERT
#  ↓
# Проверка загрузки
# -------------------------------------

import os
import psycopg2
from dotenv import load_dotenv
import logging



# CREATE TABLE staging_orders (
#     id INTEGER,
#     customer_id INTEGER,
#     product_id INTEGER,
#     amount NUMERIC,
#     status TEXT,
#     created_at TIMESTAMP
# );
#
# +
#
# CREATE TABLE error_orders (
#     id INTEGER,
#     customer_id INTEGER,
#     product_id INTEGER,
#     amount NUMERIC,
#     status TEXT,
#     created_at TIMESTAMP,
#     error_reason TEXT
# );
#
# +
#
# CREATE TABLE orders (
#     id INTEGER PRIMARY KEY,
#     customer_id INTEGER,
#     product_id INTEGER,
#     amount NUMERIC,
#     status TEXT,
#     created_at TIMESTAMP
# );
#
# +
#
# ALTER TABLE orders
# ADD CONSTRAINT fk_orders_customer
# FOREIGN KEY (customer_id)
# REFERENCES customers(id);   #<---- СОЗДАЕМ ЗАРАНЕЕ


load_dotenv()


# ┌──────────────────────────────┐
# │ Logging                      │
# └──────────────────────────────┘
#!!!!!
logging.basicConfig(
    filename=r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version7\logs\etl_orders.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ┌──────────────────────────────┐
# │ Данные подключения к БД      │
# └──────────────────────────────┘

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")


# ┌──────────────────────────────┐
# │ Путь к CSV                   │
# └──────────────────────────────┘
#!!!!!
path = r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version7\data\orders.csv"


# ┌──────────────────────────────┐
# │ Подключение к PostgreSQL     │
# └──────────────────────────────┘

connection = psycopg2.connect(
    user=user,
    password=password,
    host=host,
    port=port,
    dbname=database
)


logging.info("ETL START")


try:

    with connection.cursor() as cursor:

        # ┌──────────────────────────────┐
        # │ 1. Очищаем staging           │
        # └──────────────────────────────┘
        # !!!!!
        cursor.execute("""
            TRUNCATE TABLE staging_orders;
        """)

        cursor.execute("""
            DROP TABLE IF EXISTS validated_orders;
        """)

        cursor.execute("""
            TRUNCATE TABLE error_orders;
        """)


        # ┌──────────────────────────────┐
        # │ 2. CSV → staging через COPY  │
        # └──────────────────────────────┘

        logging.info("Начинаем COPY CSV → staging")

        with open(path, "r", encoding="utf-8") as file:
            # !!!!!
            cursor.copy_expert(
                """
                COPY staging_orders (
                    id,
                    customer_id,
                    product_id,
                    amount,
                    status,
                    created_at
                )
                FROM STDIN
                WITH CSV HEADER
                """,
                file
            )

        logging.info("COPY завершён")


        # ┌──────────────────────────────┐
        # │ 3. Создаём validation layer  │
        # └──────────────────────────────┘
        # !!!!!
        cursor.execute("""
            CREATE TABLE validated_orders AS
            SELECT
                *,
                NULLIF(
                    CONCAT_WS(
                        '; ',

                        CASE
                            WHEN amount < 0
                            THEN 'invalid amount'
                        END,
                        
                        CASE
                            WHEN created_at > CURRENT_TIMESTAMP
                            THEN 'future date'
                        END,
                        
                        CASE
                    WHEN NOT EXISTS (
                        SELECT 1
                        FROM customers c
                        WHERE c.id = staging_orders.customer_id
                    )
                    THEN 'customer not found'
                END
         
                    ),
                    ''
                ) AS error_reason

            FROM staging_orders;
        """)


        # ┌──────────────────────────────┐
        # │ Считаем ошибочные строки     │
        # └──────────────────────────────┘
        # !!!!!
        cursor.execute("""
            SELECT COUNT(*)
            FROM validated_orders
            WHERE error_reason IS NOT NULL;
        """)

        error_count = cursor.fetchone()[0]

        logging.info(
            "Ошибочных строк: %s",
            error_count
        )


        # ┌──────────────────────────────┐
        # │ Считаем корректные строки    │
        # └──────────────────────────────┘
        # !!!!!
        cursor.execute("""
            SELECT COUNT(*)
            FROM validated_orders
            WHERE error_reason IS NULL;
        """)

        clean_count = cursor.fetchone()[0]

        logging.info(
            "Корректных строк: %s",
            clean_count
        )


        # ┌──────────────────────────────┐
        # │ 4. Ошибки → error_customers  │
        # └──────────────────────────────┘
        # !!!!!
        cursor.execute("""
            INSERT INTO error_orders (
                id,
                customer_id,
                product_id,
                amount,
                status,
                created_at,
                error_reason
            )
            SELECT
                id,
                customer_id,
                product_id,
                amount,
                status,
                created_at,
                error_reason
            FROM validated_orders
            WHERE error_reason IS NOT NULL;
        """)


        # ┌──────────────────────────────┐
        # │ 5. Clean → customers         │
        # │    через UPSERT               │
        # └──────────────────────────────┘
        # !!!!!
        cursor.execute("""
            INSERT INTO orders (
                id,
                customer_id,
                product_id,
                amount,
                status,
                created_at
            )
            SELECT
                id,
                customer_id,
                product_id,
                amount,
                status,
                created_at
            FROM validated_orders
            WHERE error_reason IS NULL

            ON CONFLICT (id)
            DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                product_id = EXCLUDED.product_id,
                amount = EXCLUDED.amount,
                status = EXCLUDED.status,
                created_at = EXCLUDED.created_at;
        """)

        logging.info("UPSERT завершён")


        # ┌──────────────────────────────┐
        # │ 6. Post-load check           │
        # └──────────────────────────────┘
        # !!!!!
        cursor.execute("""
            SELECT COUNT(*)
            FROM validated_orders v
            JOIN orders c
                ON c.id = v.id
            WHERE v.error_reason IS NULL;
        """)

        loaded_clean_count = cursor.fetchone()[0]

        logging.info(
            "Проверено загруженных клиентов: %s",
            loaded_clean_count
        )


        if loaded_clean_count != clean_count:

            raise Exception(
                f"Post-load check failed: "
                f"ожидалось {clean_count}, "
                f"загружено {loaded_clean_count}"
            )


        logging.info(
            "Post-load check успешно пройден"
        )


    # ┌──────────────────────────────┐
    # │ 7. Всё успешно → COMMIT      │
    # └──────────────────────────────┘

    connection.commit()

    logging.info("ETL SUCCESS")

    print("ETL успешно завершён")


except Exception as e:

    # ┌──────────────────────────────┐
    # │ Ошибка → ROLLBACK             │
    # └──────────────────────────────┘

    connection.rollback()

    print("Ошибка ETL:", e)

    logging.error(
        "ETL ERROR: %s",
        e
    )

    raise


finally:

    connection.close()




~~~
