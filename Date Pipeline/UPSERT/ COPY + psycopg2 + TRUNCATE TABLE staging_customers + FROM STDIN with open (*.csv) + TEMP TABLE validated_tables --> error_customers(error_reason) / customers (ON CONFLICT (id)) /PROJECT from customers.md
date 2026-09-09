~~~

CSV → Python (pandas) → PostgreSQL

COPY_Staging

customers.csv

id,name,email,age,status,created_at
1,Alex,alex@mail.com,25,active,2026-09-01
2,Anna,anna@mail.com,32,active,2026-09-01
3,John,john@mail.com,17,active,2026-09-02
4,Mike,mike@mail.com,40,blocked,2026-09-02
5,Lisa,lisa@mail.com,28,active,2026-09-02
6,Peter,peter@mail.com,-5,active,2026-09-03
7,Sarah,sarah@mail.com,29,pending,2026-09-03
8,Tom,tom@mail.com,35,active,2026-09-03
9,Emma,emma@mail.com,22,deleted,2026-09-04
10,David,david@mail.com,999,active,2026-09-04

-----------------------------------------------------------------

import os
import psycopg2
from dotenv import load_dotenv
import logging


load_dotenv()


# ┌──────────────────────────────┐
# │ Logging                      │
# └──────────────────────────────┘

logging.basicConfig(
    filename=r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version7\logs\etl_customers.log",
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

path = r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version7\data\customers.csv"


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

        cursor.execute("""
            TRUNCATE TABLE staging_customers;
        """)

        cursor.execute("""
            TRUNCATE TABLE error_customers;
        """)


        # ┌──────────────────────────────┐
        # │ 2. CSV → staging через COPY  │
        # └──────────────────────────────┘

        logging.info("Начинаем COPY CSV → staging")

        with open(path, "r", encoding="utf-8") as file:

            cursor.copy_expert(
                """
                COPY staging_customers (
                    id,
                    name,
                    email,
                    age,
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

        cursor.execute("""
            CREATE TEMP TABLE validated_customers AS
            SELECT
                *,
                NULLIF(
                    CONCAT_WS(
                        '; ',

                        CASE
                            WHEN age <= 0
                            THEN 'invalid age'
                        END,

                        CASE
                            WHEN name IS NULL
                            THEN 'name is null'
                        END,

                        CASE
                            WHEN email IS NULL
                            THEN 'email is null'
                        END,

                        CASE
                            WHEN created_at > CURRENT_TIMESTAMP
                            THEN 'future date'
                        END,

                        CASE
                            WHEN status NOT IN (
                                'active',
                                'blocked',
                                'pending',
                                'deleted'
                            )
                            THEN 'invalid status'
                        END
                    ),
                    ''
                ) AS error_reason

            FROM staging_customers;
        """)


        # ┌──────────────────────────────┐
        # │ Считаем ошибочные строки     │
        # └──────────────────────────────┘

        cursor.execute("""
            SELECT COUNT(*)
            FROM validated_customers
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

        cursor.execute("""
            SELECT COUNT(*)
            FROM validated_customers
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

        cursor.execute("""
            INSERT INTO error_customers (
                id,
                name,
                email,
                age,
                status,
                created_at,
                error_reason
            )
            SELECT
                id,
                name,
                email,
                age,
                status,
                created_at,
                error_reason
            FROM validated_customers
            WHERE error_reason IS NOT NULL;
        """)


        # ┌──────────────────────────────┐
        # │ 5. Clean → customers         │
        # │    через UPSERT               │
        # └──────────────────────────────┘

        cursor.execute("""
            INSERT INTO customers (
                id,
                name,
                email,
                age,
                status,
                created_at
            )
            SELECT
                id,
                name,
                email,
                age,
                status,
                created_at
            FROM validated_customers
            WHERE error_reason IS NULL

            ON CONFLICT (id)
            DO UPDATE SET
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                age = EXCLUDED.age,
                status = EXCLUDED.status,
                created_at = EXCLUDED.created_at;
        """)

        logging.info("UPSERT завершён")


        # ┌──────────────────────────────┐
        # │ 6. Post-load check           │
        # └──────────────────────────────┘

        cursor.execute("""
            SELECT COUNT(*)
            FROM validated_customers v
            JOIN customers c
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
