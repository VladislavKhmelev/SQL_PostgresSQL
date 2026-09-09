~~~

RAW → STAGING → CORE → DATA MART

customers.csv

id,name,email,age,status,created_at
1,Alex,alex@mail.com,25,active,2026-09-01
2,Anna,anna@mail.com,31,active,2026-09-01
3,John,john@mail.com,17,active,2026-09-02
4,Mike,mike@mail.com,42,blocked,2026-09-02
5,Lisa,lisa@mail.com,28,active,2026-09-03
6,Peter,peter@mail.com,-5,active,2026-09-03
7,Sarah,sarah@mail.com,29,pending,2026-09-03
8,Tom,tom@mail.com,35,active,2026-09-04
9,Emma,emma@mail.com,22,deleted,2026-09-04
10,David,david@mail.com,45,unknown,2026-09-05

-----------------------------------------------------------------

import os
import psycopg2
from dotenv import load_dotenv
import logging
import pandas as pd
from io import StringIO

load_dotenv(r"C:\Users\Vladislav X\Desktop\proj\ver2\1.env")


# ┌──────────────────────────────┐
# │ Logging                      │
# └──────────────────────────────┘

logging.basicConfig(
    filename=r"C:\Users\Vladislav X\Desktop\proj\ver2\logs\etl_customers.log",
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

# df = r"C:\Users\Vladislav X\Desktop\proj\ver2\data\customers.csv"
#
# print(df)



df = pd.read_csv(
    r"C:\Users\Vladislav X\Desktop\proj\ver2\data\customers.csv",
    parse_dates=["created_at"]
)


print(df)
print(df.dtypes)
# print(df.columns)

df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
df.columns = df.columns.str.strip()

df["email"] = df["email"].str.lower()
df["status"] = df["status"].str.lower()
df["name"] = df["name"].str.title()

print(df.isna().sum())
# print(df.notnull().all(axis=1))
print(df.duplicated().sum())




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
# подключаем к Postgres, курсор позваляляет передавать запросы через пайтон
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

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)






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
            buffer
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
                            WHEN id is null
                            THEN 'id is null'
                        END,
                        
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
# fetchone() → забирает одну строку результата: (2,) [0] → берёт первый элемент: 2 И:error_count = 2  То есть error_count = количество невалидных строк.
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
# JOIN сопоставляет строки по id. передается запрос только
        cursor.execute("""
            SELECT COUNT(*)
            FROM validated_customers v
            JOIN customers c
                ON c.id = v.id
            WHERE v.error_reason IS NULL;
        """)
# забирает одну строку результата.
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
#«После rollback не скрывай ошибку — передай её дальше».
    raise


finally:
#«Как бы ни закончилась программа, закрой соединение с БД».
    connection.close()

~~~~
