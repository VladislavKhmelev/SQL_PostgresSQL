~~~
# --✅ Python управляет подключением и транзакцией;
# --✅ SQL выполняется через SQLAlchemy.

# with engine.begin() as connection:
#     ...
#
# тогда транзакцией управляет уже SQLAlchemy, которое отправляет соответствующие команды/управляет соединением с PostgreSQL.

# ┌──────────────────────────────┐
# │ PYTHON + SQLALCHEMY          │
# │                              │
# │ engine.begin()               │
# │    ↓                         │
# │ connection.execute(...)      │
# │    ↓                         │
# │ COMMIT / ROLLBACK             │
# │                              │
# │ Управляет: SQLAlchemy        │
# └──────────────────────────────┘

# 1.CSV читает PostgreSQL Server через COPY FROM;
# 2.Python сам CSV не открывает;
# 3.Pandas нет;
# 4.Psycopg2 напрямую нет;
# 5.\copy нет;
# 6.SQLAlchemy управляет подключением и транзакцией;
# 7.RAW → VALIDATED → ERROR → CORE → MART;
# 8.в ERROR есть error_id SERIAL;
# 9.supplier_id DUPLICATE проверяется валидацией;
# 10.CORE загружается через UPSERT.


import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import psycopg2




load_dotenv(
    r"C:\Users\Vladislav X\Desktop\proj\ver7_API\1.env"
)

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")


engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
)



pg_connection = psycopg2.connect(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password
)



#------------------------------------------
with engine.connect() as connection:

    result = connection.execute(
        text("SELECT 1")
    )

    if result.fetchone() == (1,):
        print("Подключен к PostgreSQL (SQLAlchemy)")

#--------------------------------------------------------


try:

    with engine.begin() as connection:

        print("\nТранзакция начата")




        connection.execute(
            text("""
                DROP TABLE IF EXISTS suppliers_mart;
                DROP TABLE IF EXISTS error_suppliers;
                DROP TABLE IF EXISTS suppliers;
                DROP TABLE IF EXISTS validated_suppliers;
                DROP TABLE IF EXISTS raw_suppliers;
            """)
        )


        connection.execute(
            text("""
                CREATE TABLE raw_suppliers (
                    supplier_id TEXT,
                    supplier_name TEXT,
                    country TEXT,
                    email TEXT,
                    active TEXT,
                    contract_start TEXT
                )
            """)
        )


        connection.execute(
            text("""
                CREATE TABLE validated_suppliers (
                    supplier_id INT,
                    supplier_name TEXT,
                    country TEXT,
                    email TEXT,
                    active BOOLEAN,
                    contract_start DATE,
                    error_reason TEXT
                )
            """)
        )


        connection.execute(
            text("""
                CREATE TABLE error_suppliers (
                    error_id SERIAL PRIMARY KEY,
                    supplier_id INT,
                    supplier_name TEXT,
                    country TEXT,
                    email TEXT,
                    active BOOLEAN,
                    contract_start DATE,
                    error_reason TEXT
                )
            """)
        )



        connection.execute(
            text("""
                CREATE TABLE suppliers (
                    supplier_id INT PRIMARY KEY,
                    supplier_name TEXT,
                    country TEXT,
                    email TEXT,
                    active BOOLEAN,
                    contract_start DATE
                )
            """)
        )

except Exception:
    raise

# загружаем файл через PostgresSQL Server
#--------------------------------------------
# здесь надо указывать COPY и адрес где файл находит.

        # ┌──────────────────────────────────────────────────────────────┐
        # │ Локальный CSV                                                │
        # │                                                              │
        # │ psql + \copy        → можно                                 │
        # │ SQLAlchemy + COPY   → только если файл доступен серверу      │
        # │ Python + psycopg2   → можно через COPY FROM STDIN             │
        # └──────────────────────────────────────────────────────────────┘
  #это версия правильная, но мы заменяем на Python + psycopg2   → можно через COPY FROM STDIN
 #----------------------------------------------
        # connection.execute(
        #     text(r"""
        #         COPY raw_suppliers
        #         FROM 'C:\Users\Vladislav X\Desktop\proj\ver8\data\suppliers.csv'
        #         WITH (
        #             FORMAT CSV,
        #             HEADER
        #         )
        #     """)
        # )
#-------------------------------------------------------------------

try:
    with pg_connection.cursor() as cursor:
        with open(
                r"C:\Users\Vladislav X\Desktop\proj\ver8\data\suppliers.csv",
                "r",
                encoding="utf-8"
        ) as file:


            cursor.copy_expert(
                """
                COPY raw_suppliers
                FROM STDIN
                WITH (
                    FORMAT CSV,
                    HEADER
                )
                """,
                file
            )

        pg_connection.commit()

except Exception:
    pg_connection.rollback()
    raise

finally:
    pg_connection.close()





try:

    with engine.begin() as connection:

        query = text("""
            INSERT INTO validated_suppliers (
                supplier_id,
                supplier_name,
                country,
                email,
                active,
                contract_start,
                error_reason
            )
            SELECT

                -- supplier_id
                CASE
                    WHEN NULLIF(TRIM(supplier_id), '') ~ '^\\d+$'
                         AND NULLIF(TRIM(supplier_id), '')::int > 0
                    THEN NULLIF(TRIM(supplier_id), '')::int
                    ELSE NULL
                END,

                -- supplier_name
                NULLIF(TRIM(supplier_name), ''),

                -- country
                NULLIF(TRIM(country), ''),

                -- email
                NULLIF(TRIM(email), ''),

                -- active
                CASE
                    WHEN LOWER(TRIM(active)) IN ('true', 'false')
                    THEN TRIM(active)::boolean
                    ELSE NULL
                END,

                -- contract_start
                CASE
                    WHEN TRIM(contract_start)
                         ~ '^\\d{4}-\\d{2}-\\d{2}$'
                    THEN TRIM(contract_start)::date

                    WHEN TRIM(contract_start)
                         ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$'
                    THEN TO_DATE(
                        TRIM(contract_start),
                        'DD.MM.YYYY'
                    )

                    WHEN TRIM(contract_start)
                         ~ '^\\d{4}/\\d{2}/\\d{2}$'
                    THEN TO_DATE(
                        TRIM(contract_start),
                        'YYYY/MM/DD'
                    )

                    ELSE NULL
                END,

                -- error_reason
                NULLIF(
                    CONCAT_WS(
                        '; ',

                        -- supplier_id
                        CASE
                            WHEN NULLIF(TRIM(supplier_id), '') IS NULL
                                THEN 'supplier_id is NULL'

                            WHEN NULLIF(TRIM(supplier_id), '')
                                 !~ '^\\d+$'
                                THEN 'supplier_id is not integer'

                            WHEN NULLIF(TRIM(supplier_id), '')::int <= 0
                                THEN 'supplier_id <= 0'
                        END,

                        -- duplicate supplier_id
                        CASE
                            WHEN COUNT(*) OVER (
                                PARTITION BY supplier_id
                            ) > 1
                                THEN 'supplier_id DUPLICATE'
                        END,

                        -- supplier_name
                        CASE
                            WHEN NULLIF(TRIM(supplier_name), '') IS NULL
                                THEN 'supplier_name is NULL'
                        END,

                        -- country
                        CASE
                            WHEN NULLIF(TRIM(country), '') IS NULL
                                THEN 'country is NULL'
                        END,

                        -- email
                        CASE
                            WHEN NULLIF(TRIM(email), '') IS NULL
                                 OR TRIM(email) NOT LIKE '%@%'
                                THEN 'email is NULL or invalid'
                        END,

                        -- active
                        CASE
                            WHEN NULLIF(TRIM(active), '') IS NULL
                                 OR LOWER(TRIM(active))
                                    NOT IN ('true', 'false')
                                THEN 'active is NULL or invalid'
                        END,

                        -- contract_start
                        CASE
                            WHEN NULLIF(
                                TRIM(contract_start), ''
                            ) IS NULL
                                THEN 'contract_start is NULL'

                            WHEN TRIM(contract_start)
                                 !~ '^\\d{4}-\\d{2}-\\d{2}$'

                             AND TRIM(contract_start)
                                 !~ '^\\d{2}\\.\\d{2}\\.\\d{4}$'

                             AND TRIM(contract_start)
                                 !~ '^\\d{4}/\\d{2}/\\d{2}$'

                                THEN 'contract_start invalid format'
                        END
                    ),
                    ''
                )

            FROM raw_suppliers
        """)

        connection.execute(query)



        # 3.8. POST LOAD CHECK VALIDATED
# --------------------------------------------

        result = connection.execute(
            text("""
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(
                        CASE
                            WHEN error_reason IS NOT NULL
                            THEN 1
                        END
                    ) AS error_rows
                FROM validated_suppliers
            """)
        )

        total_rows, error_rows = result.fetchone()

        print(
            "VALIDATED:",
            total_rows,
            "строк;",
            "ошибок:",
            error_rows
        )
# --------------------------------------------



        connection.execute(
            text("""
                INSERT INTO error_suppliers (
                    supplier_id,
                    supplier_name,
                    country,
                    email,
                    active,
                    contract_start,
                    error_reason
                )
                SELECT
                    supplier_id,
                    supplier_name,
                    country,
                    email,
                    active,
                    contract_start,
                    error_reason
                FROM validated_suppliers
                WHERE error_reason IS NOT NULL
            """)
        )




        connection.execute(
            text("""
                INSERT INTO suppliers (
                    supplier_id,
                    supplier_name,
                    country,
                    email,
                    active,
                    contract_start
                )
                SELECT
                    supplier_id,
                    supplier_name,
                    country,
                    email,
                    active,
                    contract_start
                FROM validated_suppliers
                WHERE error_reason IS NULL

                ON CONFLICT (supplier_id)

                DO UPDATE SET
                    supplier_name = EXCLUDED.supplier_name,
                    country = EXCLUDED.country,
                    email = EXCLUDED.email,
                    active = EXCLUDED.active,
                    contract_start = EXCLUDED.contract_start
            """)
        )



        # 3.11. POST LOAD CHECK ERROR
# --------------------------------------------

        result = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM error_suppliers
            """)
        )

        error_count = result.scalar()

        print(
            "ERROR загружено:",
            error_count
        )



        result = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM suppliers
            """)
        )

        core_count = result.scalar()

        print(
            "CORE загружено:",
            core_count
        )



                    # +
     # ПРОВЕРКА ДУБЛИКАТОВ В ERROR


        result = connection.execute(
            text("""
                SELECT
                    supplier_id,
                    COUNT(*) AS cnt
                FROM error_suppliers
                GROUP BY supplier_id
                HAVING COUNT(*) > 1
                ORDER BY supplier_id
            """)
        )

        duplicates = result.fetchall()

        print("\nДубликаты supplier_id в ERROR:")

        for row in duplicates:
            print(row)

# --------------------------------------------



        connection.execute(
            text("""
                CREATE TABLE suppliers_mart AS

                SELECT
                    country,

                    COUNT(supplier_id)
                        AS supplier_count,

                    COUNT(
                        CASE
                            WHEN active = TRUE
                            THEN 1
                        END
                    ) AS active_supplier_count,

                    COUNT(
                        CASE
                            WHEN active = FALSE
                            THEN 1
                        END
                    ) AS inactive_supplier_count

                FROM suppliers

                GROUP BY country
            """)
        )



        # POST LOAD CHECK MART
# --------------------------------------------

        result = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM suppliers_mart
            """)
        )

        mart_count = result.scalar()

        print(
            "MART загружено:",
            mart_count
        )
# --------------------------------------------



    print("\nETL успешно завершён — COMMIT")


except Exception as e:

    print("\nОшибка ETL — ROLLBACK")
    print(e)

    raise


~~~
