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


#-----------------------------------

with engine.connect() as connection:
    result = connection.execute(text("SELECT 1"))

    if result.fetchone() == (1,):
        print("Подключен к PostgresSQL (SQLAlchemy)")

# -----------------------------------



connection = psycopg2.connect(
    user=user,
    password=password,
    host=host,
    port=port,
    dbname=database
)



file = r"C:\Users\Vladislav X\Desktop\proj\ver6\data\customers.csv"


try:
    with connection.cursor() as cursor:

        with open(file, "r", encoding="utf-8") as f:


            cursor.execute("""TRUNCATE TABLE raw_customers""")

            cursor.copy_expert(
                """
                COPY raw_customers (
                    customer_id,
                    name,
                    email,
                    city,
                    registration_date
                )
                FROM STDIN
                WITH CSV HEADER
                """,
                f
            )



        connection.commit()



        cursor.execute(
            "SELECT COUNT(*) FROM raw_customers"
        )

        count = cursor.fetchone()[0]

        print("RAW_table: загружено:", count)


except Exception as e:
    connection.rollback()
    print("ошибка, откат", "\n")
    raise




try:
    with engine.begin() as connection:

        query = text("""
            CREATE TABLE validated_customers AS
            WITH tt AS (
                SELECT
                    TRIM(customer_id)::int AS customer_id,
                    TRIM(name)::text AS name,
                    LOWER(TRIM(email))::text AS email,
                    INITCAP(TRIM(city))::text AS city,

                    CASE
                        WHEN TRIM(registration_date) ~ '^\\d{4}-\\d{2}-\\d{2}$'
                             AND SUBSTRING(TRIM(registration_date), 6, 2)::int BETWEEN 1 AND 12
                             AND SUBSTRING(TRIM(registration_date), 9, 2)::int BETWEEN 1
                                 AND EXTRACT(
                                     DAY FROM (
                                         DATE_TRUNC(
                                             'month',
                                             MAKE_DATE(
                                                 SUBSTRING(TRIM(registration_date), 1, 4)::int,
                                                 SUBSTRING(TRIM(registration_date), 6, 2)::int,
                                                 1
                                             )
                                             + INTERVAL '1 month'
                                         ) - INTERVAL '1 day'
                                     )
                                 )
                        THEN TO_DATE(TRIM(registration_date), 'YYYY-MM-DD')

                        WHEN TRIM(registration_date) ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$'
                             AND SUBSTRING(TRIM(registration_date), 4, 2)::int BETWEEN 1 AND 12
                             AND SUBSTRING(TRIM(registration_date), 1, 2)::int BETWEEN 1
                                 AND EXTRACT(
                                     DAY FROM (
                                         DATE_TRUNC(
                                             'month',
                                             MAKE_DATE(
                                                 SUBSTRING(TRIM(registration_date), 7, 4)::int,
                                                 SUBSTRING(TRIM(registration_date), 4, 2)::int,
                                                 1
                                             )
                                             + INTERVAL '1 month'
                                         ) - INTERVAL '1 day'
                                     )
                                 )
                        THEN TO_DATE(TRIM(registration_date), 'DD.MM.YYYY')

                        WHEN TRIM(registration_date) ~ '^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$'
                        THEN TO_TIMESTAMP(
                            TRIM(registration_date),
                            'YYYY-MM-DD HH24:MI:SS'
                        )::DATE

                        WHEN TRIM(registration_date) ~ '^\\d{2}/\\d{2}/\\d{4}$'
                        THEN TO_DATE(
                            TRIM(registration_date),
                            'DD/MM/YYYY'
                        )

                        ELSE NULL
                    END AS registration_date

                FROM raw_customers
            )

            SELECT
                customer_id,
                name,
                email,
                city,
                registration_date,

                NULLIF(
                    CONCAT_WS(
                        '; ',

                        CASE
                            WHEN customer_id IS NULL
                            THEN 'customer_id is null'
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
                            WHEN city IS NULL
                            THEN 'city is null'
                        END,

                        CASE
                            WHEN registration_date IS NULL
                            THEN 'registration_date is null'
                        END,

                        CASE
                            WHEN registration_date > CURRENT_DATE
                            THEN 'date future'
                        END

                    ),
                    ''
                )::text AS error_reason

            FROM tt;
        """)


        connection.execute(
            text("DROP TABLE IF EXISTS validated_customers")
        )

        connection.execute(query)




                # VALIDATED_table проверка загрузки строк rows и rows_error
#---------------------------------------------------------

        result = connection.execute(
            text("SELECT COUNT(*) FROM validated_customers")
        )

        result_error = connection.execute(
            text("SELECT COUNT(case when error_reason is not null then 1 end)  FROM validated_customers")
        )


        rows = result.scalar()

        rows_error = result_error.scalar()

        print("VALIDATED_table: загружено строк", rows, "из них error: ", rows_error, "\n")
#---------------------------------------------------------

        query = text("""
                            INSERT INTO customers (
                                customer_id,
                                name,
                                email,
                                city,
                                registration_date
                     
                            )
                            select 
                                customer_id,
                                name,
                                email,
                                city,
                                registration_date

                            from validated_customers

                            WHERE error_reason IS NULL

                            ON CONFLICT (customer_id)
                            DO UPDATE SET
                                name = EXCLUDED.name,
                                email = EXCLUDED.email,
                                city = EXCLUDED.city,
                                registration_date = EXCLUDED.registration_date
                   

                        """)

        connection.execute(query)

        query = text("""
                                INSERT INTO error_customers (
                                customer_id,
                                name,
                                email,
                                city,
                                registration_date,
                                error_reason
                                    )
                                SELECT 
                                customer_id,
                                name,
                                email,
                                city,
                                registration_date,
                                error_reason

                                from validated_customers

                                WHERE error_reason IS not NULL

                                on conflict(customer_id)
                                do nothing

                                """)

        connection.execute(query)

        # customers и error_customers проверка загруженных строк
#--------------------------------------------------------
        result = connection.execute(
            text("SELECT COUNT(*) FROM customers")
        )

        result_error = connection.execute(
            text("SELECT COUNT(*)  FROM error_customers")
        )

        rows = result.scalar()

        rows_error = result_error.scalar()

        print(
            f"CORE_table and ERROR_table: загружено валидных строк: {rows}, "
            f"невалидных строк: {rows_error}\n"
            f"всего строк: {rows + rows_error}"
        )
#--------------------------------------------------------

except Exception as e:
    print("ошибка, откат")
    raise

~~~
