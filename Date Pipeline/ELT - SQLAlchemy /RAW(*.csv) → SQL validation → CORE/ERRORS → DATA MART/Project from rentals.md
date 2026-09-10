~~~
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text
import logging

load_dotenv(r"C:\Users\Vladislav X\Desktop\proj\ver5\1.env")

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
)

# -----------------------------------

with engine.connect() as connection:
    result = connection.execute(text("SELECT 1"))

    if result.fetchone() == (1,):
        print("Подключен")

# -----------------------------------
query = text("""
    INSERT INTO raw_rentals (
        rental_id,
        customer_id,
        car_id,
        days,
        daily_price,
        total_amount,
        status,
        rental_date

    )
    VALUES (
        :rental_id,
        :customer_id,
        :car_id,
        :days,
        :daily_price,
        :total_amount,
        :status,
        :rental_date

    )
""")

df = pd.read_csv(
    r"C:\Users\Vladislav X\Desktop\proj\ver5\data\rentals.csv"
)

file = df.to_dict(orient="records")

try:
    with engine.begin() as connection:
        connection.execute(
            text("TRUNCATE TABLE raw_rentals")
        )

        connection.execute(query, file)

        query1 = text("""
            CREATE TABLE validated_rentals AS
            SELECT
                rental_id::int,
                customer_id::int,
                car_id::int,
                days::int,
                daily_price::int,
                total_amount::int,
                status::text,
                rental_date::date,

                NULLIF(
                    CONCAT_WS(
                        '; ',
                        CASE
                            WHEN rental_id is null
                            THEN 'rental_id is null'
                        END,

                        CASE
                            WHEN customer_id  is null
                            THEN 'customer_id  is null'
                        END,

                        CASE
                            WHEN customer_id not in (select customer_id from customers)
                            THEN 'oprhan'
                        END,

                        CASE
                            WHEN car_id is null
                            THEN 'car_id is null'
                        END,

                        CASE
                            WHEN days <= 0
                            THEN 'days <= 0'
                        END,

                        CASE
                            WHEN daily_price <= 0
                            THEN 'daily_price <= 0'
                        END,

                        CASE
                            WHEN total_amount <= 0
                            THEN 'total_amount <= 0'
                        END,
                        
                          CASE
                            WHEN status not in ('completed','cancelled','pending')
                            THEN 'status error'
                        END,
                        
                          CASE
                            WHEN rental_date::date > current_date
                            THEN 'date error'
                        END,
                        
                          CASE
                            WHEN total_amount != days * daily_price
                            THEN 'total'
                        END
                    ),
                    ''
                )::text AS error_reason

            FROM raw_rentals;
        """)

        connection.execute(
            text("DROP TABLE IF EXISTS validated_rentals")
        )

        connection.execute(query1)

        query = text("""
                    INSERT INTO rentals (
                        rental_id,
                        customer_id,
                        car_id,
                        days,
                        daily_price,
                        total_amount,
                        status,
                        rental_date
                    )
                    select 
                      rental_id,
                        customer_id,
                        car_id,
                        days,
                        daily_price,
                        total_amount,
                        status,
                        rental_date

                    from validated_rentals
                    WHERE error_reason IS NULL
                    ON CONFLICT (rental_id)
                    DO UPDATE SET
                        customer_id = EXCLUDED.customer_id,
                        car_id = EXCLUDED.car_id,
                        days = EXCLUDED.days,
                        daily_price = EXCLUDED.daily_price,
                        total_amount = EXCLUDED.total_amount,
                        status = EXCLUDED.status,
                        rental_date = EXCLUDED.rental_date

                """)

        connection.execute(query)

        query = text("""
                 INSERT INTO error_rentals (
                        rental_id,
                        customer_id,
                        car_id,
                        days,
                        daily_price,
                        total_amount,
                        status,
                        rental_date,
                        error_reason
                    )
                    select 
                        rental_id,
                        customer_id,
                        car_id,
                        days,
                        daily_price,
                        total_amount,
                        status,
                        rental_date,
                        error_reason

                    from validated_rentals
                    WHERE error_reason IS not NULL
                    ON CONFLICT (rental_id)
                    do nothing

                    """)

        connection.execute(query)

        query = text("""
                                  
CREATE TABLE dm_customer_rentals AS

SELECT
    c.customer_id AS customer_id,
    c.name AS customer_name,
    c.city AS city,

    COUNT(r.rental_id) AS total_rentals,

    COUNT(
        CASE
            WHEN r.status = 'completed' THEN 1
        END
    ) AS completed_rentals,

    COUNT(
        CASE
            WHEN r.status = 'cancelled' THEN 1
        END
    ) AS cancelled_rentals,

    COUNT(
        CASE
            WHEN r.status = 'pending' THEN 1
        END
    ) AS pending_rentals,

    COALESCE(
        SUM(r.total_amount), 0
    ) AS total_amount,

    COALESCE(
        SUM(
            CASE
                WHEN r.status = 'completed'
                THEN r.total_amount
            END
        ), 0
    ) AS completed_amount,

    ROUND(
        COALESCE(
            AVG(
                CASE
                    WHEN r.status = 'completed'
                    THEN r.total_amount
                END
            ), 0
        ),
        2
    ) AS average_completed_amount

FROM customers AS c

LEFT JOIN rentals AS r
    ON r.customer_id = c.customer_id

GROUP BY
    c.customer_id,
    c.name,
    c.city;

                                """)

        connection.execute(
            text("DROP TABLE IF EXISTS dm_customer_sales")
        )
        connection.execute(query)

        print("операция выполнена")







except Exception as e:
    raise

~~~
