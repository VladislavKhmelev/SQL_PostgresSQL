import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text
import logging

load_dotenv(r"C:\Users\Vladislav X\Desktop\proj\ver4\1.env")

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
    INSERT INTO raw_orders (
        order_id,
        customer_id,
        product_id,
        amount,
        status,
        order_date
   
    )
    VALUES (
        :order_id,
        :customer_id,
        :product_id,
        :amount,
        :status,
        :order_date
   
    )
""")

df = pd.read_csv(
    r"C:\Users\Vladislav X\Desktop\proj\ver4\data\orders.csv"
)

file = df.to_dict(orient="records")

try:

    with engine.begin() as connection:

        connection.execute(
            text("TRUNCATE TABLE raw_orders")
        )

        connection.execute(query, file)

        query1 = text("""
            CREATE TABLE validated_orders AS
            SELECT
                order_id::int,
                customer_id::int,
                product_id::int,
                amount::numeric(10,2),
                status::text,
                order_date::date,

                NULLIF(
                    CONCAT_WS(
                        '; ',
                        CASE
                            WHEN order_id IS NULL
                            THEN 'order_id is null'
                        END,

                        CASE
                            WHEN customer_id IS NULL
                            THEN 'customer_id is null'
                        END,

                        CASE
                            WHEN product_id IS NULL
                            THEN 'product_id is null'
                        END,

                        CASE
                            WHEN amount <= 0
                            THEN 'amount <= 0'
                        END,

                        CASE
                            WHEN status NOT IN ('completed','cancelled','pending')
                            THEN 'status error'
                        END,

                        CASE
                            WHEN order_date > CURRENT_DATE
                            THEN 'date error'
                        END,
                        
                        CASE
                            WHEN customer_id NOT IN (
                                SELECT customer_id
                                FROM customers
                            )
                            THEN 'customer_id not found'
                        END
                    ),
                    ''
                )::text AS error_reason

            FROM raw_orders;
        """)


        connection.execute(
            text("DROP TABLE IF EXISTS validated_orders")
        )

        connection.execute(query1)

        query = text("""
            INSERT INTO orders (
                order_id,
                customer_id,
                product_id,
                amount,
                status,
                order_date
            )
            select 
                order_id,
                customer_id,
                product_id,
                amount,
                status,
                order_date

            from validated_orders
            WHERE error_reason IS NULL
            ON CONFLICT (order_id)
            DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                product_id = EXCLUDED.product_id,
                amount = EXCLUDED.amount,
                status = EXCLUDED.status,
                order_date = EXCLUDED.order_date
           
        """)

        connection.execute(query)

        connection.execute(
            text("DROP TABLE IF EXISTS error_orders")
        )

        query = text("""
                    create table error_orders as 
                    select 
                        order_id,
                        customer_id,
                        product_id,
                        amount,
                        status,
                        order_date,
                        error_reason

                    from validated_orders
                    WHERE error_reason IS not NULL

                """)

        connection.execute(query)

        query = text("""
                           CREATE table dm_customer_sales as 

                            SELECT
                            
                            e.customer_id as customer_id,
                            e.name as customer_name,
                            e.city as city,
                            
                            count(ee.order_id) as total_orders,
                            
                            count(case when ee.status = 'completed' then 1 end) as completed_orders,
                            
                            count(case when ee.status = 'cancelled' then 1 end) as cancelled_orders,
                            
                            COALESCE(sum(ee.amount),0)  as total_amount,
                            
                            COALESCE(sum(case when ee.status = 'completed' then amount end),0) as completed_amount,
                            
                            round(COALESCE(avg(case when ee.status = 'completed' then amount end),0),2)  as average_completed
                            
                            from customers e
                            INNER JOIN orders ee
                            on ee.customer_id = e.customer_id
                            
                            GROUP BY e.customer_id, e.name, e.city;

                        """)

        connection.execute(
            text("DROP TABLE IF EXISTS dm_customer_sales")
        )
        connection.execute(query)

        print("операция выполнена")



except Exception as e:

    raise

















