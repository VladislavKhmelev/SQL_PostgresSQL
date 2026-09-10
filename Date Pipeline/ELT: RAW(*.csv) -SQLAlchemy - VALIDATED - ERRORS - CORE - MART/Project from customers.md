~~~
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





#-----------------------------------

with engine.connect() as connection:
    result = connection.execute(text("SELECT 1"))

    if result.fetchone() == (1,):
        print("Подключен")

# -----------------------------------


query = text("""
    INSERT INTO raw_customers (
        customer_id,
        name,
        email,
        city,
        age,
        status,
        registered_at
    )
    VALUES (
        :customer_id,
        :name,
        :email,
        :city,
        :age,
        :status,
        :registered_at
    )
""")


df = pd.read_csv(
    r"C:\Users\Vladislav X\Desktop\proj\ver4\data\customers.csv"
)

file = df.to_dict(orient="records")


try:

    with engine.begin() as connection:

        connection.execute(
            text("TRUNCATE TABLE raw_customers")
        )


        connection.execute(query, file)


        query1 = text("""
            CREATE TABLE validated_customers AS
            SELECT
                customer_id::int,
                name::text,
                email::text,
                city::text,
                age::int,
                status::text,
                registered_at::date,
                
                NULLIF(
                    CONCAT_WS(
                        '; ',
                        CASE
                            WHEN age < 18 or age > 100 
                            THEN 'age < 18 or age > 100 '
                        END,
        
                        CASE
                            WHEN city not in ('Ufa','Kazan')
                            THEN ' city problem'
                        END,
        
                        CASE
                            WHEN status not in ('active','blocked','pending','deleted')
                            THEN 'status error'
                        END,
        
                        CASE
                            WHEN name is null
                            THEN 'name is null'
                        END,
        
                        
        
                        CASE
                            WHEN email is null
                            THEN 'email is null'
                        END
                    ),
                    ''
                )::text AS error_reason
        
            FROM raw_customers;
        """)

        connection.execute(
            text("DROP TABLE IF EXISTS validated_customers")
        )

        connection.execute(query1)





        query = text("""
            INSERT INTO customers (
                customer_id,
                name,
                email,
                city,
                age,
                status,
                registered_at
            )
            select 
                customer_id,
                name,
                email,
                city,
                age,
                status,
                registered_at
            
            from validated_customers
            WHERE error_reason IS NULL
            ON CONFLICT (customer_id)
            DO UPDATE SET
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                city = EXCLUDED.city,
                age = EXCLUDED.age,
                status = EXCLUDED.status,
                registered_at = EXCLUDED.registered_at
        """)

        connection.execute(query)

        connection.execute(
            text("Drop TABLE error_customers")
        )

        query = text("""
                    create table error_customers as 
                    select 
                        customer_id,
                        name,
                        email,
                        city,
                        age,
                        status,
                        registered_at,
                        error_reason

                    from validated_customers
                    WHERE error_reason IS not NULL
                    
                """)

        connection.execute(query)


except Exception as e:

    raise


~~~
