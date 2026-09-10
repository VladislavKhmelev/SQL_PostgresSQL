~~~
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text
import logging

load_dotenv(r"C:\Users\Vladislav X\Desktop\proj\ver3\1.env")

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
)

# Тогда Pandas будет стараться вывести всю таблицу целиком, без ... по столбцам.
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)


df = pd.read_csv(
    r"C:\Users\Vladislav X\Desktop\proj\ver3\data\deliveries.csv"
)







df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
df.columns = df.columns.str.strip()
#
# df["name"] = df["name"].str.title()
# df["city"] = df["city"].str.title()
df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce")
#
# numeric_columns = [
#     "customer_id",
#     "phone",
#     "age"
# ]
# for column in numeric_columns:
#     df[column] = pd.to_numeric(
#         df[column],
#         errors="coerce"
#     )

print(df)
print(df.dtypes)
# print(df.isna().sum())


print("ALL Rows:", len(df))

# -----------------------------------

with engine.connect() as connection:
    result = connection.execute(text("SELECT 1"))
    print(result.fetchone())

# -----------------------------------


with engine.begin() as connection:
    connection.execute(
        text("TRUNCATE TABLE staging_deliveries")
    )

    df.to_sql(
        "staging_deliveries",
        connection,
        if_exists="append",
        index=False
    )

    connection.execute(
        text("DROP TABLE IF EXISTS validated_deliveries")
    )

    connection.execute(
        text("""
            CREATE TABLE validated_deliveries AS
            SELECT
                *,
                NULLIF(
                    CONCAT_WS(
                        '; ',
                        CASE WHEN delivery_id IS NULL
                             THEN 'delivery_id is null' END,

                        CASE WHEN customer_id is null
                             THEN 'customer_id is null' END,
                             
                               CASE WHEN restaurant_id is null
                             THEN 'restaurant_id is null' END,

                        CASE WHEN amount <= 0
                             THEN 'amount <= 0' END,

                        CASE WHEN delivery_fee < 0 
                             THEN 'delivery_fee < 0' END,

                             CASE WHEN status not in ('delivered', 'cancelled', 'pending')
                             THEN 'status problem' END,



                        CASE WHEN delivery_date > CURRENT_TIMESTAMP
                             THEN 'future date' END

                       
                    ),
                    ''
                ) AS error_reason
            FROM staging_deliveries;
        """)
    )
    connection.execute(
        text("truncate table error_deliveries")
    )

    connection.execute(
        text("""
                INSERT INTO error_deliveries (
                    delivery_id,
                    customer_id,
                    restaurant_id,
                    amount,
                    delivery_fee,
                    status,
                    delivery_date,
                  error_reason
                )
                SELECT
                    delivery_id,
                    customer_id,
                    restaurant_id,
                    amount,
                    delivery_fee,
                    status,
                    delivery_date,
                    error_reason
                FROM validated_deliveries
                WHERE error_reason IS NOT NULL;
            """)
    )

    connection.execute(
        text("""
            INSERT INTO deliveries (
                delivery_id,
                    customer_id,
                    restaurant_id,
                    amount,
                    delivery_fee,
                    status,
                    delivery_date
            )
            SELECT
               delivery_id,
                    customer_id,
                    restaurant_id,
                    amount,
                    delivery_fee,
                    status,
                    delivery_date
            FROM validated_deliveries
            WHERE error_reason IS NULL
            ON CONFLICT (delivery_id)
            DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                restaurant_id = EXCLUDED.restaurant_id,
                amount = EXCLUDED.amount,
                delivery_fee = EXCLUDED.delivery_fee,
                status = EXCLUDED.status,
                delivery_date = EXCLUDED.delivery_date;
              
        """)
    )


~~~
