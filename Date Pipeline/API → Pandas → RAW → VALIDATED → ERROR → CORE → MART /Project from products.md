~~~
import pandas as pd
import os
from dotenv import load_dotenv
from psycopg2 import connect
from sqlalchemy import create_engine
from sqlalchemy import text
import requests


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
        print("Подключен к PostgresSQL")

# -----------------------------------

url = "https://fakestoreapi.com/products?utm_source=chatgpt.com"


response = requests.get(url)

print(response.status_code, "\n")

data = response.json()


# print(data)


pd.set_option("display.width", 2000)
pd.set_option("display.max_columns", None)
# pd.set_option("display.max_colwidth", 20)

df = pd.DataFrame(data)

# print(df)

# здесь указываем data это наш ВЕСЬ JSON
# сохраняет в талицу черех csv - удобно смотреть df = ...users


# df = pd.DataFrame(data)
# df.to_csv("API_all_data_table_.csv", index=False)

# далее комментируем оба, и запускаем оба те что ниже


# print("Поля:", df.columns.tolist())
# print(df.head(1))


# Достаём city из вложенного address
df["rating"] = df["rating"].apply(lambda x: x["rate"])

df = df.rename(columns={
    "id": "product_id",
    "title": "product_name"
})

df = df[
    [
        "product_id",
        "product_name",
        "price",
        "category",
        "rating"

    ]
]




# print(df)
# print(df.dtypes)


try:
    with engine.begin() as connection:

        connection.execute(
            text("TRUNCATE TABLE raw_products")
        )


        df.to_sql(
            "raw_products",
            connection,
            if_exists="append",
            index=False
        )

        print("raw rows load:", len(df))

        query = text("""
            CREATE TABLE validated_products AS
            SELECT
                product_id::int,
                product_name::text,
                price::numeric(10,2),
                category::text,
                rating::numeric(2,1),
          

                NULLIF(
                    CONCAT_WS(
                        '; ',
                        CASE
                            WHEN product_id is null
                            THEN 'product_id is null'
                        END,

                        CASE
                            WHEN product_name is null
                            THEN 'product_name is null'
                        END,



                        CASE
                            WHEN price <=0
                            THEN 'price <=0'
                        END,

                        CASE
                            WHEN rating <0 or rating > 5
                            THEN 'rating <0 or rating > 5'
                        END,

                        CASE
                            WHEN category is null
                            THEN 'category is null'
                        END


                    ),
                    ''
                )::text AS error_reason

            FROM raw_products;
        """)

        connection.execute(
            text("DROP TABLE IF EXISTS validated_products")
        )

        connection.execute(query)

        result = connection.execute(
            text("SELECT COUNT(*) FROM validated_products")
        )

        result_error = connection.execute(
            text("SELECT COUNT(case when error_reason is not null then 1 end)  FROM validated_products")
        )

        rows = result.scalar()

        rows_error = result_error.scalar()

        print("rows load in validated_table:", rows, "count error rows ", rows_error, "\n")

        query = text("""
                            INSERT INTO products (
                                product_id,
                                product_name,
                                price,
                                category,
                                rating
                           
                            )
                            select 
                                product_id,
                                product_name,
                                price,
                                category,
                                rating

                            from validated_products

                            WHERE error_reason IS NULL

                            ON CONFLICT (product_id)
                            DO UPDATE SET
                                product_name = EXCLUDED.product_name,
                                price = EXCLUDED.price,
                                category = EXCLUDED.category,
                                rating = EXCLUDED.rating
                          

                        """)

        connection.execute(query)

        query = text("""
                INSERT INTO error_products (
                product_id,
                product_name,
                price,
                category,
                rating,
                error_reason
                )
                SELECT 
                
                product_id,
                product_name,
                price,
                category,
                rating,
                error_reason

                from validated_products

                WHERE error_reason IS not NULL

                on conflict(product_id)
                do nothing

                """)

        connection.execute(query)

        result = connection.execute(
            text("SELECT COUNT(*) FROM error_products")
        )

        rows = result.scalar()

        print("rows load in error_table:", rows)

        query = text("""
            
             CREATE OR REPLACE VIEW  dm_products_sales as
            
             SELECT

                DISTINCT
                *,
                count(*) over(PARTITION BY category) as  total_products,
                
                round(avg(price)  over(PARTITION BY category),2) as avg_price,
                
                min(price) over(PARTITION BY category) as min_price,
                
                max(price) over(PARTITION BY category) as max_price,
                
                round(avg(rating) over(PARTITION BY category),2) as avg_rating



                from products 
                ORDER BY category
                  """)



        connection.execute(query)

        print("операция выполнена")



except Exception as e:

    raise


~~~
