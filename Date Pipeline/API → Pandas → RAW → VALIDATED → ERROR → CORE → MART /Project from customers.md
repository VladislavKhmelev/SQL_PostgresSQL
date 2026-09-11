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


url = "https://dummyjson.com/users"

response = requests.get(url)

print(response.status_code)

data = response.json()

# print(data)

users = data["users"]

# print(users)


# pd.set_option("display.max_columns", None)
# pd.set_option("display.max_colwidth", None)

pd.set_option("display.width", 2000)
pd.set_option("display.max_columns", None)


df = pd.DataFrame(users)




# здесь указываем data это наш ВЕСЬ JSON
# сохраняет в талицу черех csv - удобно смотреть df = ...users


# df = pd.DataFrame(data)
# df.to_csv("API_all_data_table_.csv", index=False)

# далее комментируем оба, и запускаем оба те что ниже
#-------------------------------------------------------------------------


# здесь указываем ключ (users) он находится в data -json, два csv в итоге надо сохранить, через data посмотреть какой нам клю нужен, и потом сохраняем по отдельности


# df = pd.DataFrame(users)
# df.to_csv("API_table_users.csv", index=False)




# для users:
print("Поля:", df.columns.tolist())



# Нам нужны наши поля:
#
# customer_id
# name
# email
# age
# city
# phone


df = df.rename(columns={
    "id": "customer_id",
    "firstName": "name"
})


# Достаём city из вложенного address
df["city"] = df["address"].apply(lambda x: x["city"])



# Теперь оставляем только нужные поля
df = df[
    [
        "customer_id",
        "name",
        "email",
        "age",
        "city",
        "phone"
    ]
]

print(df)
# print(df.dtypes)

# Это техническая подготовка данных:
#
# переименовали id → customer_id;
# переименовали firstName → name;
# достали city из вложенного address;
# оставили нужные поля.

# API → Pandas
# мы приводим данные к технически нужной структуре.


# RAW
# сохраняем полученный/подготовленный поток.


try:
    with engine.begin() as connection:

        connection.execute(
            text("TRUNCATE TABLE raw_customers")
        )


        df.to_sql(
            "raw_customers",
            connection,
            if_exists="append",
            index=False
        )

        print("raw rows load:", len(df))

        query = text("""
                    CREATE TABLE validated_customers AS
                    SELECT
                        customer_id::int,
                        name::text,
                        email::text,
                        age::int,
                        city::text,
                        phone::text,

                        NULLIF(
                            CONCAT_WS(
                                '; ',
                                CASE
                                    WHEN age < 18 or age > 100
                                    THEN 'age < 18 or age > 100'
                                END,

                                CASE
                                    WHEN customer_id IS NULL
                                    THEN 'customer_id is null'
                                END,

                                

                                CASE
                                    WHEN name is null
                                    THEN 'name is null'
                                END,

                                CASE
                                    WHEN email is null
                                    THEN 'email is null'
                                END,

                                CASE
                                    WHEN city is null
                                    THEN 'city is null'
                                END

                               
                            ),
                            ''
                        )::text AS error_reason

                    FROM raw_customers;
                """)

        connection.execute(
            text("DROP TABLE IF EXISTS validated_customers")
        )

        connection.execute(query)



        result = connection.execute(
            text("SELECT COUNT(*) FROM validated_customers")
        )

        result_error = connection.execute(
            text("SELECT COUNT(case when error_reason is not null then 1 end)  FROM validated_customers")
        )

        rows = result.scalar()

        rows_error = result_error.scalar()


        print("rows load in validated_table:", rows, "count error rows ", rows_error, "\n")



        query = text("""
                    INSERT INTO customers (
                        customer_id,
                        name,
                        email,
                        age,
                        city,
                        phone
                    )
                    select 
                        customer_id,
                        name,
                        email,
                        age,
                        city,
                        phone

                    from validated_customers
                    
                    WHERE error_reason IS NULL
                    
                    ON CONFLICT (customer_id)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        email = EXCLUDED.email,
                        age = EXCLUDED.age,
                        city = EXCLUDED.city,
                        phone = EXCLUDED.phone

                """)

        connection.execute(query)



        query = text("""
                        insert into error_customers (
                        customer_id,
                        name,
                        email,
                        age,
                        city,
                        phone,
                        error_reason
                            )
                        select 
                        customer_id,
                        name,
                        email,
                        age,
                        city,
                        phone,
                        error_reason

                            from validated_customers
                            
                            WHERE error_reason IS not NULL
                            
                            on conflict(customer_id)
                            do nothing

                        """)

        connection.execute(query)

        result = connection.execute(
            text("SELECT COUNT(*) FROM error_customers")
        )

        rows = result.scalar()

        print("rows load in error_table:", rows)

        query = text("""
                CREATE table dm_customer_sales as

                SELECT
                
                city,
                count(*) as count_
                
                
                from customers

                GROUP BY city   """)

        connection.execute(
            text("DROP TABLE IF EXISTS dm_customer_sales")
        )


        connection.execute(query)

        print("операция выполнена")



except Exception as e:

    raise






~~~
