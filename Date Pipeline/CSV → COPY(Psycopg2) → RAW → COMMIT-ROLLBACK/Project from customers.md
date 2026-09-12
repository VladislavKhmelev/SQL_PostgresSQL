~~~
import os
import psycopg2
from dotenv import load_dotenv
import logging
import pandas as pd
from io import StringIO

load_dotenv(r"C:\Users\Vladislav X\Desktop\proj\ver7_API\1.env")


user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")


connection = psycopg2.connect(
    user=user,
    password=password,
    host=host,
    port=port,
    dbname=database
)

# engine = create_engine(
#     f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
# )

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

        print("загружено в raw:", count)


except Exception as e:
    connection.rollback()
    print("ошибка, откат")
    raise


~~~
