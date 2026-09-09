~~~
CSV → Python (pandas) → PostgreSQL

COPY

orders.csv


id,customer_id,product_id,amount,status,created_at
1,1,10,5000,completed,2026-09-01
2,2,11,7000,completed,2026-09-01
3,1,12,3000,cancelled,2026-09-01
4,3,10,8000,completed,2026-09-02
5,2,11,6000,completed,2026-09-02
6,5,13,4500,completed,2026-09-03
7,8,14,6200,completed,2026-09-03

-----------------------------------------------------------------


import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text
import logging
import psycopg2


# ---------- LOGGING ----------

#!!!!etl_orders
logging.basicConfig(
    filename=r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version4\logs\etl_orders.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------- DATABASE ----------


load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")


engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
)


connection = psycopg2.connect(
    user=user,
    password=password,
    host=host,
    port=port,
    dbname=database
)




# ---------- EXTRACT ----------
#!!!!!!!!!!orders.csv
df = pd.read_csv(
    r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version1\data/orders.csv",
    parse_dates=["created_at"]
)
print("ALL Rows:", len(df))

logging.info("CSV загружен. Получено строк: %s", len(df))



# ---------- VALIDATION ----------
#!!!!!!!!df["amount"] >= 0
valid_rows_df = df["amount"] >= 0

valid_date = df["created_at"] <= pd.Timestamp.now()

valid_nulls = df.notnull().all(axis=1)



valid_rows = (
    valid_rows_df
    &
    valid_date
    &
    valid_nulls
)


# ---------- CLEAN / ERROR ----------

clean_df = df[valid_rows]

error_df = df[~valid_rows]

print("Clean rows:", len(clean_df))
print("Error rows:", len(error_df))

logging.info("Корректных строк: %s", len(clean_df))
logging.warning("Ошибочных строк: %s", len(error_df))




# ---------- SAVE ERRORS and SAVE clean_df----------

#!!!!!!!!!error_orders
error_df.to_csv(
    r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version4\data\Error\error_orders.csv",
    index=False
)
# !!!!!!!
logging.info(
    "Ошибочные строки сохранены в error_orders.csv"
)

# !!!!!!!
clean_df.to_csv(
    r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version4\data\clean_orders.csv",
    index=False
)


# ---------- DATABASE CONNECTION ----------

try:

    with connection.cursor() as cursor:
        with open(
                # !!!!!!!
                r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version4\data\clean_orders.csv",
            "r",
            encoding="utf-8"
        ) as file:
#!!!!!!!!
            cursor.copy_expert(
                """
                COPY orders (
                    id,
                    customer_id,
                    product_id,
                    amount,
                    status,
                    created_at
                )
                FROM STDIN
                WITH CSV HEADER
                """,
                file
            )

    connection.commit()

    logging.info(
        "COPY успешно выполнен. Загружено строк: %s",
        len(clean_df)
    )

except Exception as e:

    connection.rollback()

    logging.error(
        "Ошибка COPY: %s",
        e
    )

    raise

finally:

    connection.close()





# ---------- POST-LOAD CHECK ----------
#!!!!!!!!!!
query = text("""
    SELECT COUNT(*)
    FROM orders
""")

with engine.connect() as connection:
    result = connection.execute(query)

    loaded_count = result.scalar()

print("Loaded rows:", loaded_count)

expected_count = len(clean_df)

if loaded_count == expected_count:
    print("All Clean rows loaded successfully")
    logging.info("COPY: все строки загружены")
else:
    print("Expected:", expected_count)
    print("Loaded:", loaded_count)
    logging.error(
        "COPY: количество строк не совпадает"
    )









~~~
