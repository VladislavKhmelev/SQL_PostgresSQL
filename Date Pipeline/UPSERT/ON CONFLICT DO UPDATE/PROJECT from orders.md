~~~
CSV → Python (pandas) → PostgreSQL

ON CONFLICT DO UPDATE

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


# ---------- LOGGING ----------

#!!!!!!!!
logging.basicConfig(
    filename=r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version2\logs\etl_orders.log",
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




# ---------- EXTRACT ----------

df = pd.read_csv(
    r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version1\data/orders.csv",
    parse_dates=["created_at"]
)
print("ALL Rows:", len(df))

logging.info("CSV загружен. Получено строк: %s", len(df))




# ---------- VALIDATION ----------

valid_column = df["amount"] >= 0

valid_date = df["created_at"] <= pd.Timestamp.now()

valid_nulls = df.notnull().all(axis=1)



valid_rows = (
    valid_column
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




# ---------- SAVE ERRORS ----------

#!!!!!!!
error_df.to_csv(
    r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version2\data\Error\error_orders.csv",
    index=False
)

logging.info(
    "Ошибочные строки сохранены в error_customers.csv"
)


# ---------- DATABASE CONNECTION ----------

with engine.connect() as connection:
    print("PostgreSQL connection successful")

logging.info("В PostgreSQL загружено строк: %s", len(clean_df))



# ---------- INSERT ----------
#!!!!!!!!!
query = text("""
    INSERT INTO orders (
        id,
        customer_id,
        product_id,
        amount,
        status,
        created_at
    )
    VALUES (
        :id,
        :customer_id,
        :product_id,
        :amount,
        :status,
        :created_at
    )
    ON CONFLICT (id)
    DO UPDATE SET
        customer_id = EXCLUDED.name,
        product_id = EXCLUDED.product_id,
        amount = EXCLUDED.amount,
        status = EXCLUDED.status,
        created_at = EXCLUDED.created_at
""")



data = clean_df.to_dict(orient="records")


try:
    with engine.begin() as connection:
        connection.execute(query, data)

    logging.info(
        "В PostgreSQL загружено строк: %s",
        len(data)
    )

except Exception as e:
    logging.error(
        "Ошибка загрузки в PostgreSQL: %s",
        e
    )
    # raise

# ---------- POST-LOAD CHECK ----------
#!!!!!!
query = text("""
    SELECT id
    FROM orders
""")

with engine.connect() as connection:
    result = connection.execute(query)

    loaded_ids = [row[0] for row in result]



print("Loaded IDs:", loaded_ids)

logging.info(
    "Проверка PostgreSQL: найдено строк: %s",
    len(loaded_ids)
)


expected_ids = clean_df["id"].tolist()

missing_ids = set(expected_ids) - set(loaded_ids)

if not missing_ids:
    print("All customers loaded successfully", missing_ids)
    logging.info("Проверка загрузки пройдена успешно")
else:
    print("Missing IDs:", missing_ids)
    logging.error("Не загружены ID: %s", missing_ids)



~~~
