~~~
CSV → Python (pandas) → PostgreSQL

staging → UPSERT

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
# #todo ---- CSV → Python → PostgreSQL
# ----------------------------------------


# #todo Будем использовать два CSV:
#
# customers.csv
# orders.csv
#
# ----------------------------------------------
#

# #todo  и построим:
#
# CSV
#  ↓
# Pandas
#  ↓
# Проверка
#  ↓
# Очистка
#  ↓
# PostgreSQL
#  ↓
# INSERT
#  ↓
# Проверка загрузки
# -------------------------------------

import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text
import logging



# ---------- LOGGING ----------

#!!!!version5\logs\etl_customers.log
logging.basicConfig(
    filename=r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version6\logs\etl_orders.log",
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
#!!!!!
df = pd.read_csv(
    r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version1\data/orders.csv",
    parse_dates=["created_at"]
)
print("ALL Rows:", len(df))

logging.info("CSV загружен. Получено строк: %s", len(df))



# ---------- VALIDATION ----------
#!!!!!!
valid_rows_df = df["amount"] > 0

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

#!!!!!!!!!version5\data\Error\error_customers.csv
error_df.to_csv(
    r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version6\data\Error\error_orders.csv",
    index=False
)
#!!!!!
logging.info(
    "Ошибочные строки сохранены в error_orders.csv"
)





# ---------- STAGING → CUSTOMERS ----------
#!!!!!! INSERT INTO customers  + колонки +  FROM staging_customers + ON CONFLICT (id) + name = EXCLUDED.name, ...
query = text("""
    INSERT INTO orders (
        id,
        customer_id,
        product_id,
        amount,
        status,
        created_at
    )
    SELECT
        id,
        customer_id,
        product_id,
        amount,
        status,
        created_at
    FROM staging_orders          
    ON CONFLICT (id)
    DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        product_id = EXCLUDED.product_id,
        amount = EXCLUDED.amount,
        status = EXCLUDED.status,
        created_at = EXCLUDED.created_at
""")


try:
#!!!!!   text("TRUNCATE TABLE staging_orders")
    with engine.begin() as connection:
        # Очищаем staging перед новой загрузкой
        connection.execute(
            text("TRUNCATE TABLE staging_orders")
        )
        # Загружаем свежие данные в staging
        #!!!!!!
        clean_df.to_sql(
            "staging_orders",
            connection,
            if_exists="append",
            index=False
        )




        # Переносим staging → customers
        connection.execute(query)
#!!!!!!!UPSERT staging → customers
        logging.info(
            "UPSERT staging → orders успешно выполнен"
        )


except Exception as e:

    logging.error(
        "Ошибка ETL: %s",
        e
    )

    raise



# ---------- POST-LOAD CHECK ----------
#!!!!!!!!!!FROM customers ( это верно)
query = text("""
    SELECT COUNT(*)
    FROM orders;
""")

with engine.connect() as connection:
    result = connection.execute(query)

    loaded_count = result.scalar()

print("Loaded rows:", loaded_count)

expected_count = len(clean_df)
#!!!!!!to_sql   ----->>>>staging → UPSERT
if loaded_count == expected_count:
    print("All Clean rows loaded successfully")
    logging.info("staging → UPSERT: все строки загружены")
else:
    print("Expected:", expected_count)
    print("Loaded:", loaded_count)
    logging.error(
        "staging → UPSERT: количество строк не совпадает"
    )




# ---------- LOGGING ----------

#!!!!version5\logs\etl_customers.log
logging.basicConfig(
    filename=r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version6\logs\etl_orders.log",
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
#!!!!!!!!!
df = pd.read_csv(
    r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version1\data/orders.csv",
    parse_dates=["created_at"]
)
print("ALL Rows:", len(df))

logging.info("CSV загружен. Получено строк: %s", len(df))



# ---------- VALIDATION ----------
#!!!!!!!
valid_rows_df = df["amount"] > 0

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

#!!!!!!!!!version5\data\Error\error_customers.csv
error_df.to_csv(
    r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version6\data\Error\error_orders.csv",
    index=False
)

logging.info(
    "Ошибочные строки сохранены в error_customers.csv"
)



# ---------- DATABASE CONNECTION ----------

try:
#!!!!!!!!
    clean_df.to_sql(
        "staging_orders;",
        engine,
        if_exists="append",
        index=False
    )
#!!!!!!!to_sql успешно выполнен
    logging.info(
        "UPSERT успешно выполнен. Загружено строк: %s",
        len(clean_df)
    )

except Exception as e:

    logging.error(
        "Ошибка UPSERT: %s",
        e
    )

    raise





# ---------- POST-LOAD CHECK ----------
#!!!!!!!!!!FROM customers
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
    logging.info("to_sql: все строки загружены")
else:
    print("Expected:", expected_count)
    print("Loaded:", loaded_count)
    logging.error(
        "to_sql: количество строк не совпадает"
    )








~~~
