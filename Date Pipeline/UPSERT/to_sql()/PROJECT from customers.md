~~~
CSV → Python (pandas) → PostgreSQL

to_sql()

customers.csv

id,name,email,age,status,created_at
1,Alex,alex@mail.com,25,active,2026-09-01
2,Anna,anna@mail.com,32,active,2026-09-01
3,John,john@mail.com,17,active,2026-09-02
4,Mike,mike@mail.com,40,blocked,2026-09-02
5,Lisa,lisa@mail.com,28,active,2026-09-02
6,Peter,peter@mail.com,-5,active,2026-09-03
7,Sarah,sarah@mail.com,29,pending,2026-09-03
8,Tom,tom@mail.com,35,active,2026-09-03
9,Emma,emma@mail.com,22,deleted,2026-09-04
10,David,david@mail.com,999,active,2026-09-04

-----------------------------------------------------------------

import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text
import logging



# ---------- LOGGING ----------

#!!!!version5\logs\etl_customers.log
logging.basicConfig(
    filename=r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version5\logs\etl_customers.log",
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
    r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version1\data/customers.csv",
    parse_dates=["created_at"]
)
print("ALL Rows:", len(df))

logging.info("CSV загружен. Получено строк: %s", len(df))



# ---------- VALIDATION ----------

valid_age = df["age"] > 0

valid_date = df["created_at"] <= pd.Timestamp.now()

valid_nulls = df.notnull().all(axis=1)



valid_rows = (
    valid_age
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
    r"C:\Users\Vladislav X\Desktop\etl_project\data\UPSERT\version5\data\Error\error_customers.csv",
    index=False
)

logging.info(
    "Ошибочные строки сохранены в error_customers.csv"
)



# ---------- DATABASE CONNECTION ----------

try:

    clean_df.to_sql(
        "customers",
        engine,
        if_exists="append",
        index=False
    )

    logging.info(
        "to_sql успешно выполнен. Загружено строк: %s",
        len(clean_df)
    )

except Exception as e:

    logging.error(
        "Ошибка to_sql: %s",
        e
    )

    raise





# ---------- POST-LOAD CHECK ----------
#!!!!!!!!!!FROM customers
query = text("""
    SELECT COUNT(*)
    FROM customers
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


finally:

    connection.close()








~~~
