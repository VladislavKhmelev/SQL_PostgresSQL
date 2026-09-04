~~~
Архитектура для ETL:

# ┌──────────────────────────────────────────┐
# │ CSV                                      │
# │  ↓                                       │
# │ Pandas                                   │
# │  ↓                                       │
# │ DataFrame                                │
# │  ↓                                       │
# │ Validation                               │
# │  ↓                                       │
# │ clean_df                                 │
# │  ↓                                       │
# │ PostgreSQL                               │
# │  ↓                                       │
# │ orders                                   │
# └──────────────────────────────────────────┘




import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
load_dotenv()
import logging
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text, bindparam



user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")


logging.basicConfig(
    filename=r"C:\Users\Vladislav X\Desktop\etl_project\logs/etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s")


df = pd.read_csv(r"C:\Users\Vladislav X\Desktop\etl_project\data\1.csv", parse_dates=["created_at"])

logging.info("CSV успешно прочитан. Строк: %s", len(df))

def check_duplicate_ids(df):

    bad_ids = df[df["id"].duplicated(keep=False)]

    if not bad_ids.empty:
        return {
            "type": "duplicate ids",
            "count": len(bad_ids),
            "rows": bad_ids
        }

    return None


def check_negative_amount(df):

    bad_amount = df[df["amount"] < 0]

    if not bad_amount.empty:
        return {
            "type": "negative amount",
            "count": len(bad_amount),
            "rows": bad_amount
        }

    return None


def check_status(df):

    allowed_statuses = [
        "completed",
        "cancelled"
    ]

    bad_status = df[
        ~df["status"].isin(allowed_statuses)
    ]

    if not bad_status.empty:
        return {
            "type": "invalid status",
            "count": len(bad_status),
            "rows": bad_status
        }

    return None


def check_future_dates(df):

    bad_dates = df[
        df["created_at"] > pd.Timestamp.now()
    ]

    if not bad_dates.empty:
        return {
            "type": "future dates",
            "count": len(bad_dates),
            "rows": bad_dates
        }

    return None


def validate_orders(df):

    errors = []

    checks = [
        check_duplicate_ids,
        check_negative_amount,
        check_status,
        check_future_dates
    ]

    for check in checks:

        error = check(df)

        if error is not None:
            errors.append(error)

    return errors

errors = validate_orders(df)

if errors:
    print("Data is invalid")
    logging.warning("Data is invalid")

    for error in errors:
        print("Error:", error["type"])
        print("Problem rows:", error["count"])
        print(error["rows"])

        logging.warning(
            "Ошибка: %s, проблемных строк: %s",
            error["type"],
            error["count"]
        )

        error["rows"].to_csv(
            r"C:\Users\Vladislav X\Desktop\etl_project\data\error_1.csv",
            index=False
        )

else:
    print("Data is valid")
    logging.info("Data is valid")


error["rows"].to_csv(r"C:\Users\Vladislav X\Desktop\etl_project\data\error_1.csv",index=False)

allowed_statuses = [
    "completed",
    "cancelled"
]

clean_df = df[
    (df["amount"] >= 0)
    &
    (df["status"].isin(allowed_statuses))
]

clean_df.to_csv(r"C:\Users\Vladislav X\Desktop\etl_project\data\clean_1.csv",index=False)

logging.info("Очищенный DataFrame создан. Строк: %s", len(clean_df))



engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert

metadata = MetaData()

orders = Table(
    "orders",
    metadata,
    autoload_with=engine
)

logging.info("Начинаем загрузку данных в PostgreSQL")

with engine.connect() as connection:
    print("Подключение успешно")



try:

    clean_df.to_sql(
        "orders",
        engine,
        if_exists="append",
        index=False
    )

    logging.info("Данные успешно загружены")

except Exception as e:

    logging.error("Ошибка загрузки: %s", e)


# print(host)

stmt = insert(orders).values(
    clean_df.to_dict(orient="records")
)

stmt = stmt.on_conflict_do_update(
    index_elements=["id"],
    set_={
        "customer_id": stmt.excluded.customer_id,
        "product_id": stmt.excluded.product_id,
        "amount": stmt.excluded.amount,
        "status": stmt.excluded.status,
        "created_at": stmt.excluded.created_at
    }
)

etl_status = "SUCCESS"
missing_ids = set()
try:

    with engine.begin() as connection:
        connection.execute(stmt)

    ids = clean_df["id"].tolist()

    with engine.connect() as connection:

        query = text("""
            SELECT id
            FROM orders
            WHERE id IN :ids
        """).bindparams(
            bindparam("ids", expanding=True)
        )

        result = connection.execute(
            query,
            {"ids": ids}
        )

        loaded_ids = [
            row[0]
            for row in result
        ]

    missing_ids = set(ids) - set(loaded_ids)

    if not missing_ids:

        logging.info(
            "Проверка загрузки пройдена. Все ID присутствуют в PostgreSQL."
        )

    else:

        etl_status = "FAILED"

        logging.error(
            "Не загружены ID: %s",
            missing_ids
        )

except Exception as e:

    etl_status = "FAILED"

    logging.error(
        "Ошибка ETL: %s",
        e
    )

print(missing_ids)
print("ETL status:", etl_status)



Резюмируем:

# ┌──────────────────────────────────────────────────────────┐
# │ Если совсем коротко, архитектура такая:                 │
# │                                                          │
# │ 1. Extract                                               │
# │                                                          │
# │ df = pd.read_csv(...)                                    │
# │                                                          │
# │ → Забираем CSV.                                          │
# │                                                          │
# │ 2. Validate                                              │
# │                                                          │
# │ → Проверяем технические и бизнес-правила.               │
# │                                                          │
# │ 3. Split                                                 │
# │                                                          │
# │ clean_df                                                 │
# │ error_df                                                 │
# │                                                          │
# │ → Правильные строки отделяем от ошибочных.               │
# │                                                          │
# │ 4. Error handling                                        │
# │                                                          │
# │ error_df → error_orders.csv                              │
# │                                                          │
# │ → Ошибочные строки сохраняем отдельно.                   │
# │                                                          │
# │ 5. Load                                                  │
# │                                                          │
# │ clean_df → PostgreSQL                                    │
# │                                                          │
# │ 6. Idempotency                                           │
# │                                                          │
# │ PRIMARY KEY (id)                                         │
# │        +                                                 │
# │ ON CONFLICT                                               │
# │                                                          │
# │ → Чтобы повторная загрузка не создавала дубликаты.        │
# │                                                          │
# │ 7. Transaction                                           │
# │                                                          │
# │ COMMIT / ROLLBACK                                        │
# │                                                          │
# │ → Чтобы загрузка была атомарной.                         │
# │                                                          │
# │ 8. Post-load validation                                  │
# │                                                          │
# │ → Проверяем, что загруженные id действительно            │
# │   присутствуют в БД.                                     │
# │                                                          │
# │ 9. Logging                                               │
# │                                                          │
# │ etl.log                                                  │
# │                                                          │
# │ Фиксируем:                                               │
# │                                                          │
# │ - сколько строк получили;                                │
# │ - сколько валидных;                                      │
# │ - сколько ошибочных;                                     │
# │ - началась ли загрузка;                                  │
# │ - успешно ли загрузили;                                  │
# │ - были ли ошибки;                                        │
# │ - итоговый SUCCESS / FAILED.                             │
# └──────────────────────────────────────────────────────────┘





~~~
