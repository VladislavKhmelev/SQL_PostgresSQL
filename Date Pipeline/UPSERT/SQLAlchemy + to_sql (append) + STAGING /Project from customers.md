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


logging.basicConfig(
    filename=r"C:\Users\Vladislav X\Desktop\proj\ver3\logs\etl_customers.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
)


# Тогда Pandas будет стараться вывести всю таблицу целиком, без ... по столбцам.
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)

logger.info("ETL started")

df = pd.read_csv(
    r"C:\Users\Vladislav X\Desktop\proj\ver3\data\customers.csv"
)



df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
df.columns = df.columns.str.strip()

df["name"] = df["name"].str.title()
df["city"] = df["city"].str.title()
df["phone"] = df["phone"].astype("string")
df["registered_at"] = pd.to_datetime(df["registered_at"], errors="coerce")

numeric_columns = [
    "customer_id",
    "age"
]
for column in numeric_columns:
    df[column] = pd.to_numeric(
        df[column],
        errors="coerce"
    )

print(df)
# print(df.dtypes)
# print(df.isna().sum())


print("ALL Rows:", len(df))
logger.info("Data transformation completed")
logger.info(
    f"Null values: {df.isna().sum().sum()}"
)
logger.info(f"CSV loaded: {len(df)} rows")

#-----------------------------------

with engine.connect() as connection:
    result = connection.execute(text("SELECT 1"))

    if result.fetchone() == (1,):
        print("Подключен")

# -----------------------------------


try:

    with engine.begin() as connection:
        connection.execute(
            text("TRUNCATE TABLE staging_customers")
        )



        df.to_sql(
            "staging_customers",
            connection,
            if_exists="append",
            index=False
        )

        logger.info(
            f"Loaded {len(df)} rows into staging"
        )

        connection.execute(
            text("DROP TABLE IF EXISTS validated_customers")
        )

        connection.execute(
            text("""
                CREATE TABLE validated_customers AS
                SELECT
                    *,
                    NULLIF(
                        CONCAT_WS(
                            '; ',
                            CASE WHEN customer_id IS NULL
                                 THEN 'customer_id is null' END,
                                 
                            CASE WHEN name is null
                                 THEN 'name is null' END,
                                 
                            CASE WHEN email IS NULL
                                 THEN 'email IS NULL' END,
                                 
                            CASE WHEN age < 18 or age > 100
                                 THEN 'age not 18-100' END,
                                 
                                 CASE WHEN city not in ('Ufa', 'Kazan')
                                 THEN 'city problem' END,
                                 
                                 
                                 
                            CASE WHEN registered_at > CURRENT_TIMESTAMP
                                 THEN 'future date' END,
                                 
                            CASE WHEN status NOT IN ('active', 'blocked')
                                 THEN 'invalid status' END,
                                 
                                 CASE WHEN phone is null
                                 THEN 'phone is null' END
                        ),
                        ''
                    ) AS error_reason
                FROM staging_customers;
            """)

        )


        result = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM validated_customers
                WHERE error_reason IS NOT NULL
            """)
        )

        error_count = result.scalar()


        logger.info(
            f"Validation completed: {error_count} invalid rows"
        )

        clean_count = len(df) - error_count

        print(clean_count, "- чистых строк")




        connection.execute(
            text("truncate table error_customers")
        )

        connection.execute(
            text("""
                    INSERT INTO error_customers (
                        customer_id,
                        name,
                        email,
                        phone,
                        age,
                        city,
                        registered_at,
                        status,
                        error_reason
                    )
                    SELECT
                        customer_id,
                        name,
                        email,
                        phone,
                        age,
                        city,
                        registered_at,
                        status,
                        error_reason
                    FROM validated_customers
                    WHERE error_reason IS NOT NULL;
                """)
        )

        result = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM error_customers
            """)
        )

        error_count = result.scalar()

        print(error_count, "- невалидных строк")

        logger.info(
            f"Error rows loaded: {error_count}"
        )





        connection.execute(
            text("""
                INSERT INTO customers (
                    customer_id,
                    name,
                    email,
                    phone,
                    age,
                    city,
                    registered_at,
                    status
                )
                SELECT
                    customer_id,
                    name,
                    email,
                    phone,
                    age,
                    city,
                    registered_at,
                    status
                FROM validated_customers
                WHERE error_reason IS NULL
                ON CONFLICT (customer_id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    age = EXCLUDED.age,
                    city = EXCLUDED.city,
                    registered_at = EXCLUDED.registered_at,
                    status = EXCLUDED.status;
            """)
        )

        logger.info("data file UPSERT completed")

        result = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM validated_customers
                WHERE error_reason IS NULL
            """)
        )

        expected_count = result.scalar()

        result = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM customers c
                JOIN validated_customers v
                    ON c.customer_id = v.customer_id
                WHERE v.error_reason IS NULL
            """)
        )

        loaded_count = result.scalar()

        if loaded_count != expected_count:
            print("error ошибка, разница загрузки id не совпадает")
            raise Exception(
                f"Post-load check failed: "
                f"expected {expected_count}, loaded {loaded_count}"
            )


        logger.info(
            f"Post-load check passed: {loaded_count} clean rows loaded"
        )

        f = loaded_count - expected_count
        print(f, " - разница сколько загрузили id и было в clean_df")




except Exception:
    logger.exception("ETL failed")
    raise

~~~
