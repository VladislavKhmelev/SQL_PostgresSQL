~~~
import os
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text

load_dotenv(r"C:\Users\Vladislav X\Desktop\proj\ver7_API\1.env")

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
)

# -----------------------------------

with engine.connect() as connection:
    result = connection.execute(text("SELECT 1"))

    if result.fetchone() == (1,):
        print("Подключен к PostgresSQL (SQLAlchemy)")

# -----------------------------------


connection = psycopg2.connect(
    user=user,
    password=password,
    host=host,
    port=port,
    dbname=database
)

file = r"C:\Users\Vladislav X\Desktop\proj\ver6\data\orders.csv"

try:
    with connection.cursor() as cursor:

        with open(file, "r", encoding="utf-8") as f:
            cursor.execute("""TRUNCATE TABLE raw_orders""")

            cursor.copy_expert(
                """
                COPY raw_orders (
                    order_id,
                    customer_id,
                    order_date,
                    product_category,
                    quantity,
                    unit_price,
                    status
                 
                )
                FROM STDIN
                WITH CSV HEADER
                """,
                f
            )

        connection.commit()

        cursor.execute(
            "SELECT COUNT(*) FROM raw_orders"
        )

        count = cursor.fetchone()[0]

        print("RAW_table: загружено:", count)


except Exception as e:
    connection.rollback()
    print("ошибка, откат", "\n")
    raise

try:
    with engine.begin() as connection:

        query = text("""
            CREATE TABLE validated_orders AS
            WITH tt AS (
                SELECT
                TRIM(order_id)::int  as order_id ,       
                TRIM(customer_id)::int  as customer_id     ,        
                
                LOWER(TRIM(product_category)::text    )  as product_category      ,   
                TRIM(quantity)::int    as quantity      , 
                TRIM(unit_price)::numeric(10,2)     as unit_price    ,     
                LOWER(TRIM(status)::text   )     as status ,

                    CASE
                        WHEN TRIM(order_date) ~ '^\\d{4}-\\d{2}-\\d{2}$'
                             AND SUBSTRING(TRIM(order_date), 6, 2)::int BETWEEN 1 AND 12
                             AND SUBSTRING(TRIM(order_date), 9, 2)::int BETWEEN 1
                                 AND EXTRACT(
                                     DAY FROM (
                                         DATE_TRUNC(
                                             'month',
                                             MAKE_DATE(
                                                 SUBSTRING(TRIM(order_date), 1, 4)::int,
                                                 SUBSTRING(TRIM(order_date), 6, 2)::int,
                                                 1
                                             )
                                             + INTERVAL '1 month'
                                         ) - INTERVAL '1 day'
                                     )
                                 )
                        THEN to_TIMESTAMP(TRIM(order_date), 'YYYY-MM-DD')

                        WHEN TRIM(order_date) ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$'
                             AND SUBSTRING(TRIM(order_date), 4, 2)::int BETWEEN 1 AND 12
                             AND SUBSTRING(TRIM(order_date), 1, 2)::int BETWEEN 1
                                 AND EXTRACT(
                                     DAY FROM (
                                         DATE_TRUNC(
                                             'month',
                                             MAKE_DATE(
                                                 SUBSTRING(TRIM(order_date), 7, 4)::int,
                                                 SUBSTRING(TRIM(order_date), 4, 2)::int,
                                                 1
                                             )
                                             + INTERVAL '1 month'
                                         ) - INTERVAL '1 day'
                                     )
                                 )
                        THEN to_TIMESTAMP(TRIM(order_date), 'DD.MM.YYYY')

                        WHEN TRIM(order_date) ~ '^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$'
                        THEN TO_TIMESTAMP(
                            TRIM(order_date),
                            'YYYY-MM-DD HH24:MI:SS'
                        )::DATE

                        WHEN TRIM(order_date) ~ '^\\d{2}/\\d{2}/\\d{4}$'
                        THEN to_TIMESTAMP(
                            TRIM(order_date),
                            'DD/MM/YYYY'
                        )

                        ELSE NULL
                    END AS order_date

                FROM raw_orders
            )

            SELECT
                order_id,
                customer_id,
                order_date,
                product_category,
                quantity,
                unit_price,
                status,

                NULLIF(
                    CONCAT_WS(
                        '; ',

                        CASE
                            WHEN order_id  IS NULL
                            THEN 'order_id  is null'
                        END,

                        CASE
                            WHEN customer_id  IS NULL
                            THEN 'customer_id  is null'
                        END,

                        CASE
                            WHEN order_date  IS NULL
                            THEN 'order_date  is null'
                        END,

                        CASE
                            WHEN quantity <= 0
                            THEN 'quantity <= 0'
                        END,

                        CASE
                            WHEN unit_price <= 0
                            THEN 'unit_price <= 0'
                        END,

                        CASE
                            WHEN status not in ('completed','cancelled','pending')
                            THEN 'status error'
                        END,
                        
                         CASE
                            WHEN customer_id not in (
                            SELECT
                            customer_id
                            from customers
                            )
                            THEN 'customer_id is OPRHAN'
                        END

                    ),
                    ''
                )::text AS error_reason

            FROM tt;
        """)

        connection.execute(
            text("DROP TABLE IF EXISTS validated_orders")
        )

        connection.execute(query)

        # VALIDATED_table проверка загрузки строк rows и rows_error
        # ---------------------------------------------------------

        result = connection.execute(
            text("SELECT COUNT(*) FROM validated_orders")
        )

        result_error = connection.execute(
            text("SELECT COUNT(case when error_reason is not null then 1 end)  FROM validated_orders")
        )

        rows = result.scalar()

        rows_error = result_error.scalar()

        print(f"VALIDATED_table: загружено строк", rows, "из них error:", f"{rows_error} / {rows}", "\n")
        # ---------------------------------------------------------

        query = text("""
                            INSERT INTO orders (
                                order_id,
                                customer_id,
                                order_date,
                                product_category,
                                quantity,
                                unit_price,
                                status

                            )
                            select 
                                order_id,
                                customer_id,
                                order_date,
                                product_category,
                                quantity,
                                unit_price,
                                status

                            from validated_orders

                            WHERE error_reason IS NULL

                            ON CONFLICT (order_id)
                            DO UPDATE SET
                                customer_id = EXCLUDED.customer_id,
                                order_date = EXCLUDED.order_date,
                                product_category = EXCLUDED.product_category,
                                quantity = EXCLUDED.quantity,
                                unit_price = EXCLUDED.unit_price,
                                status = EXCLUDED.status


                        """)

        connection.execute(query)

        query = text("""
                                INSERT INTO error_orders (
                                 order_id,
                                customer_id,
                                order_date,
                                product_category,
                                quantity,
                                unit_price,
                                status,
                                error_reason
                                    )
                                SELECT 
                                order_id,
                                customer_id,
                                order_date,
                                product_category,
                                quantity,
                                unit_price,
                                status,
                                error_reason

                                from validated_orders

                                WHERE error_reason IS not NULL

                                on conflict(order_id)
                                do nothing

                                """)

        connection.execute(query)

# customers и error_customers проверка загруженных строк
# --------------------------------------------------------
        result = connection.execute(
            text("SELECT COUNT(*) FROM orders")
        )

        result_error = connection.execute(
            text("SELECT COUNT(*)  FROM error_orders")
        )

        rows = result.scalar()

        rows_error = result_error.scalar()

        print(
            f"CORE_table and ERROR_table: загружено валидных строк: {rows}, "
            f"невалидных строк: {rows_error}\n"
            f"всего строк: {rows + rows_error}"
        )
# --------------------------------------------------------

        query = text("""
            
                CREATE OR REPLACE VIEW INNER_table AS
                
                
                
                SELECT
                customers.customer_id as customers_customer_id,
                customers.name as name,
              
                customers.city as city,
               
                
                
                orders.order_id  as  order_id,
                orders.customer_id as orders_customer_id ,
                orders.order_date  as order_date,
                orders.product_category   as product_category,
                orders.quantity  as quantity,
                orders.unit_price     as  unit_price,
                orders.status as status
                
                from customers
                LEFT JOIN orders
                on orders.customer_id = customers.customer_id
                
                
                
                        
                        
                           """)

        connection.execute(
            text("DROP VIEW IF EXISTS INNER_table")
        )


        connection.execute(query)

        print("транзакция выполнена")

except Exception as e:
    print("ошибка, откат")
    raise


~~~
