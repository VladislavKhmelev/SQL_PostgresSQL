~~~
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd



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
        print("Подключен к PostgresSQL (SQLAlchemy)")

# -----------------------------------


df = pd.read_json(r"C:\Users\Vladislav X\Desktop\proj\ver7\data\orders.json")

df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

#!!!!!ловим пустые строки, ВАЖНО!!!
# Pandas превратил их в NA, а при загрузке в PostgreSQL они попадут как NULL.

df = df.replace(r'^\s*$', pd.NA, regex=True)

print(df)
# print(df.dtypes)

#!!!!!!replace ставим - сперва очистит таблицу, потом вставит
try:

        df.to_sql(
        "raw_orders",
        engine,
        if_exists="replace",
        index=False
    )

        print("загрузили в RAW: ", len(df))

except Exception as e:
    print("ошибка, откат")
    raise


try:
    with engine.begin() as connection:

        query1 = text("""
                    INSERT INTO validated_orders (
                    
                    order_id,  
                     customer_id,     
                     customer_name,    
                     city, 
                     product_category,   
                     amount,      
                     status,          
                     created_at,
                     error_reason
                    )
                    SELECT
                     order_id,  
                     customer_id,     
                     customer_name,    
                     city, 
                     product_category,   
                     amount,      
                     status,          
                     created_at,
        
                    NULLIF(
                        CONCAT_WS(
                            '; ',
                            CASE
                                WHEN order_id is null or order_id <= 0
                                THEN 'order_id is null or order_id <= 0 '
                            END,
    
                            CASE
                                WHEN customer_id is null or customer_id <= 0
                                THEN ' customer_id is null or customer_id <= 0'
                            END,
    
                            CASE
                                WHEN customer_name is null
                                THEN 'customer_name is null'
                            END,
    
                            CASE
                                WHEN city  is null
                                THEN 'city  is null'
                            END,
    
    
    
                            CASE
                                WHEN product_category  is null
                                THEN 'product_category  is null'
                            END,
                            
                            CASE
                                WHEN amount is null or amount <= 0
                                THEN 'amount is null or amount <= 0'
                            END,
                            
                            CASE
                                WHEN status is null or status not in ('completed','processing','cancelled')
                                THEN 'status error'
                            END,
                            
                              CASE
                                WHEN created_at IS NULL
                                THEN 'created_at error'
                            END,
                            
                            CASE
                                WHEN COUNT(*) OVER (PARTITION BY order_id) > 1
                                THEN 'order_id DUPLICATE'
                            END
                        ),
                        ''
                    )::text AS error_reason
    
                FROM raw_orders;
            """)

        connection.execute(
            text("TRUNCATE TABLE validated_orders")
        )

        connection.execute(query1)


                            #POST LOAD CHECK
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



# DDL: тут есть важная особенность когда есть дубликация
# и сперва загружаем в error_orders все ошибки и дубликаты и потом уже оттуда достаем, так наш код прописан чтоб оттуда доставать
# --------------------------------------------------
# CREATE TABLE error_orders (
#     error_id SERIAL PRIMARY KEY,      (новое поле!!!! + serial (это автокремент)
#     order_id int,
#     customer_id int,
#     customer_name text,
#     city text,
#     product_category text,
#     amount numeric(10,2),
#     status text,
#     created_at timestamp,
#     error_reason text
# );
# --------------------------------------------------


        query = text("""
        INSERT INTO error_orders (
        order_id,         -- тут другой serial не пишем, он автокремент
        customer_id,     
        customer_name,    
        city, 
        product_category,   
        amount,      
        status,          
        created_at,
        error_reason
        )
        SELECT 
        order_id,  
        customer_id,     
        customer_name,    
        city, 
        product_category,   
        amount,      
        status,          
        created_at,
        error_reason
        
        from validated_orders
        
        WHERE error_reason IS not NULL
     
        on conflict (error_id)      -- ключ другой
        
        do nothing
        
        
        """)

        connection.execute(
            text("TRUNCATE TABLE error_orders")
        )



        connection.execute(query)

        query = text("""
                        INSERT INTO orders (
                        
                        order_id,
                        customer_id,
                        customer_name,
                        city,
                        product_category,
                        amount,
                        status,
                        created_at
                            )
                            
                        SELECT
                        
                        order_id,
                        customer_id,
                        customer_name,
                        city,
                        product_category,
                        amount,
                        status,
                        created_at
            
                    FROM validated_orders
                    WHERE error_reason IS NULL
            
                    UNION ALL
            
                    SELECT
                        order_id,
                        customer_id,
                        customer_name,
                        city,
                        product_category,
                        amount,
                        status,
                        created_at
                    FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (                         -- выбираем по позднему времени наш дубликат
                        PARTITION BY order_id
                        ORDER BY created_at DESC
                    ) AS r
                FROM error_orders
                WHERE error_reason = 'order_id DUPLICATE'           -- название ошибки обязательно 
            ) tt
            WHERE r = 1

                ON CONFLICT (order_id)
        
                DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    customer_name = EXCLUDED.customer_name,
                    city = EXCLUDED.city,
                    product_category = EXCLUDED.product_category,
                    amount = EXCLUDED.amount,
                    status = EXCLUDED.status,
                    created_at = EXCLUDED.created_at;
                        """)

        connection.execute(query)

        #POST LOAD CHECK
        # ---------------------------------------------------------
        result_error = connection.execute(
            text("SELECT COUNT(*) FROM error_orders")
        )

        result_core = connection.execute(
            text("SELECT COUNT(*) FROM orders")
        )

        rows = result_error.scalar()

        rows2 = result_core.scalar()

        print("загружено в error_table (возможность есть дубликаты):", rows)

        print("загружено в core_table:", rows2)

        # ---------------------------------------------------------

        query = text("""
                     create table orders_mart  as 
                        
                        SELECT
                        
                        city,
                        
                        count(order_id) as order_count,
                        COALESCE(sum(amount),0)  as total_amount,
                        round(COALESCE(avg(amount),0),2) as avg_amount,
                        
                        count(case when status = 'completed' then 1 end) as completed_count,
                        count(case when status = 'cancelled ' then 1 end) as cancelled_count,
                        
                        count(DISTINCT customer_id) as customer_count
                        
                        
                        from orders
                        
                        GROUP BY city;
                                        """)


        connection.execute(text("DROP TABLE if exists orders_mart"))


        connection.execute(query)

        print("аналитическая таблица создана")





except Exception as e:
    print("ошибка, откат")
    raise



# ==============================================================================================
# ===============================================================================================
#
# проверка вконце!!!!
#  Схема  |            Имя            |        Тип         | Владелец
# --------+---------------------------+--------------------+----------
#  public | error_orders              | таблица            | postgres
#  public | error_orders_error_id_seq | последовательность | postgres
#  public | orders                    | таблица            | postgres
#  public | orders_mart               | таблица            | postgres
#  public | raw_orders                | таблица            | postgres
#  public | validated_orders          | таблица            | postgres
# (6 строк)
#
#
#
# SELECT 'raw_orders' AS table_name, COUNT(*) AS rows_count
# FROM raw_orders
#
# UNION ALL
#
# SELECT 'validated_orders', COUNT(*)
# FROM validated_orders
#
# UNION ALL
#
# SELECT 'error_orders', COUNT(*)
# FROM error_orders
#
# UNION ALL
#
# SELECT 'orders', COUNT(*)
# FROM orders
#
# UNION ALL
#
# SELECT 'orders_mart', COUNT(*)
# FROM orders_mart;
#
#
#
#     table_name    | rows_count
# ------------------+------------
#  raw_orders       |         10
#  validated_orders |         10
#  error_orders     |          6
#  orders           |          5
#  orders_mart      |          5
# (5 строк)
#
#
#
# Проверяем дубликаты order_id в ERROR
#
# SELECT
#     order_id,
#     COUNT(*) AS cnt
# FROM error_orders
# GROUP BY order_id
# HAVING COUNT(*) > 1
# ORDER BY order_id;
#
#
#
#
#  order_id | cnt
# ----------+-----
#      1008 |   2
# (1 строка)
#

~~~
