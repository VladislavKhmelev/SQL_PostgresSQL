~~~
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text
import logging



load_dotenv(r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\1.env")

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
        print("Подключен к PostgresSQL", '\n')

# -----------------------------------



pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)


df = pd.read_csv(r"C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\customers.csv")


# print(df.dtypes)


df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
df.columns = df.columns.str.strip()

df = df.astype(object).where(pd.notna(df), None)  # из NaN будет Null в PostgreSQL
# print(df)


file = df.to_dict(orient="records")


query = text("""
    INSERT INTO raw_customers (
        customer_id,
        name,
        email,
        age,
        city

    )
    VALUES (
        :customer_id,
        :name,
        :email,
        :age,
        :city
    )
""")


try:

    with engine.begin() as connection:

        connection.execute(
            text("TRUNCATE TABLE raw_customers")
        )

        connection.execute(query, file)

        print("загрузили в RAW: ", len(df))




        query = text(r"""
                    CREATE Table validated_customers as


SELECT

 case when NULLIF(trim(customer_id)     ,'')    ~ '^\d+$'
 then NULLIF(trim(customer_id)     ,'')::int 
 else null end as    customer_id,

 NULLIF( trim(name)    ,'')  as      name  ,   

 NULLIF( trim(email)  ,'')      as email,

 case when NULLIF(  trim(age)  ,'')  ~ '^\d+$'
 then NULLIF(  trim(age)  ,'')::int 

 else null end as  age,

 NULLIF(  trim(city) ,'')  as city,

NULLIF(
    CONCAT_WS(
        '; ',
        CASE
            WHEN  NULLIF(trim(customer_id)     ,'') is null
                THEN 'customer_id is NULL'
            
            WHEN NULLIF(TRIM(customer_id), '')  !~ '^\d+$' 
                    THEN 'customer_id error format'
            
            WHEN NULLIF(TRIM(customer_id), '')::int <= 0
                    THEN 'customer_id <=0'
     
        END,

        CASE
            WHEN  NULLIF( trim(name)    ,'') is null
                THEN 'name is NULL'
        END,

        CASE
            WHEN    NULLIF( trim(email)  ,'') IS NULL
                THEN 'email is NULL'
            
             WHEN    NULLIF( trim(email)  ,'') not like '%@%'
                THEN 'email not @'

            
        END,

        case 

        when NULLIF(  trim(age)  ,'') is null
            then 'age is null'

        when NULLIF(  trim(age)  ,'')  !~ '^\d+$'
            then 'age error format'

        when NULLIF(  trim(age)  ,'')::int < 18
            then 'age < 18'

        end,

             CASE
            WHEN  NULLIF(  trim(city) ,'') is null
                THEN 'city is NULL'
            
        END
        
    ),
    ''
) AS error_reason

FROM raw_customers;
                """)

        connection.execute(
            text("DROP TABLE IF EXISTS validated_customers")
        )

        connection.execute(query)

        # POST LOAD CHECK
        # ---------------------------------------------------------

        result = connection.execute(
            text("SELECT COUNT(*) FROM validated_customers")
        )

        result_error = connection.execute(
            text("SELECT COUNT(case when error_reason is not null then 1 end)  FROM validated_customers")
        )

        rows = result.scalar()

        rows_error = result_error.scalar()

        print(f"VALIDATED_table: загружено строк", rows, "из них error:", f"{rows_error} / {rows}", "\n")
        # ---------------------------------------------------------

        query = text("""
                INSERT INTO error_customers (
                customer_id,         -- тут другой serial не пишем, он автокремент
                name,     
                email,    
                age, 
                city,
                error_reason
                )
                SELECT 
                customer_id,         
                name,     
                email,    
                age, 
                city,
                error_reason

                from validated_customers

                WHERE error_reason IS not NULL



                """)

        connection.execute(
            text("TRUNCATE TABLE error_customers")
        )

        connection.execute(query)

                        # POST LOAD CHECK
        #--------------------------------------------------
        result = connection.execute(
            text("SELECT COUNT(*) FROM error_customers")
        )

        rows = result.scalar()

        print("ERROR_TABLE: загружено строк:", rows, '\n')

        # --------------------------------------------------

        query = text("""
                               INSERT INTO core_customers (

                               customer_id,
                               name,
                               email,
                               age,
                               city
                     
                                   )

                               SELECT

                               customer_id,
                               name,
                               email,
                               age,
                               city

                           FROM validated_customers
                           WHERE error_reason IS NULL

                          

                       ON CONFLICT (customer_id)

                       DO UPDATE SET
                           name = EXCLUDED.name,
                           email = EXCLUDED.email,
                           age = EXCLUDED.age,
                           city = EXCLUDED.city
                       
                               """)

        connection.execute(query)

        # POST LOAD CHECK
        # ---------------------------------------------------------

        result_core = connection.execute(
            text("SELECT COUNT(*) FROM core_customers")
        )


        rows2 = result_core.scalar()


        print("CORE_table загружено строк:", rows2)
        print("--------------------------------------")

        # ---------------------------------------------------------


#================заказы======================

# загрузил через \copy  ( raw_orders )



except Exception as e:

    raise



try:

    with engine.begin() as connection:

        query = text(r"""
                          CREATE TABLE validated_orders AS

SELECT

    CASE
        WHEN NULLIF(TRIM(order_id), '') ~ '^\d+$'
            THEN NULLIF(TRIM(order_id), '')::int
        ELSE NULL
    END AS order_id,


    CASE
        WHEN NULLIF(TRIM(customer_id), '') ~ '^\d+$'
            THEN NULLIF(TRIM(customer_id), '')::int
        ELSE NULL
    END AS customer_id,


    CASE
        WHEN NULLIF(TRIM(car_id), '') ~ '^\d+$'
            THEN NULLIF(TRIM(car_id), '')::int
        ELSE NULL
    END AS car_id,


    CASE
        WHEN NULLIF(TRIM(days), '') ~ '^\d+$'
            THEN NULLIF(TRIM(days), '')::int
        ELSE NULL
    END AS days,


    CASE
        WHEN NULLIF(TRIM(daily_price), '') ~ '^\d+$'
            THEN NULLIF(TRIM(daily_price), '')::int
        ELSE NULL
    END AS daily_price,


    CASE
        WHEN NULLIF(TRIM(total_amount), '') ~ '^\d+(\.\d+)?$'
            THEN NULLIF(TRIM(total_amount), '')::numeric
        ELSE NULL
    END AS total_amount,


    NULLIF(TRIM(status), '') AS status,


    CASE
        WHEN NULLIF(TRIM(order_date), '') ~ '^\d{4}-\d{2}-\d{2}$'
            THEN NULLIF(TRIM(order_date), '')::date

        WHEN NULLIF(TRIM(order_date), '') ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_DATE(
                NULLIF(TRIM(order_date), ''),
                'DD.MM.YYYY'
            )

        WHEN NULLIF(TRIM(order_date), '') ~ '^\d{4}/\d{2}/\d{2}$'
            THEN TO_DATE(
                NULLIF(TRIM(order_date), ''),
                'YYYY/MM/DD'
            )

        ELSE NULL
    END AS order_date,


    NULLIF(
        CONCAT_WS(
            '; ',

            CASE
                WHEN NULLIF(TRIM(order_id), '') IS NULL
                    THEN 'order_id is NULL'

                WHEN NULLIF(TRIM(order_id), '') !~ '^\d+$'
                    THEN 'order_id error format'

                WHEN NULLIF(TRIM(order_id), '')::int <= 0
                    THEN 'order_id <=0'
            END,


            CASE
                WHEN NULLIF(TRIM(customer_id), '') IS NULL
                    THEN 'customer_id is NULL'

                WHEN NULLIF(TRIM(customer_id), '') !~ '^\d+$'
                    THEN 'customer_id error format'

                WHEN NULLIF(TRIM(customer_id), '')::int <= 0
                    THEN 'customer_id <=0'
                    
      
                WHEN NULLIF(TRIM(customer_id), '') ~ '^\d+$'
                     AND NULLIF(TRIM(customer_id), '')::int NOT IN (
                         SELECT customer_id
                         FROM core_customers
                     )
                THEN 'orphan'

            END,


            CASE
                WHEN NULLIF(TRIM(car_id), '') IS NULL
                    THEN 'car_id is NULL'

                WHEN NULLIF(TRIM(car_id), '') !~ '^\d+$'
                    THEN 'car_id error format'

                WHEN NULLIF(TRIM(car_id), '')::int <= 0
                    THEN 'car_id <=0'
            END,


            CASE
                WHEN NULLIF(TRIM(days), '') IS NULL
                    THEN 'days is NULL'

                WHEN NULLIF(TRIM(days), '') !~ '^\d+$'
                    THEN 'days error format'

                WHEN NULLIF(TRIM(days), '')::int <= 0
                    THEN 'days <=0'
            END,


            CASE
                WHEN NULLIF(TRIM(daily_price), '') IS NULL
                    THEN 'daily_price is NULL'

                WHEN NULLIF(TRIM(daily_price), '') !~ '^\d+$'
                    THEN 'daily_price error format'

                WHEN NULLIF(TRIM(daily_price), '')::int <= 0
                    THEN 'daily_price <=0'
            END,


            CASE
                WHEN NULLIF(TRIM(total_amount), '') IS NULL
                    THEN 'total_amount is NULL'

                WHEN NULLIF(TRIM(total_amount), '') !~ '^\d+(\.\d+)?$'
                    THEN 'total_amount error format'

                WHEN NULLIF(TRIM(total_amount), '')::numeric <= 0
                    THEN 'total_amount <=0'

                WHEN NULLIF(TRIM(total_amount), '')::numeric
                     != NULLIF(TRIM(days), '')::int
                        * NULLIF(TRIM(daily_price), '')::int
                    THEN 'total_amount != days * daily_price'
            END,


            CASE
                WHEN NULLIF(TRIM(status), '') IS NULL
                    THEN 'status is NULL'

                WHEN NULLIF(TRIM(status), '')
                     NOT IN ('completed', 'cancelled', 'pending')
                    THEN 'status is error'
            END,


            CASE
                WHEN NULLIF(TRIM(order_date), '') IS NULL
                    THEN 'order_date is NULL'

                WHEN NULLIF(TRIM(order_date), '') !~ '^\d{4}-\d{2}-\d{2}$'
                 AND NULLIF(TRIM(order_date), '') !~ '^\d{2}\.\d{2}\.\d{4}$'
                 AND NULLIF(TRIM(order_date), '') !~ '^\d{4}/\d{2}/\d{2}$'
                    THEN 'order_date error format'

                WHEN NULLIF(TRIM(order_date), '')::date > CURRENT_DATE
                    THEN 'order_date is future'
            END

        ), 
        ''
    ) AS error_reason 

FROM raw_orders;

                        """)

        connection.execute(
            text("DROP TABLE IF EXISTS validated_orders")
        )

        connection.execute(query)

        # POST LOAD CHECK
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
                        INSERT INTO error_orders (
                        order_id,         
                        customer_id,     
                        car_id,    
                        days, 
                        daily_price,
                        total_amount,
                        status,
                        order_date,
                        error_reason
                        )
                        SELECT 
                          order_id,         
                        customer_id,     
                        car_id,    
                        days, 
                        daily_price,
                        total_amount,
                        status,
                        order_date,
                        error_reason

                        from validated_orders

                        WHERE error_reason IS not NULL



                        """)

        connection.execute(
            text("TRUNCATE TABLE error_orders")
        )

        connection.execute(query)

        # POST LOAD CHECK
        # --------------------------------------------------
        result = connection.execute(
            text("SELECT COUNT(*) FROM error_orders")
        )

        rows = result.scalar()

        print("ERROR_TABLE: загружено строк:", rows, '\n')

        # --------------------------------------------------

        query = text("""
                                       INSERT INTO core_orders (

                                      order_id,         
                                        customer_id,     
                                        car_id,    
                                        days, 
                                        daily_price,
                                        total_amount,
                                        status,
                                        order_date
                                      

                                           )

                                       SELECT

                                       order_id,         
                                        customer_id,     
                                        car_id,    
                                        days, 
                                        daily_price,
                                        total_amount,
                                        status,
                                        order_date
                                    

                                   FROM validated_orders
                                   WHERE error_reason IS NULL



                               ON CONFLICT (order_id)

                               DO UPDATE SET
                                   customer_id = EXCLUDED.customer_id,
                                   car_id = EXCLUDED.car_id,
                                   days = EXCLUDED.days,
                                   daily_price = EXCLUDED.daily_price,
                                   total_amount = EXCLUDED.total_amount,
                                   status = EXCLUDED.status,
                                   order_date = EXCLUDED.order_date

                                       """)


        connection.execute(query)

        # POST LOAD CHECK
        # ---------------------------------------------------------

        result_core = connection.execute(
            text("SELECT COUNT(*) FROM core_orders")
        )

        rows2 = result_core.scalar()

        print("CORE_table загружено строк:", rows2, '\n')

        # ---------------------------------------------------------

        connection.execute(
            text("  DROP TABLE IF EXISTS dm_customer_orders;")
        )

        connection.execute(text("""
                  CREATE TABLE dm_customer_orders AS

                    SELECT
                        core_customers.customer_id AS номер,
                        core_customers.name AS имя,
                        core_customers.city AS город,
                    
                        COUNT(order_id) AS order_count,
                    
                        COALESCE(SUM(total_amount), 0) AS total_amount,
                    
                        ROUND(
                            COALESCE(AVG(total_amount), 0),
                            2
                        ) AS avg_order_amount
                    
                    FROM core_customers
                    
                    LEFT JOIN core_orders
                        ON core_orders.customer_id = core_customers.customer_id
                    
                    GROUP BY
                        core_customers.customer_id,
                        core_customers.name,
                        core_customers.city;
               """))

        # POST LOAD CHECK   (LEFT JOIN)
        # --------------------------------------------------
        result_1 = connection.execute(
            text("SELECT COUNT(*) FROM core_customers")
        )

        result = connection.execute(
            text("SELECT COUNT(*) FROM dm_customer_orders")
        )

        rows = result.scalar()
        rows_1 = result_1.scalar()
        print('------------------------------------------------')
        print("DATA MART: загружено строк после LEFT JOIN:", rows)
        print(rows_1, 'строк левой таблицы')

        # --------------------------------------------------

except Exception as e:

    raise

~~~
