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

        # ---------------------------------------------------------




except Exception as e:

    raise


~~~
