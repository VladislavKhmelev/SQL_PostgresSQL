~~~
CREATE table customers (
customer_id text,
customer_name text,
email text,
city text,
age text,
registration_date text
);


CREATE table orders (
order_id text,
customer_id text,
product_name text,
category text,
amount text,
status text,
created_at text
);


\copy customers FROM 'C:\Users\Vladislav X\Desktop\proj\ver10\data\customers.csv' WITH (FORMAT CSV, HEADER);


\copy orders FROM 'C:\Users\Vladislav X\Desktop\proj\ver10\data\orders.csv' WITH (FORMAT CSV, HEADER);



SELECT
registration_date


from customers;






#-----------

trim() 
INITCAP() 
lower() 
NULLIF(  ,'') 

NULLIF(  ,'')  trim() 
#---------

 ~ '^\d+$'    (для int)-
~ '^\d+(\.\d+)?$'    (для numeric) 


CREATE TABLE validated_customers AS
SELECT
  case when  NULLIF(  trim(customer_id)  ,'')    ~ '^\d+$'
  and NULLIF(  trim(customer_id)  ,'')::int >0
  then NULLIF(  trim(customer_id)  ,'')::int
  else null end as customer_id,

   NULLIF(trim(customer_name)   ,'') as   customer_name  ,
    
    NULLIF( trim(email)  ,'')  as  email ,
    
     NULLIF( trim(city) ,'')  as  city ,

    case when NULLIF( trim(age)  ,'')  ~ '^\d+$'
    and NULLIF( trim(age)  ,'')::int >0
    then NULLIF( trim(age)  ,'')::int
    else null end as age,
                     --дата date
--------------------------------------------------
     CASE
        WHEN TRIM(registration_date) ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TRIM(registration_date)::DATE

        WHEN TRIM(registration_date) ~ '^\d{2}\.\d{2}\.\d{4}$'
        THEN TO_DATE(
            TRIM(registration_date),
            'DD.MM.YYYY'
        )

        WHEN TRIM(registration_date) ~ '^\d{4}/\d{2}/\d{2}$'
        THEN TO_DATE(
            TRIM(registration_date),
            'YYYY/MM/DD'
        )

        ELSE NULL
    END AS registration_date,

     NULLIF(
    CONCAT_WS(
        '; ',
        CASE
            WHEN  NULLIF(TRIM(customer_id),'') IS NULL
                THEN 'customer_id is NULL'

            WHEN NULLIF(TRIM(customer_id), '')  !~ '^\d+$' 
                    THEN 'customer_id error format'

            WHEN  NULLIF(TRIM(customer_id),'')::int <= 0
                THEN 'customer_id <= 0'


        END,

        CASE
            WHEN  NULLIF(initcap(trim(customer_name)),'' ) IS NULL
                THEN 'customer_name is NULL'
        END,

        CASE

         WHEN   NULLIF(INITCAP(trim(email) ),'') is null
                THEN 'email is null'

            WHEN   NULLIF(INITCAP(trim(email) ),'') not like '%@%'
                THEN 'email not @'
        END,

        CASE
            WHEN  NULLIF(trim(city),''  )  IS NULL
                THEN 'city is NULL'
            end,
            
            CASE
            WHEN  NULLIF(trim(age),''  )  IS NULL
                THEN 'age is NULL'
            
              WHEN  NULLIF(trim(age),''  ) !~ '^\d+$'  
                THEN 'age error format'

                    WHEN  NULLIF(trim(age),''  )::int <=  0
                THEN 'age <= 0'

                end,
            
            
                     --дата date
----------------------------------------------
        CASE
                WHEN NULLIF(TRIM(registration_date), '') IS NULL
                    THEN 'registration_date is NULL'

                WHEN NULLIF(TRIM(registration_date), '') !~ '^\d{4}-\d{2}-\d{2}$'
                     AND NULLIF(TRIM(registration_date), '') !~ '^\d{2}\.\d{2}\.\d{4}$'
                     AND NULLIF(TRIM(registration_date), '') !~ '^\d{4}/\d{2}/\d{2}$'
                    THEN 'registration_date error format'
            END
       
        
    ),
    ''
) AS error_reason

FROM customers;









CREATE TABLE validated_orders AS
SELECT
 
  case when NULLIF( trim(order_id)   ,'')  ~ '^\d+$' 
  and NULLIF( trim(order_id)   ,'')::int >0
  then NULLIF( trim(order_id)   ,'')::int 
  else null end as    order_id,

   case when   NULLIF( trim(customer_id) ,'') ~ '^\d+$'
   and    NULLIF( trim(customer_id) ,'')::int >0
   then NULLIF( trim(customer_id) ,'')::int 
   else null end as  customer_id  ,

      NULLIF( trim(product_name)  ,'') as  product_name  ,
      NULLIF( trim(category) ,'')      as category,
      case when NULLIF(trim(amount)   ,'') ~ '^\d+(\.\d+)?$'   
      and NULLIF(trim(amount)   ,'')::numeric > 0
      then NULLIF(trim(amount)   ,'')::NUMERIC
      else null end as amount,


      NULLIF( trim(status) ,'') as  status   ,
     



                     -- дата timestamp
--------------------------------------------------
CASE
    WHEN TRIM(created_at) ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
        THEN TRIM(created_at)::TIMESTAMP

    WHEN TRIM(created_at) ~ '^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP(
            TRIM(created_at),
            'DD.MM.YYYY HH24:MI:SS'
        )::TIMESTAMP

    WHEN TRIM(created_at) ~ '^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$'
        THEN TO_TIMESTAMP(
            TRIM(created_at),
            'YYYY/MM/DD HH24:MI:SS'
        )::TIMESTAMP

    ELSE NULL
END AS created_at,

------------------------------------------------
     NULLIF(
    CONCAT_WS(
        '; ',
        CASE
            WHEN  NULLIF(TRIM(order_id),'') IS NULL
                THEN 'order_id is NULL'

            WHEN NULLIF(TRIM(order_id), '')  !~ '^\d+$' 
                    THEN 'order_id error format'

            WHEN  NULLIF(TRIM(order_id),'')::int <= 0
                THEN 'order_id <= 0'


        END,

         CASE
            WHEN  NULLIF(TRIM(customer_id),'') IS NULL
                THEN 'customer_id is NULL'

            WHEN NULLIF(TRIM(customer_id), '')  !~ '^\d+$' 
                    THEN 'customer_id error format'

            WHEN  NULLIF(TRIM(customer_id),'')::int <= 0
                THEN 'customer_id <= 0'


        END,

        CASE
            WHEN  NULLIF(initcap(trim(product_name)),'' ) IS NULL
                THEN 'product_name is NULL'
        END,

        CASE

         WHEN   NULLIF(INITCAP(trim(category) ),'') is null
                THEN 'category is null'

        END,

        CASE
            WHEN  NULLIF(trim(amount),''  )  IS NULL
                THEN 'amount is NULL'

            WHEN  NULLIF(trim(amount),''  )  !~ '^\d+(\.\d+)?$'
                THEN 'amount error format'
            
            WHEN  NULLIF(trim(amount),''  )::numeric  <= 0
                THEN 'amount <= 0'
            end,
            
            CASE
            WHEN  NULLIF(trim(status),''  )  IS NULL
                THEN 'status is NULL'
            
              WHEN  NULLIF(trim(status),''  ) not in ('completed','processing','cancelled') 
                THEN 'status error status'

                end,

            
            
                     -- дата ~timestamp
------------------------------------------------
CASE
    WHEN NULLIF(TRIM(created_at), '') IS NULL
        THEN 'created_at is NULL'

    WHEN NULLIF(TRIM(created_at), '') !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
         AND NULLIF(TRIM(created_at), '') !~ '^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}$'
         AND NULLIF(TRIM(created_at), '') !~ '^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$'
        THEN 'created_at error format'
END
       
        
    ),
    ''
) AS error_reason

FROM orders;



CREATE table error_customers (
    error_id SERIAL PRIMARY key,
customer_id int,
customer_name text,
email text,
city text,
age int,
registration_date date,
error_reason text


);



CREATE table error_orders (
    error_id SERIAL PRIMARY key,
order_id int,
customer_id int,
product_name text,
category text,
amount numeric,
status text,
created_at TIMESTAMP,
error_reason text
);




CREATE table core_customers (

customer_id int PRIMARY KEY,
customer_name text,
email text,
city text,
age int,
registration_date date


);


CREATE table core_orders (

order_id int PRIMARY key,
customer_id int,
product_name text,
category text,
amount numeric,
status text,
created_at TIMESTAMP

);


INSERT INTO error_customers (

customer_id ,
customer_name ,
email ,
city ,
age ,
registration_date ,
error_reason 
)
SELECT

 customer_id ,
customer_name ,
email ,
city ,
age ,
registration_date ,
error_reason 


FROM validated_customers


WHERE error_reason IS NOT NULL;




INSERT INTO error_orders (

order_id,
customer_id,
product_name ,
category ,
amount ,
status ,
created_at ,
error_reason 
)
SELECT

order_id,
customer_id,
product_name ,
category ,
amount ,
status ,
created_at ,
error_reason 


FROM validated_orders


WHERE error_reason IS NOT NULL;




INSERT INTO core_customers (
customer_id ,
customer_name ,
email ,
city ,
age ,
registration_date 

)
SELECT
customer_id ,
customer_name ,
email ,
city ,
age ,
registration_date 

FROM (
    select
    *,  -- тут уже лежит error_reason

    ROW_NUMBER() over(PARTITION BY customer_id ORDER BY registration_date DESC) as r


    from validated_customers

) tt 

where r = 1 and error_reason is null



on conflict(customer_id)

DO UPDATE SET

customer_name = EXCLUDED.customer_name,
email = EXCLUDED.email,
city = EXCLUDED.city,
age = EXCLUDED.age,
registration_date = EXCLUDED.registration_date;







INSERT INTO core_orders (
order_id,
customer_id,
product_name ,
category ,
amount ,
status ,
created_at 

)
SELECT
order_id,
customer_id,
product_name ,
category ,
amount ,
status ,
created_at 

FROM (
    select
    *,  -- тут уже лежит error_reason

    ROW_NUMBER() over(PARTITION BY order_id ORDER BY created_at DESC) as r


    from validated_orders

) tt 

where r = 1 and error_reason is null



on conflict(order_id)

DO UPDATE SET

customer_id = EXCLUDED.customer_id,
product_name = EXCLUDED.product_name,
category = EXCLUDED.category,
amount = EXCLUDED.amount,
status = EXCLUDED.status,
created_at = EXCLUDED.created_at;



сирот ищем
#---------------------------------
SELECT
core_orders.order_id,
core_orders.product_name
from core_orders
LEFT JOIN core_customers
on core_customers.customer_id = core_orders.customer_id

where core_customers.customer_id is null;


 order_id |  product_name
----------+-----------------
     1039 | Unknown Product
(1 строка)



DELETE from core_orders
where order_id = 1039;




INSERT INTO error_orders (

order_id,
customer_id,
product_name ,
category ,
amount ,
status ,
created_at ,
error_reason 
)
SELECT

order_id,
customer_id,
product_name ,
category ,
amount ,
status ,
created_at ,
error_reason 


FROM validated_orders


WHERE order_id = 1039;


подписали столбец а то пусто было там 
#-----
update error_orders
SET error_reason  = 'orhan'
where  order_id = 1039;





1. Python → PostgreSQL

Подключись к PostgreSQL через SQLAlchemy и получи из core_orders все данные в Pandas DataFrame.

Задача: получить df.




import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
)


# Забираем core_orders в DataFrame

query = """
SELECT *
FROM core_orders
"""

df = pd.read_sql(query, engine)

print(df)
print(df.info())




2. Задание для Pandas

После загрузки core_orders в df сделай простую трансформацию:

Добавь новый столбец amount_with_tax, где сумма увеличена на 20%.

То есть:

amount_with_tax = amount * 1.20



добавляем 
---------------------------

df["amount_with_tax"] = (df["amount"] * 1.20).round(2)

print(df.head(5))



-------------------------------

После этого дам следующий шаг — загрузку обработанного df обратно в PostgreSQL.

CREATE table python_orders (
order_id int,
customer_id int,
product_name text,
category text,
amount NUMERIC,
status text,
created_at TIMESTAMP,
amount_with_tax NUMERIC
);



перенесли в таблицу PostgresSQL
#------------------------
df.to_sql(
    name="python_orders",
    con=engine,
    if_exists="replace",
    index=False
)

print("загружено")



Дальше переходим к финальному слою — Data Mart.


CREATE table orders_mart as 

SELECT

category,

COUNT(python_orders.order_id) as orders_count,
COALESCE(sum(amount),0) as total_amount,
round(COALESCE(avg(amount)::NUMERIC,0),2) as avg_amount,
COALESCE(sum(amount_with_tax),0) as total_amount_with_tax


from python_orders

GROUP BY category;



~~~
