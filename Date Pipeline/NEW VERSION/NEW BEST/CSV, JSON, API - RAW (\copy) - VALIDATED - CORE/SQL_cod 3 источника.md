~~~
CREATE table raw_products (
    product_id text,
    product_name text,
    category text,
    price text,
    stock text
);

\copy raw_products FROM 'C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\обработка через pandas file\products.csv' WITH (FORMAT csv, HEADER true);

CREATE Table raw_orders (
    order_id text,
    customer_id text,
    product_id text,
    quantity text,
    order_date text,
    status text
);

\copy raw_orders FROM 'C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\обработка через pandas file\orders.csv' WITH (FORMAT csv, HEADER true);

CREATE TABLE raw_customers (
    customer_id text,
    customer_name text,
    email text,
    age text,
    city text
);

\copy raw_customers FROM 'C:\Users\VladK\OneDrive\Desktop\proj\ver10\data\обработка через pandas file\customers.csv' WITH (FORMAT csv, HEADER true);

NULLIF(  ,'')  trim() 
#---------

 ~ '^\d+$'    (для int)
 ~ '^\d+(\.\d+)?$'    (для numeric)

CREATE TABLE validated_products AS


SELECT

    CASE
        WHEN NULLIF(TRIM(product_id), '') ~ '^\d+$'
            THEN NULLIF(TRIM(product_id), '')::int
        ELSE NULL
    END AS product_id,

    NULLIF(TRIM(product_name), '') AS product_name,

    NULLIF(TRIM(category), '') AS category,

    CASE
        WHEN NULLIF(TRIM(price), '') ~ '^\d+(\.\d+)?$'
            THEN NULLIF(TRIM(price), '')::numeric
        ELSE NULL
    END AS price,

    CASE
        WHEN NULLIF(TRIM(stock), '') ~ '^\d+$'
            THEN NULLIF(TRIM(stock), '')::int
        ELSE NULL
    END AS stock,

    NULLIF(
        CONCAT_WS(
            '; ',

            CASE
                WHEN NULLIF(TRIM(product_id), '') IS NULL
                    THEN 'product_id is NULL'

                WHEN NULLIF(TRIM(product_id), '') !~ '^\d+$'
                    THEN 'product_id error format'

                WHEN NULLIF(TRIM(product_id), '')::int <= 0
                    THEN 'product_id <= 0'
                
                
 WHEN count(*) over(PARTITION BY NULLIF(TRIM(product_id), '')::int) > 1
                    THEN 'product_id is dub'

            END,

            CASE
                WHEN NULLIF(TRIM(product_name), '') IS NULL
                    THEN 'product_name is NULL'
            END,

            CASE
                WHEN NULLIF(TRIM(category), '') IS NULL
                    THEN 'category is NULL'
            END,

            CASE
                WHEN NULLIF(TRIM(price), '') IS NULL
                    THEN 'price is NULL'

                WHEN NULLIF(TRIM(price), '') !~ '^\d+(\.\d+)?$'
                    THEN 'price not format'

                WHEN NULLIF(TRIM(price), '')::numeric <= 0
                    THEN 'price <= 0'
            END,

            CASE
                WHEN NULLIF(TRIM(stock), '') IS NULL
                    THEN 'stock is NULL'

                WHEN NULLIF(TRIM(stock), '') !~ '^\d+$'
                    THEN 'stock error format'

                WHEN NULLIF(TRIM(stock), '')::int < 0
                    THEN 'stock < 0'
            END

        ),
        ''
    ) AS error_reason

FROM raw_products;

NULLIF(  ,'')  trim() 
#---------

 ~ '^\d+$'    (для int)
 ~ '^\d+(\.\d+)?$'    (для numeric)

CREATE table validated_orders AS


SELECT


case when NULLIF( trim(order_id) ,'') ~ '^\d+$'
then    NULLIF( trim(order_id)   ,'')::int
else null end as order_id,

 case when NULLIF( trim(customer_id) ,'')    ~ '^\d+$'
 then   NULLIF( trim(customer_id) ,'') ::int
else null end as customer_id,

case when NULLIF( trim(product_id) ,'') ~ '^\d+$'
then NULLIF( trim(product_id) ,'') ::int
else null end as product_id,

case when NULLIF( trim(quantity) ,'')   ~ '^\d+$' 
then   NULLIF( trim(quantity) ,'') ::int
else null end as quantity,

CASE
    WHEN NULLIF(TRIM(order_date), '') ~ '^\d{4}-\d{2}-\d{2}$'
        THEN NULLIF(TRIM(order_date), '')::date

    WHEN NULLIF(TRIM(order_date), '') ~ '^\d{2}\.\d{2}\.\d{4}$'
        THEN to_date(
            NULLIF(TRIM(order_date), ''),
            'DD.MM.YYYY'
        )

    WHEN NULLIF(TRIM(order_date), '') ~ '^\d{4}/\d{2}/\d{2}$'
        THEN to_date(
            NULLIF(TRIM(order_date), ''),
            'YYYY/MM/DD'
        )

    ELSE NULL
END AS order_date, 

NULLIF(  trim(status),'')    as status,


NULLIF(
    CONCAT_WS(
        '; ',
        CASE
            WHEN  NULLIF( trim(order_id)   ,'') is null
                THEN 'order_id is NULL'
            
            WHEN NULLIF( trim(order_id)   ,'')  !~ '^\d+$' 
                    THEN 'order_id error format'
            
            WHEN NULLIF( trim(order_id)   ,'')::int <= 0
                    THEN 'order_id <=0'
            
             WHEN count(*) over(PARTITION BY NULLIF(TRIM(order_id), '')::int) > 1
                    THEN 'product_id is dub'

     
        END,

           CASE
            WHEN  NULLIF( trim(customer_id) ,'') is null
                THEN 'customer_id is NULL'
            
            WHEN NULLIF( trim(customer_id) ,'') !~ '^\d+$' 
                    THEN 'customer_id error format'
            
            WHEN NULLIF( trim(customer_id) ,'')::int <= 0
                    THEN 'customer_id <=0'
            
            WHEN NULLIF(TRIM(customer_id), '') ~ '^\d+$'
                    AND NULLIF(TRIM(customer_id), '')::int NOT IN (
                        SELECT customer_id
                        FROM validated_customers
                    )
                THEN 'orphan'
                    
            END,

          CASE
            WHEN  NULLIF(  trim(product_id)  ,'') is null
                THEN 'product_id is NULL'
            
            WHEN NULLIF(  trim(product_id)  ,'') !~ '^\d+$' 
                    THEN 'product_id error format'
            
            WHEN NULLIF(  trim(product_id)  ,'')::int <= 0
                    THEN 'product_id <=0'

             WHEN NULLIF(TRIM(product_id), '') ~ '^\d+$'
                    AND NULLIF(TRIM(product_id), '')::int NOT IN (
                        SELECT product_id
                        FROM validated_customers
                    )
                THEN 'orphan'
     
        END,

          CASE
            WHEN  NULLIF( trim(quantity) ,'') is null
                THEN 'quantity is NULL'
            
            WHEN NULLIF( trim(quantity) ,'') !~ '^\d+$' 
                    THEN 'quantity error format'
            
            WHEN NULLIF( trim(quantity) ,'')::int <= 0
                    THEN 'quantity <=0'
     
        END,

      CASE
            WHEN  NULLIF(TRIM(order_date), '') is null
                THEN 'order_date is NULL'
            
              
            
            
            WHEN NULLIF(TRIM(order_date), '') !~ '^\d{4}-\d{2}-\d{2}$'
            AND NULLIF(TRIM(order_date), '') !~ '^\d{2}\.\d{2}\.\d{4}$'
            AND NULLIF(TRIM(order_date), '') !~ '^\d{4}/\d{2}/\d{2}$'
                THEN 'order_date error format'

              WHEN  NULLIF(TRIM(order_date), '')::date > CURRENT_DATE
                THEN 'order_date is future'
          
     
        END,

          CASE
            WHEN  NULLIF(  trim(status),'') is null
                THEN 'status is NULL'
            
             WHEN  NULLIF(  trim(status),'') not in ('completed','cancelled')
                THEN 'status error format'

        END,
        
    ),
    ''
) AS error_reason

FROM raw_orders;





NULLIF(  ,'')  trim() 
#---------

 ~ '^\d+$'            (для int)
 ~ '^\d+(\.\d+)?$'        (для numeric)


CREATE TABLE validated_customers AS 

SELECT

    CASE 
        WHEN NULLIF(TRIM(customer_id), '') ~ '^\d+$' 
            THEN NULLIF(TRIM(customer_id), '')::int
        ELSE NULL 
    END AS customer_id,

    NULLIF(TRIM(customer_name), '') AS customer_name,

    NULLIF(TRIM(email), '') AS email,

    CASE 
        WHEN NULLIF(TRIM(age), '') ~ '^\d+$' 
            THEN NULLIF(TRIM(age), '')::int
        ELSE NULL 
    END AS age,

    NULLIF(TRIM(city), '') AS city,

    NULLIF(
        CONCAT_WS(
            '; ',

            CASE
                WHEN NULLIF(TRIM(customer_id), '') IS NULL
                    THEN 'customer_id is NULL'
                
                WHEN NULLIF(TRIM(customer_id), '') !~ '^\d+$' 
                    THEN 'customer_id error format'
                
                WHEN NULLIF(TRIM(customer_id), '')::int <= 0
                    THEN 'customer_id <= 0'

                WHEN COUNT(*) OVER (
                    PARTITION BY NULLIF(TRIM(customer_id), '')::int
                ) > 1
                    THEN 'customer_id is dub'
            END,

            CASE
                WHEN NULLIF(TRIM(customer_name), '') IS NULL
                    THEN 'customer_name is NULL'
            END,

            CASE
                WHEN NULLIF(TRIM(email), '') IS NULL
                    THEN 'email is NULL'
                
                WHEN NULLIF(TRIM(email), '') NOT LIKE '%@%'
                    THEN 'email not @'
            END,

            CASE 
                WHEN NULLIF(TRIM(age), '') IS NULL
                    THEN 'age is NULL'

                WHEN NULLIF(TRIM(age), '') !~ '^\d+$'
                    THEN 'age error format'

                WHEN NULLIF(TRIM(age), '')::int < 18
                    THEN 'age < 18'
            END,

            CASE
                WHEN NULLIF(TRIM(city), '') IS NULL
                    THEN 'city is NULL'
            END

        ),
        ''
    ) AS error_reason

FROM raw_customers;


CREATE Table core_products (
    product_id int PRIMARY KEY,
    product_name text not NULL,
    category text not null,
    price numeric check (price > 0),
    stock int check (stock > 0)
);



CREATE Table error_products (
    error_id serial PRIMARY KEY,
    product_id int,
    product_name text,
    category text,
    price numeric,
    stock int,
    error_reason text
);


insert into
    error_products (
        product_id,
        product_name,
        category,
        price,
        stock,
        error_reason
    )

SELECT
    product_id,
    product_name,
    category,
    price,
    stock,
    error_reason
from validated_products
where
    error_reason is not null;





insert into core_products (
        product_id,
        product_name,
        category,
        price,
        stock

    )

SELECT
    product_id,
    product_name,
    category,
    price,
    stock

from validated_products
where
    error_reason is null

on conflict (product_id)
do update
set 
product_name = EXCLUDED.product_name,
category = EXCLUDED.category,
price = EXCLUDED.price,
stock = EXCLUDED.stock;




----------------------------------------



CREATE Table core_customers (
customer_id int PRIMARY KEY,     
customer_name text not NULL,                             
email text not NULL,  
age int NOT NULL,           
city text NOT NULL
);



CREATE Table error_customers (
    error_id serial PRIMARY KEY,
    customer_id int,
    customer_name text,
    email text,
    age int,
    city text,
    error_reason text
);

insert into
    error_customers (
        customer_id,
        customer_name,
        email,
        age,
        city,
        error_reason
    )
SELECT
      customer_id,
        customer_name,
        email,
        age,
        city,
    error_reason
from validated_customers
where
    error_reason is not null;





insert into core_customers (
       customer_id,
        customer_name,
        email,
        age,
        city

    )

SELECT
  customer_id,
        customer_name,
        email,
        age,
        city

from validated_customers
where
    error_reason is null

on conflict (customer_id)
do update
set 
customer_name = EXCLUDED.customer_name,
email = EXCLUDED.email,
age = EXCLUDED.age,
city = EXCLUDED.city;




--------------------------------------------------------






CREATE Table core_orders (
order_id int not NULL,  
customer_id int NOT NULL, 
product_id int NOT NULL, 
quantity int NOT NULL, 
order_date date NOT null,     
status text NOT null,

CONSTRAINT fk_cd FOREIGN KEY (customer_id) REFERENCES core_customers(customer_id),
CONSTRAINT fk_pd FOREIGN KEY (product_id) REFERENCES core_products(product_id),

PRIMARY KEY (order_id, product_id)

);



CREATE Table error_orders (
    error_id serial PRIMARY KEY,
    order_id int,
    customer_id int,
    product_id int,
    quantity int,
    order_date date,
    status text,
    error_reason text
);

insert into
    error_orders (

        order_id,
        customer_id,
        product_id,
        quantity,
        order_date,
        status,
        error_reason
    )
SELECT

        order_id,
        customer_id,
        product_id,
        quantity,
        order_date,
        status,
        error_reason

from validated_orders

where error_reason is not null;





insert into core_orders (
     order_id,
        customer_id,
        product_id,
        quantity,
        order_date,
        status

    )

SELECT
   order_id,
        customer_id,
        product_id,
        quantity,
        order_date,
        status

from validated_orders
where
    error_reason is null

on conflict (order_id, product_id)
do update
set 
customer_id = EXCLUDED.customer_id,
quantity = EXCLUDED.quantity,
order_date = EXCLUDED.order_date,
status = EXCLUDED.status;


# ----------проверка что нет строк core_orders и error_orders
-- # JOIN используется для проверки наличия совпадения, а не для того, чтобы вывести данные обеих таблиц.


SELECT
    core_orders.order_id,
    core_orders.product_id
FROM core_orders
INNER JOIN error_orders
    ON error_orders.order_id = core_orders.order_id
   AND error_orders.product_id = core_orders.product_id;


   # core_orders
#        ↓
# берём строку
#        ↓
# ищем такую же строку
#        ↓
# error_orders
#        ↓
# ┌───────────────┐
# │ есть совпадение? │
# └───────┬───────┘
#         │
#    ┌────┴────┐
#    ↓         ↓
#   ДА         НЕТ
#    ↓         ↓
# показать   не показать



Дальше — DATA MART. Основные RAW → VALIDATION → ERROR/CORE у тебя уже собраны.

~~~
