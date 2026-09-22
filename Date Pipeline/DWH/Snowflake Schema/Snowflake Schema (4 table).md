~~~
CREATE table dim_cities (
city_key serial PRIMARY KEY,
city_id int,
city_name text,
country text,

CONSTRAINT cd_2 UNIQUE (city_id)
);



CREATE table dim_customers (
customer_key serial PRIMARY KEY,
customer_id int,
customer_name text ,   
email text,
age int,
city_key int,    ----- вместо city_id --->>> city_key Потому что в Snowflake Schema мы связываем Dimension с Dimension через их surrogate key, а не через исходный city_id.

CONSTRAINT cd UNIQUE (customer_id)
);


--- То есть между CORE и Dimension используем natural key, а между Dimension и Dimension — surrogate key.
--- То есть UNIQUE(customer_id) позволяет использовать customer_id как конфликтный ключ для UPSERT.

    # customer_id  → определяем, существует ли клиент
    # customer_key  → внутренний ID записи DWH

--Именно поэтому мы сделали UNIQUE на customer_id.

# core_customers
#      │
#      │ city_id
#      ↓
# dim_customers
#      │
#      │ city_key
#      ↓
# dim_cities


CREATE table dim_categories (
category_key serial PRIMARY KEY,
category_id int,
category_name text,

CONSTRAINT cd_1 UNIQUE (category_id)

);

CREATE TABLE dim_products (
product_key SERIAL PRIMARY KEY,
product_id int,
product_name text,
category_key int,   -- вместо category_id --->>> category_key
price numeric,

CONSTRAINT pd_1 UNIQUE (product_id)
);






INSERT INTO dim_cities (
    city_id,
    city_name,
    country
)
SELECT
    city_id,
    city_name,
    country
FROM core_cities
ON CONFLICT (city_id)
DO UPDATE SET
    city_name = EXCLUDED.city_name,
    country = EXCLUDED.country;





insert into dim_categories (
    category_id,
    category_name

)

SELECT
  category_id,
    category_name

from core_categories

on conflict (category_id)
do update SET
category_name = EXCLUDEd.category_name;



----для видимости--------------------
CREATE table dim_customers (
customer_key serial PRIMARY KEY,
customer_id int,
customer_name text ,   
email text,
age int,
city_key int,     ---- тут 
---------------------------------



insert into dim_customers (
    customer_id,
    customer_name,
    email,
    age,
    city_key

)

SELECT
    customer_id,
    customer_name,
    email,
    age,
    city_key   ---в структуре у нас уже сурогат, поэтому join

from core_customers
INNER JOIN dim_cities
on dim_cities.city_id = core_customers.city_id

on conflict (customer_id)
do update SET
customer_name = EXCLUDEd.customer_name,
email = EXCLUDEd.email,
age = EXCLUDEd.age,
city_key = EXCLUDEd.city_key;




insert into dim_products  (
    product_id,
    product_name,
    category_key,
    price


)

SELECT
    product_id,
    product_name,
    category_key,
    price

from core_products
INNER JOIN dim_categories
on dim_categories.category_id = core_products.category_id

on conflict (product_id)
do update SET
product_name = EXCLUDEd.product_name,
category_key = EXCLUDEd.category_key,  ---тут key тоже вместо id
price = EXCLUDEd.price;





CREATE table fact_sales (

fact_sales_key serial PRIMARY KEY,
customer_key int,
product_key int,
price NUMERIC
);


INSERT INTO fact_sales (
    customer_key,
    product_key,
    price
)
SELECT
    dim_customers.customer_key,
    dim_products.product_key,
    dim_products.price
FROM dim_customers
CROSS JOIN dim_products;






~~~
