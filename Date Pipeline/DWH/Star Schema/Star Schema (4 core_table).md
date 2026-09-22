~~~
CREATE table dim_customers (
customer_key serial PRIMARY KEY,
customer_id int NOT NULL,
customer_name text,
email text,
age int,
city text,

CONSTRAINT cd UNIQUE (customer_id)
);




CREATE table dim_products (
product_key serial PRIMARY KEY,
product_id int NOT NULL,
product_name text,
category text,
price numeric,
stock int,

CONSTRAINT pd UNIQUE (product_id)
);


insert into dim_products (
product_id ,
product_name ,
category,
price ,
stock 
)
SELECT
product_id ,
product_name ,
category,
price ,
stock 


from core_products

on conflict (product_id)
do update SET
product_name = EXCLUDEd.product_name,
category = EXCLUDEd.category,
price = EXCLUDEd.price,
stock = EXCLUDEd.stock;




insert into dim_customers (
customer_id ,
customer_name ,
email,
age ,
city 
)
SELECT
customer_id ,
customer_name ,
email,
age ,
city 


from core_customers

on conflict (customer_id)
do update SET
customer_name = EXCLUDEd.customer_name,
email = EXCLUDEd.email,
age = EXCLUDEd.age,
city = EXCLUDEd.city;



CREATE Table fact_sales (

fact_sales_key SERIAL PRIMARY KEY,
order_id int,           --core_orders
customer_key int,       -- dim_customers
product_key int,          -- dim_products
quantity int,         -- core_orders
amount numeric,         -- core_payments
order_date date       -- core_orders
);


insert into fact_sales (
    order_id ,
    customer_key ,
    product_key ,
    quantity ,
    amount ,
    order_date
)

SELECT
    core_orders.order_id,
    dim_customers.customer_key,
    dim_products.product_key,
    core_orders.quantity,
    core_payments.amount,
    core_orders.order_date

from core_orders
INNER JOIN dim_customers
on dim_customers.customer_id = core_orders.customer_id
INNER JOIN dim_products
on dim_products.product_id = core_orders.product_id
INNER JOIN core_payments
on core_payments.order_id = core_orders.order_id;




# добавляем внешний ключ

alter table fact_sales
add CONSTRAINT fk_cy FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key);

alter table fact_sales
add CONSTRAINT fk_py FOREIGN KEY (product_key) REFERENCES dim_products(product_key);



CREATE table dim_date (
date_key SERIAL PRIMARY KEY,
full_date date,
month int,
year int
);





insert into dim_date (
    full_date,  
    month ,
    year
)
SELECT
DISTINCT

order_date,
extract(month from order_date)::int as month,
extract(year from order_date)::int as year

from fact_sales;








-- тут postgressql  потребует уникальности для full_date, чтобы он смог ссылаться на единственный unique


ALTER table dim_date
add CONSTRAINT un UNIQUE (full_date);



alter table fact_sales
add CONSTRAINT fk_oe FOREIGN KEY (order_date) REFERENCES dim_date(full_date);




--data mart 

CREATE table dm_sales_report as 

SELECT
customer_name,                             
product_name,                              
category  ,  
full_date ,                                                   month  ,                                          
year    ,                                  
quantity  ,                                
amount       

from fact_sales
INNER JOIN dim_customers
on dim_customers.customer_key = fact_sales.customer_key
INNER JOIN dim_date
on dim_date.full_date = fact_sales.order_date
INNER JOIN dim_products
on dim_products.product_key = fact_sales.product_key;
~~~
