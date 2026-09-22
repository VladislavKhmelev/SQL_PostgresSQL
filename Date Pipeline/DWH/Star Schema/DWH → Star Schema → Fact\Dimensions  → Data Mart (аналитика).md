~~~
DWH → организован по Star Schema → Fact + Dimensions → Data Mart → Analytics.



Star Schema — это модель/структура организации DWH.




SOURCES + RAW + STAGING (VAlidated) + CORE/ERROR 

			  ↓
             DWH 
					
DIMENSIONS (обьект, сущность, «кто/что/какой/где/когда?) 
	dim_customer 
	    -surrogate key  создаем - customer_key serial primary key
	    - alter table (UNIQUE (customer_id) чтобы on conflict сработал при insert ....core_customers → dim_customer     │                

	dim_product                             
	dim_date                                               

FACT ( событие,  - что произошло/сколько/какой показатель)
    - определяем зерно Fact — что означает одна строка fact_sales (наше название)
        - мы решили одна строка — одна продажа/заказ с соответствующим платежом
                - или можно построить отдельный Fact,
                        одна строка = один платеж, 
                 Тогда уже payment_id становится центральным событием, и мы строим fact_payments.
        - Затем решаем, какие поля нужны для анализа
        - И уже после этого собираем fact_sale


	 fact_sales (собираем - grain Fact):
     grain — смысл одной строки Fact.

     например (описывает продажу - grain):
        sales_key serial PRIMARY KEY,
        customer_key int NOT NULL,
        product_key int NOT NULL,
        order_id int NOT NULL,
        payment_id int,
        quantity int,
        amount numeric,
        order_date date                                                     
        далее INSERT INTO  fact_sales 
            -смотрим по полям какие таблицы нужны для join



-Dimension — таблица, которая описывает объект/сущность.
-Fact — таблица событий с измеряемыми показателями.


дальше
        здесь просто добавляем два FOREIGN KEY для fact_sales

ALTER TABLE fact_sales
ADD CONSTRAINT fk_1
FOREIGN KEY (customer_key)
REFERENCES dim_customer(customer_key);




DATA MART
	dm_sales_report (аналитическая таблица, Именно здесь начинается практическая часть OLAP)
	SUM(amount)
	GROUP BY customer
	GROUP BY category
	и другие аналитические запросы





CREATE TABLE dim_customer (

    customer_key serial PRIMARY KEY,

    customer_id int NOT NULL,
    customer_name text,
    email text,
    age int,
    city text

);


CREATE table dim_product (
    product_key serial PRIMARY KEY, 
product_id int NOT NULL, 
product_name   text, 
 category text, 
 price numeric,
  stock int

);


# -----  чтобы сработал on conflict при INSERT INTO


alter table dim_customer
add CONSTRAINT ci UNIQUE (customer_id);

alter table dim_product
add CONSTRAINT pd UNIQUE (product_id);





INSERT INTO dim_product (
product_id , 
product_name   , 
 category , 
 price ,
  stock 
)

SELECT
product_id , 
product_name   , 
 category , 
 price ,
  stock 

FROM core_customers

ON CONFLICT (product_id)
DO UPDATE SET

    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    price = EXCLUDED.price,
    stock = EXCLUDED.stock;





INSERT INTO dim_product (
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

FROM core_products

ON CONFLICT (product_id)
DO UPDATE SET

    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    price = EXCLUDED.price,
    stock = EXCLUDED.stock;





# ----определили Grain Fact (строка - продажа )

CREATE TABLE fact_sales (
    sales_key serial PRIMARY KEY,
    customer_key int NOT NULL,
    product_key int NOT NULL,
    order_id int NOT NULL,
    payment_id int,
    quantity int,
    amount numeric,
    order_date date
);






INSERT INTO fact_sales (
    customer_key,
    product_key,
    order_id,
    payment_id,
    quantity,
    amount,
    order_date
)
SELECT
    dim_customer.customer_key,
    dim_product.product_key,
    core_orders.order_id,
    core_payments.payment_id,
    core_orders.quantity,
    core_payments.amount,
    core_orders.order_date
FROM core_orders

INNER JOIN dim_customer
    ON dim_customer.customer_id = core_orders.customer_id

INNER JOIN dim_product
    ON dim_product.product_id = core_orders.product_id

INNER JOIN core_payments
    ON core_payments.order_id = core_orders.order_id;




----дальше
----Да, здесь просто добавляем два FOREIGN KEY.


ALTER TABLE fact_sales
ADD CONSTRAINT fk_1
FOREIGN KEY (customer_key)
REFERENCES dim_customer(customer_key);


ALTER TABLE fact_sales
ADD CONSTRAINT fk_2
FOREIGN KEY (product_key)
REFERENCES dim_product(product_key);




Следующее задание — создать dim_date (это тоже Dimension), чтобы потом анализировать продажи по дате, месяцу и году.

# ┌──────────────────────────────────────────────┐
# │ ЗАДАНИЕ — DIMENSION ДАТЫ                     │
# ├──────────────────────────────────────────────┤
# │ 1. Создай таблицу dim_date.                  │
# │                                              │
# │ 2. Поля:                                    │
# │    date_key                                 │
# │    full_date                                │
# │    month                                    │
# │    year                                     │
# │                                              │
# │ 3. date_key должен быть PRIMARY KEY.         │
# │                                              │
# │ 4. Возьми даты из fact_sales    │
# │                                              │
# │ 5. Для каждой уникальной даты создай         │
# │    одну строку в dim_date.                  │
# │                                              │
# │ 6. Пока не создавай Data Mart.              │
# └──────────────────────────────────────────────┘


CREATE table dim_date (
date_key serial PRIMARY key,
full_date date,
month int,
year int
);


insert INTO dim_date (
full_date,
month ,
year 
)

SELECT
DISTINCT
order_date,

extract(MONTH from order_date)::int as month,
 extract(year from order_date)::int as year

from fact_sales;




dim_date — это Dimension, а не Data Mart.






Мы построили аналитическую структуру DWH:

DWH — аналитическое хранилище, в котором мы организовали данные в Fact и Dimensions для последующего анализа по моделе - Star Schema.



Star Schema — это один из вариантов того, как можно организовать DWH


Название буквально означает «звёздная схема», потому что в центре находится Fact, а вокруг него — Dimensions.

fact_sales — центральная таблица событий/показателей;
dim_customer — кто;
dim_product — что;
dim_date — когда.


#                    dim_customer
#                         │
#                         │
# dim_date ──────── fact_sales ──────── dim_product
#                         │
#                         │
#                    другие Dimension


Почему «звезда»:

fact_sales — центр;
dim_customer, dim_product, dim_date — лучи;
Fact связывается непосредственно с Dimensions.




Вот как собрать проверочный SELECT:

SELECT
    dim_customer.customer_name,
    dim_product.product_name,
    dim_product.category,
    dim_date.full_date,
    fact_sales.quantity,
    fact_sales.amount
FROM fact_sales

INNER JOIN dim_customer
    ON dim_customer.customer_key = fact_sales.customer_key

INNER JOIN dim_product
    ON dim_product.product_key = fact_sales.product_key

INNER JOIN dim_date
    ON dim_date.full_date = fact_sales.order_date;



 customer_name  | product_name |  category   | full_date  | quantity | amount
----------------+--------------+-------------+------------+----------+--------
 Ivan Petrov    | Keyboard     | Electronics | 2026-09-01 |        2 |    200
 Ivan Petrov    | Mouse        | Electronics | 2026-09-02 |        1 |     50
 Alex Smirnov   | Office Chair | Furniture   | 2026-09-03 |        2 |    300


 #                 fact_sales
#                     │
#          ┌──────────┼──────────┐
#          ↓          ↓          ↓
#   dim_customer  dim_product  dim_date
#       │             │           │
#     кто?           что?        когда?
#       │             │           │
# customer_name   product_name  full_date
#                 category
#
#             + quantity
#             + amount



То есть мы сейчас не создаём новую таблицу, а проверяем, что наша Star Schema действительно позволяет получить нормальную аналитическую строку




А дальше уже:

DWH → Data Mart → Analytics

Следующее — создать первую Data Mart на основе нашей Star Schema.


# ┌──────────────────────────────────────────────┐
# │ ЗАДАНИЕ — DATA MART ПРОДАЖ                   │
# ├──────────────────────────────────────────────┤
# │ 1. Создай таблицу dm_sales_report.           │
# │                                              │
# │ 2. Собери её из:                             │
# │    fact_sales                                │
# │    dim_customer                              │
# │    dim_product                               │
# │    dim_date                                  │
# │                                              │
# │ 3. В Data Mart должны быть поля:             │
# │    customer_name                             │
# │    product_name                              │
# │    category                                  │
# │    full_date                                 │
# │    month                                     │
# │    year                                      │
# │    quantity                                  │
# │    amount                                    │
# │                                              │
# │ 4. Пока НЕ делай GROUP BY и агрегирование.   │
# │                                              │
# │ 5. Одна строка Data Mart должна              │
# │    соответствовать одной строке              │
# │    fact_sales.                               │
# └──────────────────────────────────────────────┘



CREATE table dm_sales_report as 

SELECT
customer_name,
product_name,
category,
full_date,
month,
year,
quantity,
amount


from fact_sales
INNER JOIN dim_product
on dim_product.product_key = fact_sales.product_key
INNER JOIN dim_date
on dim_date.full_date = fact_sales.order_date
INNER JOIN dim_customer
on dim_customer.customer_key = fact_sales.customer_key;



SELECT
customer_name,

SUM(amount) as r

from dm_sales_report

GROUP BY customer_name

ORDER BY r DESC;






~~~
