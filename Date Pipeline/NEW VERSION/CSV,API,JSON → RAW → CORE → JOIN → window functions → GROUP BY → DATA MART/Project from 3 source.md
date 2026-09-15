~~~
import pandas as pd
import os
from dotenv import load_dotenv
from psycopg2 import connect
from sqlalchemy import create_engine
from sqlalchemy import text
import requests


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

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)

         #JSON
#----------------------------
df = pd.read_json(r"C:\Users\Vladislav X\Desktop\proj\ver10\data\products.json")


# print(df.dtypes)
print(df.head(3))

# хорошо подходит, когда JSON уже имеет плоскую табличную структуру:
                    # json плоский
# ┌────┬────────────┬──────────┬──────────────┐
# │ id │ name       │ price    │ stock        │
# ├────┼────────────┼──────────┼──────────────┤
# │  1 │ Laptop     │ 1200     │ 10           │
# │  2 │ Keyboard   │  100      │ 25           │
# └────┴────────────┴──────────┴──────────────┘

# если json вложенный, тоже самое как и с API делаем, понимаем какие поля нам нужны, опускаемся по уровням, и чеез цикл пайтон, панадас собираем таблицу плоскую.



# Вариант 1 — JSON → DataFrame → to_sql()   (быстрый способ)
# Вариант 2 — JSON → DataFrame → CSV в buffer → COPY(psycopg2)


#делаем вариант 1:


df.to_sql(
    name="raw_products_json",
    con=engine,
    if_exists="replace",
    index=False
)


print("загружено", len(df), "\n")
print(df.dtypes, " 'ЭТО ПЛОСКИЙ JSON ", "\n")

          #API
#----------------------------------------

url = "https://dummyjson.com/carts?limit=20&utm_source=chatgpt.com"

response = requests.get(url)

print(response.status_code)

data = response.json()

df = pd.DataFrame(data)   #открываем наш json через Дата Фрейм

# print(df_api.head(3))




# нам нужны такие поля
#----------------
# id
# userId
# productId
# quantity
#------------------

df = pd.DataFrame(data)
# df.to_csv(r"C:\Users\Vladislav X\Desktop\proj\ver10\data\API_all_table_.csv", index=False)

print("---Поля json верхний уровень:", df.columns.tolist(),"\n")
#сохраняем каждый уровень чтобы посмотреть наглядно какое поле у нас имеет массив и потом в него заходим


# вложенный массив на верхнем уровне
carts = data["carts"]
df = pd.DataFrame(carts)
# print(carts[:1],"\n")


# df.to_csv(r"C:\Users\Vladislav X\Desktop\proj\ver10\data\API_table_part_1.csv", index=False)
#сохраняем каждый уровень чтобы посмотреть наглядно какое поле у нас имеет массив и потом в него заходим

print("---Поля первый уровень:", df.columns.tolist())


# тут мы погружаемся внутрь, поэтому как со словарями прописываем, [0] - нужно, чтобы взять одну корзину, иначе цикл нужен
products = data["carts"][0]["products"]
df = pd.DataFrame(products)
df.to_csv(r"C:\Users\Vladislav X\Desktop\proj\ver10\data\API_table_part_2.csv", index=False)

print("---Поля второй уровень:", df.columns.tolist())

# нам нужны такие поля
#----------------
# id
# userId
# productId
# quantity
#------------------

# НАХОДИМ ВСЕ ПОЛЯ КОТОРЫЕ НАМ НУЖНЫ


# ---Поля json верхний уровень: ['carts', 'total', 'skip', 'limit']
#
# ---Поля первый уровень: ['id', 'products', 'total', 'discountedTotal', 'userId', 'totalProducts', 'totalQuantity']
# ---Поля второй уровень: ['id', 'title', 'price', 'quantity', 'total', 'discountPercentage', 'discountedTotal',



# нам нужны такие поля
#----------------
# id
# userId
# productId
# quantity
#------------------


# через  обычный Pandas/Python  делаем, удобнее

rows = []

for t in data["carts"]:
    for t1 in t["products"]:
        rows.append({
            "id": t["id"],
            "user_id": t["userId"],
            "product_id": t1["id"],
            "quantity": t1["quantity"]
        }) #  слева колонки называем как хотим

df = pd.DataFrame(rows)

print(df.head(3))  # готова
print(df.dtypes)

df.to_sql(
    name="api_json_table",
    con=engine,
    if_exists="replace",
    index=False
)


print("загружено API json строк: ", len(df), "\n")





CREATE table raw_customers (

customer_id text,
customer_name text,
email text,
city text

);


CREATE TABLE raw_products_json (

product_id  int,   
product_name    text ,
category   text,
price numeric

);

\copy raw_customers FROM 'C:\Users\Vladislav X\Desktop\proj\ver10\data\customers.csv' WITH (FORMAT CSV, HEADER);


CREATE Table api_json_table (

id int, 
user_id int, 
product_id  int,
quantity int

);


# делаем ВАЛИДАЦИЮ
#-------------------------------------

#-----------

trim() 
INITCAP() 
lower() 
NULLIF(  ,'') 

NULLIF(  ,'')  trim() 
#---------

 ~ '^\d+$'    (для int)
 ~ '^\d+(\.\d+)?$'    (для numeric)

CREATE table validated_customers as 

SELECT
 case when NULLIF( trim(customer_id) ,'')  ~ '^\d+$'
 and    NULLIF( trim(customer_id) ,'')::int > 0
 then NULLIF( trim(customer_id) ,'')::int
 else null end as customer_id,

NULLIF(  trim(customer_name) ,'')   as customer_name,
NULLIF( trim(email)  ,'')   as email,
NULLIF(  trim(city) ,'')   as city,


 NULLIF(
    CONCAT_WS(
        '; ',
        CASE
            WHEN  NULLIF( trim(customer_id) ,'') is null
                THEN 'customer_id is NULL'

            WHEN NULLIF(TRIM(customer_id), '')  !~ '^\d+$' 
                    THEN 'customer_id error format'

            WHEN  NULLIF(TRIM(customer_id),'')::int <= 0
                THEN 'customer_id <= 0'
            
            WHEN  count(*) over(PARTITION BY customer_id) > 1
                THEN 'customer_id is dub'
        END,

        CASE
            WHEN  NULLIF(  trim(customer_name) ,'') IS NULL
                THEN 'customer_name is NULL'
        END,

        CASE
            WHEN   NULLIF( trim(email)  ,'') IS NULL
                THEN 'email is NULL'
            
            WHEN   NULLIF( trim(email)  ,'') not like '%@%'
                THEN 'email not @'
        END,

        CASE
            WHEN  NULLIF(  trim(city) ,'')  IS NULL
                THEN 'city is NULL'

                end
        
    ),
    ''
) AS error_reason

FROM raw_customers;








#-----------

trim() 
INITCAP() 
lower() 
NULLIF(  ,'') 

NULLIF(  ,'')  trim() 
#---------

 ~ '^\d+$'    (для int)
 ~ '^\d+(\.\d+)?$'    (для numeric)

-- # json лучше прописывать NULLIF(  ,'')  trim() иначе пустые строки есть


CREATE TABLE validated_products_json as

SELECT


product_id,
 product_name     ,
 category   ,
 price,



 NULLIF(
    CONCAT_WS(
        '; ',
        CASE
            WHEN  product_id <= 0
                THEN 'product_id <= 0'

                 WHEN  count(*) over(PARTITION BY product_id) >1
                THEN 'product_id is dub'
            end,

            case 
                 WHEN NULLIF( trim(product_name)  ,'')   is null
                    THEN 'product_name is null'
            end,

            case 
                WHEN NULLIF(  trim(category) ,'')    is null
                    THEN 'category is null'
            end,
            
            case
                WHEN  price <= 0
                    THEN 'price <= 0'
            END

      
        
    ),
    ''
) AS error_reason

FROM raw_products_json;







trim() 
INITCAP() 
lower() 
NULLIF(  ,'') 

NULLIF(  ,'')  trim() 
#---------

 ~ '^\d+$'    (для int)
 ~ '^\d+(\.\d+)?$'    (для numeric)




alter table raw_api_json_table
RENAME to raw_api_json_orders;


alter table raw_api_json_table
RENAME COLUMN id to order_id;   

alter table raw_api_json_table
RENAME COLUMN user_id to customer_id;




CREATE Table validated_api_json_orders as 

select
order_id  ,
customer_id  ,
product_id  ,
quantity,

NULLIF(
    CONCAT_WS(
        '; ',
        CASE
            WHEN  order_id is null
                THEN 'order_id is null'

            WHEN  order_id <= 0
                THEN 'order_id <= 0'


                end,

  CASE
            WHEN  customer_id is null
                THEN 'customer_id is null'

            WHEN  customer_id <= 0
                THEN 'customer_id <= 0'

                end,

 CASE
            WHEN  product_id is null
                THEN 'product_id is null'

            WHEN  product_id <= 0
                THEN 'product_id <= 0'

                end,

                 CASE
            WHEN  quantity is null
                THEN 'quantity is null'

            WHEN  quantity <= 0
                THEN 'quantity <= 0'

                end
        
    ),
    ''
) AS error_reason

FROM raw_api_json_orders;




CREATE table error_customers (
error_id SERIAL PRIMARY key,
customer_id int,
customer_name text,
email text,
city text,
error_reason text
);




CREATE table error_products_json (
error_id SERIAL PRIMARY key,
product_id int,    
product_name text,    
category text,   
price numeric,
error_reason text
);


CREATE table error_api_json_orders (
error_id SERIAL PRIMARY key,
order_id int, 
customer_id int, 
product_id int, 
quantity int,
error_reason text
);






CREATE table core_customers (
customer_id int PRIMARY KEY,
customer_name text,
email text,
city text
);




CREATE table core_products_json (
product_id int PRIMARY key,    
product_name text,    
category text,   
price numeric
);



Поэтому order_id нельзя было делать единственным PRIMARY KEY.

Правильный подход перед созданием таблицы:

# ┌───────────────────────────────────────┐
# │ 1. Изучаем источник                   │
# │ 2. Смотрим структуру всех таблиц      │
# │ 3. Определяем связи между таблицами   │
# │ 4. Определяем уникальность строк      │
# │ 5. Только потом назначаем PRIMARY KEY │
# │ 6. Назначаем FOREIGN KEY               │
# └───────────────────────────────────────┘

CREATE table core_api_json_orders (
order_id int,    
customer_id int, 
product_id int, 
quantity int,

PRIMARY KEY (order_id, product_id)  

);

-- составной ключ, при дедупликации мы далее там выбираем два поля, и через конфликт в ставке в таблицу будут по двум ключам проверка, поэтому делаем составной ключ



INSERT INTO error_customers (



customer_id, 
customer_name, 
email, 
city,
error_reason 
)
SELECT


customer_id, 
customer_name, 
email, 
city,
error_reason 


FROM validated_customers


WHERE error_reason IS NOT NULL;







INSERT INTO error_products_json (

product_id,    
product_name,    
category,  
price, 
error_reason 
)
SELECT


product_id,    
product_name,    
category,  
price, 
error_reason 


FROM validated_products_json


WHERE error_reason IS NOT NULL;








INSERT INTO error_api_json_orders (

order_id, 
customer_id,  
product_id, 
quantity, 
error_reason 
)
SELECT


order_id, 
customer_id,  
product_id, 
quantity, 
error_reason 


FROM validated_api_json_orders


WHERE error_reason IS NOT NULL;





INSERT INTO core_customers (

customer_id, 
customer_name, 
email, 
city

)
SELECT
customer_id, 
customer_name, 
email, 
city

FROM (
    select
    *,
    ROW_NUMBER() over(PARTITION BY customer_id ) as r


    from validated_customers

) tt 

where r = 1 and error_reason is null



on conflict(customer_id)

DO UPDATE SET

customer_name = EXCLUDED.customer_name,
email = EXCLUDED.email,
city = EXCLUDED.city;







INSERT INTO core_products_json (

product_id,    
product_name,     
category,  
price 

)
SELECT
product_id,    
product_name,     
category,  
price 

FROM (
    select
    *,
    ROW_NUMBER() over(PARTITION BY product_id ) as r


    from validated_products_json

) tt 

where r = 1 and error_reason is null



on conflict(product_id)

DO UPDATE SET

product_name = EXCLUDED.product_name,
category = EXCLUDED.category,
price = EXCLUDED.price;




delete from validated_api_json_orders
 where error_reason = 'order_id is dub';



INSERT INTO core_api_json_orders (

order_id,  
customer_id, 
product_id, 
quantity 

)
SELECT
order_id,  
customer_id, 
product_id, 
quantity 
FROM (
    select
    *,
    ROW_NUMBER() over(PARTITION BY order_id, product_id) as r


    from validated_api_json_orders

) tt 

where r = 1 and error_reason is null

on CONFLICT(order_id, product_id)

do UPDATE set
customer_id = EXCLUDEd.customer_id,
quantity = EXCLUDEd.quantity;


--По структуре таблицы мы можем предположить, что понадобится составной ключ, но окончательно определяем его по уникальности комбинации и бизнес-смыслу данных.

ТАБЛИЦА:  core_api_json_orders
 order_id | customer_id | product_id | quantity
----------+-------------+------------+----------
        1 |           1 |        162 |        4

-- тут вероятнее всего одного order_id для on conflict , будет недостаточно, и по логике подбираем уникальность строки  order_id + product_id (без customer_id, потому что уже сам заказ по определению говорит что это чей-то одного клиента, неважно какого)




ТАБЛИЦА: core_products_json
 product_id | product_name |  category   | price
------------+--------------+-------------+-------
          1 | Product 1    | Electronics |  23.5

--тут можно предположить что продукт, будет уникальным и PK будет только по product_id









#поиск сирот


SELECT
DISTINCT  -- иначе повторяются строки
core_api_json_orders.order_id as order_id


from core_api_json_orders
LEFT JOIN core_customers
on core_customers.customer_id = core_api_json_orders.customer_id
LEFT JOIN core_products_json
on core_products_json.product_id = core_api_json_orders.product_id

where core_customers.customer_id is null or core_products_json.product_id is null;

--- !!!!!!смотреть надо на таблицу которая ссылается на несколько таблиц по ключам, тогда она скорее всего дочерняя, у нее будут внешние клюи по отношению к 2м другим таблица, которые имеют только primary key, поэтому и проверяем сразу 2 таблицы is null в where 


 order_id
----------
        1
        2
        3
        5
        6
        7
        8
        9
       10
       11
       12
       15
       17
       18
       20
(15 строк)


begin;

DELETE from core_api_json_orders
where order_id in (
          1,
        2,
        3,
        5,
        6,
        7,
        8,
        9,
       10,
       11,
       12,
       15,
       17,
       18,
       20
       );

COMMIT;





INSERT INTO error_api_json_orders (

order_id, 
customer_id,  
product_id, 
quantity, 
error_reason 
)
SELECT


order_id, 
customer_id,  
product_id, 
quantity, 
error_reason 


FROM validated_api_json_orders


WHERE order_id in (
        1,
        2,
        3,
        5,
        6,
        7,
        8,
        9,
       10,
       11,
       12,
       15,
       17,
       18,
       20

);



update error_api_json_orders
set error_reason = 'error_id is orphan';




-- после удаления сирот добавляем внешние ключи к таблицам

ALTER Table core_api_json_orders
ADD CONSTRAINT fk_cd FOREIGN KEY (customer_id) REFERENCES core_customers(customer_id);


ALTER Table core_api_json_orders
ADD CONSTRAINT fk_pd FOREIGN KEY (product_id) REFERENCES core_products_json(product_id);


--после удаления сирот обе ссылочные связи успешно --установлены.

--На этом этапе структура CORE у нас согласована с данными.






--Дальше по нашей последовательности — JOIN трёх CORE-таблиц.






--- Соедини через JOIN три CORE-таблицы: 

CREATE table orders_mart as

SELECT

core_products_json.category as category,

count(core_products_json.product_id) as количество_товарных_позиций,

COALESCE(sum(quantity),0) as общее_количество_проданных_единиц  ,

COALESCE(sum(price*quantity),0) as общую_сумму_продаж





from core_api_json_orders
LEFT JOIN core_products_json                               
on core_products_json.product_id = core_api_json_orders.product_id
LEFT JOIN core_customers
on core_customers.customer_id = core_api_json_orders.customer_id



GROUP BY core_products_json.category ;








~~~
