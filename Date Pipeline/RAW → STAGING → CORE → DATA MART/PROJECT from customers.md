~~~

RAW → STAGING → CORE → DATA MART

customers.csv

id,name,email,age,status,created_at
1,Alex,alex@mail.com,25,active,2026-09-01
2,Anna,anna@mail.com,31,active,2026-09-01
3,John,john@mail.com,17,active,2026-09-02
4,Mike,mike@mail.com,42,blocked,2026-09-02
5,Lisa,lisa@mail.com,28,active,2026-09-03
6,Peter,peter@mail.com,-5,active,2026-09-03
7,Sarah,sarah@mail.com,29,pending,2026-09-03
8,Tom,tom@mail.com,35,active,2026-09-04
9,Emma,emma@mail.com,22,deleted,2026-09-04
10,David,david@mail.com,45,unknown,2026-09-05

-----------------------------------------------------------------

import os
import psycopg2
from dotenv import load_dotenv
import logging
import pandas as pd
from io import StringIO

load_dotenv(r"C:\Users\Vladislav X\Desktop\proj\ver2\1.env")


# ┌──────────────────────────────┐
# │ Logging                      │
# └──────────────────────────────┘

logging.basicConfig(
    filename=r"C:\Users\Vladislav X\Desktop\proj\ver2\logs\etl_customers.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ┌──────────────────────────────┐
# │ Данные подключения к БД      │
# └──────────────────────────────┘

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")


# ┌──────────────────────────────┐
# │ Путь к CSV                   │
# └──────────────────────────────┘

# df = r"C:\Users\Vladislav X\Desktop\proj\ver2\data\customers.csv"
#
# print(df)



df = pd.read_csv(
    r"C:\Users\Vladislav X\Desktop\proj\ver2\data\customers.csv",
    parse_dates=["created_at"]
)


print(df)
print(df.dtypes)
# print(df.columns)

df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
df.columns = df.columns.str.strip()

df["email"] = df["email"].str.lower()
df["status"] = df["status"].str.lower()
df["name"] = df["name"].str.title()

print(df.isna().sum())
# print(df.notnull().all(axis=1))
print(df.duplicated().sum())




# ┌──────────────────────────────┐
# │ Подключение к PostgreSQL     │
# └──────────────────────────────┘

connection = psycopg2.connect(
    user=user,
    password=password,
    host=host,
    port=port,
    dbname=database
)


logging.info("ETL START")


try:
# подключаем к Postgres, курсор позваляляет передавать запросы через пайтон
    with connection.cursor() as cursor:

        # ┌──────────────────────────────┐
        # │ 1. Очищаем staging           │
        # └──────────────────────────────┘

        cursor.execute("""
            TRUNCATE TABLE staging_customers;
        """)

        cursor.execute("""
            TRUNCATE TABLE error_customers;
        """)


        # ┌──────────────────────────────┐
        # │ 2. CSV → staging через COPY  │
        # └──────────────────────────────┘

        logging.info("Начинаем COPY CSV → staging")

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)






        cursor.copy_expert(
            """
            COPY staging_customers (
                id,
                name,
                email,
                age,
                status,
                created_at
            )
            FROM STDIN
            WITH CSV HEADER
            """,
            buffer
        )

        logging.info("COPY завершён")


        # ┌──────────────────────────────┐
        # │ 3. Создаём validation layer  │
        # └──────────────────────────────┘

        cursor.execute("""
            CREATE TEMP TABLE validated_customers AS
            SELECT
                *,
                NULLIF(
                    CONCAT_WS(
                        '; ',
                        CASE
                            WHEN id is null
                            THEN 'id is null'
                        END,
                        
                        CASE
                            WHEN age <= 0
                            THEN 'invalid age'
                        END,

                        CASE
                            WHEN name IS NULL
                            THEN 'name is null'
                        END,

                        CASE
                            WHEN email IS NULL
                            THEN 'email is null'
                        END,

                        CASE
                            WHEN created_at > CURRENT_TIMESTAMP
                            THEN 'future date'
                        END,

                        CASE
                            WHEN status NOT IN (
                                'active',
                                'blocked',
                                'pending',
                                'deleted'
                            )
                            THEN 'invalid status'
                        END
                    ),
                    ''
                ) AS error_reason

            FROM staging_customers;
        """)


        # ┌──────────────────────────────┐
        # │ Считаем ошибочные строки     │
        # └──────────────────────────────┘

        cursor.execute("""
            SELECT COUNT(*)
            FROM validated_customers
            WHERE error_reason IS NOT NULL;
        """)
# fetchone() → забирает одну строку результата: (2,) [0] → берёт первый элемент: 2 И:error_count = 2  То есть error_count = количество невалидных строк.
        error_count = cursor.fetchone()[0]

        logging.info(
            "Ошибочных строк: %s",
            error_count
        )


        # ┌──────────────────────────────┐
        # │ Считаем корректные строки    │
        # └──────────────────────────────┘

        cursor.execute("""
            SELECT COUNT(*)
            FROM validated_customers
            WHERE error_reason IS NULL;
        """)

        clean_count = cursor.fetchone()[0]

        logging.info(
            "Корректных строк: %s",
            clean_count
        )


        # ┌──────────────────────────────┐
        # │ 4. Ошибки → error_customers  │
        # └──────────────────────────────┘

        cursor.execute("""
            INSERT INTO error_customers (
                id,
                name,
                email,
                age,
                status,
                created_at,
                error_reason
            )
            SELECT
                id,
                name,
                email,
                age,
                status,
                created_at,
                error_reason
            FROM validated_customers
            WHERE error_reason IS NOT NULL;
        """)


        # ┌──────────────────────────────┐
        # │ 5. Clean → customers         │
        # │    через UPSERT               │
        # └──────────────────────────────┘

        cursor.execute("""
            INSERT INTO customers (
                id,
                name,
                email,
                age,
                status,
                created_at
            )
            SELECT
                id,
                name,
                email,
                age,
                status,
                created_at
            FROM validated_customers
            WHERE error_reason IS NULL

            ON CONFLICT (id)
            DO UPDATE SET
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                age = EXCLUDED.age,
                status = EXCLUDED.status,
                created_at = EXCLUDED.created_at;
        """)

        logging.info("UPSERT завершён")


        # ┌──────────────────────────────┐
        # │ 6. Post-load check           │
        # └──────────────────────────────┘
# JOIN сопоставляет строки по id. передается запрос только
        cursor.execute("""
            SELECT COUNT(*)
            FROM validated_customers v
            JOIN customers c
                ON c.id = v.id
            WHERE v.error_reason IS NULL;
        """)
# забирает одну строку результата.
        loaded_clean_count = cursor.fetchone()[0]

        logging.info(
            "Проверено загруженных клиентов: %s",
            loaded_clean_count
        )


        if loaded_clean_count != clean_count:

            raise Exception(
                f"Post-load check failed: "
                f"ожидалось {clean_count}, "
                f"загружено {loaded_clean_count}"
            )


        logging.info(
            "Post-load check успешно пройден"
        )


    # ┌──────────────────────────────┐
    # │ 7. Всё успешно → COMMIT      │
    # └──────────────────────────────┘

    connection.commit()

    logging.info("ETL SUCCESS")

    print("ETL успешно завершён")


except Exception as e:

    # ┌──────────────────────────────┐
    # │ Ошибка → ROLLBACK             │
    # └──────────────────────────────┘

    connection.rollback()

    print("Ошибка ETL:", e)

    logging.error(
        "ETL ERROR: %s",
        e
    )
#«После rollback не скрывай ошибку — передай её дальше».
    raise


finally:
#«Как бы ни закончилась программа, закрой соединение с БД».
    connection.close()




далее SQL


ЧТО БУДЕМ СОЗДАВАТЬ!!!!


ШАГ 1
-----------------------------------------------------------------
staging_customers

CREATE table staging_customers (
id int,   
name text,           
email text, 
age int,   
status text, 
created_at TIMESTAMP
);


error_customers


CREATE table error_customers (
id int,   
name text,           
email text, 
age int,   
status text, 
created_at TIMESTAMP
);

alter table error_customers
add COLUMN error_reason text;



customers


CREATE table customers (
id int PRIMARY KEY NOT NULL,   
name text NOT NULL,           
email text NOT NULL, 
age int NOT NULL,   
status text NOT NULL, 
created_at TIMESTAMP NOT NULL

);


#ПРОВЕРКА
------------------------------------

SELECT 'customers' AS table_name, COUNT(*) AS row_count
FROM customers

UNION ALL

SELECT 'error_customers', COUNT(*)
FROM error_customers

UNION ALL

SELECT 'staging_customers', COUNT(*)
FROM staging_customers;




ШАГ 2
-----------------------------------------------------------------

для orders
id  customer_id  product_id  amount     status created_at


staging_orders

CREATE table staging_orders (
id  int, 
customer_id int,  
product_id int,  
amount numeric(10,2),     
status text, 
created_at TIMESTAMP
);

# ┌────────────────────────────────┐
# │ STAGING                        │
# │                                │
# │ Принимаем данные               │
# │ Минимум ограничений            │
# │ TRUNCATE / COPY                │
# │ Временное хранение             │
# └────────────────────────────────┘



error_orders


CREATE table error_orders (
id int, 
customer_id int, 
product_id int,  
amount numeric(10,2),     
status text, 
created_at TIMESTAMP,
error_reason text
);

# ┌────────────────────────────────┐
# │ ERROR                          │
# │                                │
# │ Храним ошибочные записи        │
# │ error_reason                   │
# │ Анализируем ошибки             │
# │ Не пускаем плохие данные       │
# │ в основной слой                │
# └────────────────────────────────┘


# для пояснения 
TEMP TABLE validated_orders / validated_customers

# ┌────────────────────────────────┐
# │ VALIDATION                     │
# │                                │
# │ Проверяем данные               │
# │ NULL                            │
# │ Дубликаты                      │
# │ Форматы                        │
# │ Бизнес-правила                 │
# │ error_reason                   │
# └────────────────────────────────┘

и 

В нашей схеме RAW — это твой исходный CSV-файл.

это просто файлы:

customers.csv
orders.csv

# ┌────────────────────────────────┐
# │ RAW                            │
# │                                │
# │ customers.csv                  │
# │ orders.csv                     │
# │                                │
# │ Исходные данные                │
# │ Ничего не изменяем             │
# └────────────────────────────────┘
#                 ↓
#             STAGING







orders


CREATE table orders (
id int PRIMARY KEY, 
customer_id int NOT NULL,  
product_id int NOT NULL, 
amount NUMERIC(10,2) NOT NULL,     
status text NOT NULL, 
created_at TIMESTAMP NOT NULL

);

#ПРОВЕРКА
------------------------------------

SELECT 'orders' AS table_name, COUNT(*) AS row_count
FROM orders

UNION ALL

SELECT 'error_orders', COUNT(*)
FROM error_orders

UNION ALL

SELECT 'staging_orders', COUNT(*)
FROM staging_orders;



SELECT *
FROM orders;

-- error_orders
SELECT *
FROM error_orders;

-- staging_orders
SELECT *
FROM staging_orders;



# ┌──────────────────────┐
# │ CSV                  │
# └──────────┬───────────┘
#            ↓
# ┌──────────────────────┐
# │ STAGING              │
# │ staging_customers    │
# │ staging_orders       │
# └──────────┬───────────┘
#            ↓
# ┌──────────────────────┐
# │ VALIDATION            │
# │ error_customers       │
# │ error_orders          │
# └──────────┬───────────┘
#            ↓
# ┌──────────────────────┐
# │ CORE                 │
# │ core_customers       │
# │ core_orders          │
# └──────────────────────┘



ШАГ 3
-----------------------------------------------------------------
CORE — это уже основной, нормализованный слой данных, которому можно доверять.

# ┌────────────────────────────────┐
# │ CORE                           │
# │                                │
# │ PRIMARY KEY                    │
# │ NOT NULL                      │
# │ UNIQUE                        │
# │ FOREIGN KEY                   │
# │ CHECK                         │
# │ и другие ограничения           │
# └────────────────────────────────┘


Создай две основные таблицы:
core_customers
core_orders


сначала нужно загрузить core_customers, а уже потом core_orders.

Почему? Потому что core_orders.customer_id ссылается на:

REFERENCES core_customers(id)

Поэтому при вставке каждого заказа PostgreSQL сразу проверяет, существует ли такой клиент в core_customers.

--------ТУТ СТРОГИЕ ПРАВИЛА НАЧИНАЮТСЯ!!!!!------


CREATE table core_customers (
id int PRIMARY KEY NOT NULL,   
name text NOT NULL,           
email text NOT NULL UNIQUE, 
age int NOT NULL CHECK(age >0),   
status text NOT NULL, 
created_at TIMESTAMP NOT NULL default current_timestamp

);


CREATE table core_orders (
id int PRIMARY KEY, 
customer_id int NOT NULL,
CONSTRAINT fk FOREIGN key (customer_id) REFERENCES core_customers(id),
product_id int NOT NULL, 
amount NUMERIC(10,2) NOT NULL CHECK(amount >0),     
status text NOT NULL, 
created_at TIMESTAMP NOT NULL default current_timestamp

);





вставляем данные в таблицы:

insert into core_customers (
id,
name,
email,
age,
status,
created_at
)
select
id,
name,
email,
age,
status,
created_at

from customers;
-------------------------------------------------

далее смотрим ЗАКАЗЫ - СИРОТЫ без клиентов, иначе не загрузятся, в core_orders, так как FOREIGN key не даст этого, он сразу будет проверять совпадения наличия клиента в другой таблице.


select
orders.id as id

from orders
LEFT JOIN customers
on customers.id = orders.customer_id

where customers.id is null;

вывод:

 id
----
 10
 11
(2 строки)

либо можно через inner JOIN (сразу исключает сирот)


insert into core_orders (
id,
customer_id,
product_id,
amount,
status,
created_at
)
select
ee.id,
ee.customer_id,
ee.product_id,
ee.amount,
ee.status,
ee.created_at

from orders ee
inner join customers e
on e.id = ee.customer_id;


# Сироты остаются в orders
# и НЕ попадают в core_orders

и можно добавить в 


Да, идея правильная: ты хочешь через INNER JOIN оставить только те заказы, у которых существует клиент.

Но у тебя не совпадают алиасы.

Ты дал:

FROM orders ee
INNER JOIN customers e
Значит обращаться к таблицам нужно через ee и e, а не через orders и customers.

Правильно:

INSERT INTO core_orders (
    id,
    customer_id,
    product_id,
    amount,
    status,
    created_at
)
SELECT
    ee.id,
    ee.customer_id,
    ee.product_id,
    ee.amount,
    ee.status,
    ee.created_at
FROM orders ee
INNER JOIN customers e
    ON e.id = ee.customer_id;
Что здесь происходит
# ┌──────────────────────┐
# │ orders AS ee         │
# │                      │
# │ customer_id = 10    │
# └──────────┬───────────┘
#            │
#            │ INNER JOIN
#            ↓
# ┌──────────────────────┐
# │ customers AS e       │
# │                      │
# │ id = 10              │
# └──────────┬───────────┘
#            ↓
#      клиент существует ✓
#            ↓
# ┌──────────────────────┐
# │ core_orders          │
# └──────────────────────┘
Если в customers нет клиента с таким id, INNER JOIN не даст этому заказу попасть в результат.

Только для твоего CORE я бы ещё поменял customers на core_customers:

FROM orders ee
INNER JOIN core_customers e
    ON e.id = ee.customer_id;
Так ты проверяешь именно то, что требует твой FOREIGN KEY:

orders.customer_id → core_customers.id.


insert into core_orders (

id,

customer_id,

product_id,

amount,

status,

created_at

)

select

ee.id,

ee.customer_id,

ee.product_id,

ee.amount,

ee.status,

ee.created_at

from orders ee

inner join customers e

on e.id = ee.customer_id;

Да, теперь запрос правильный. 👍

INSERT INTO core_orders (
    id,
    customer_id,
    product_id,
    amount,
    status,
    created_at
)
SELECT
    ee.id,
    ee.customer_id,
    ee.product_id,
    ee.amount,
    ee.status,
    ee.created_at
FROM orders ee
INNER JOIN customers e
    ON e.id = ee.customer_id;
Логика:

orders ee — исходные заказы.

customers e — клиенты.

INNER JOIN оставляет только заказы, для которых клиент существует.

эти записи вставляются в core_orders.

Но для твоей схемы CORE лучше сделать JOIN именно с core_customers:

FROM orders ee
INNER JOIN core_customers e
    ON e.id = ee.customer_id;
Потому что внешний ключ у тебя:

FOREIGN KEY (customer_id)
REFERENCES core_customers(id)
И тогда проверка полностью соответствует ограничению CORE.

Твой вариант с customers тоже логически работает, если customers и core_customers содержат одинаковых клиентов. Но архитектурно правильнее проверять относительно core_customers.


А что с сиротами? Они так и остаются, получается?

Да. При таком INNER JOIN сироты просто не попадут в core_orders, но сами из таблицы orders они никуда не исчезнут.

То есть:

# ┌──────────────────────────┐
# │ orders                   │
# │                          │
# │ нормальный заказ    ✓    │
# │ нормальный заказ    ✓    │
# │ заказ-сирота        ✗    │
# │ заказ-сирота        ✗    │
# └────────────┬─────────────┘
#              │
#              ↓ INNER JOIN
# ┌──────────────────────────┐
# │ core_orders              │
# │                          │
# │ нормальный заказ    ✓    │
# │ нормальный заказ    ✓    │
# └──────────────────────────┘
#
# Сироты остаются в orders
# и НЕ попадают в core_orders


Это правильно для нашей схемы.

Но их желательно отдельно сохранить в error_orders, например:

INSERT INTO error_orders (
    id,
    customer_id,
    product_id,
    amount,
    status,
    created_at,
    error_reason
)
SELECT
    e.id,
    e.customer_id,
    e.product_id,
    e.amount,
    e.status,
    e.created_at,
    'customer not found'
FROM orders e
LEFT JOIN core_customers ee
    ON ee.id = e.customer_id
WHERE ee.id IS NULL;





ШАГ 4   DATA MART (лучше создавать VIEW)
-------------- ---------------------------------------------------

Создай одну аналитическую таблицу:
dm_customer_sales


Она должна показывать итоги продаж по каждому клиенту.
Поля:
customer_id
customer_name
total_orders
completed_orders
total_amount
average_order_amount
Но есть дополнительное бизнес-правило:
В расчётах DATA MART учитываем только completed заказы.
То есть:
cancelled → не считаем
pending   → не считаем
completed → считаем






CREATE VIEW dm_customer_sales as 

select
core_customers.id as customer_id,
core_customers.name as customer_name,

count(case when core_orders.status = 'completed' then 1 end) as total_orders,

count(case when core_orders.status = 'completed' then amount end ) as completed_orders,

sum( case when core_orders.status = 'completed' then amount end) as total_amount,

round(avg(case when core_orders.status = 'completed' then amount end),2) as average_order_amount

from core_customers
LEFT join core_orders
on core_orders.customer_id = core_customers.id 

 GROUP BY core_customers.id , core_customers.name;


SELECT
*
from dm_customer_sales
ORDER BY  completed_orders DESC, total_orders DESC;


посмотреть код View через VS code в папке View чтоб редактировать


конец!!!!


~~~~
