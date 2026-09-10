
df = pd.read_csv (pandas + техническая подготовка) + df.to_csv(buffer, index=False) + psycopg2 + TRUNCATE TABLE staging_customers + COPY(cursor.copy_expert ---> FROM STDIN, buffer) +  CREATE TEMP TABLE validated_customers (+ error_reason где прописываем что не входит в бизнес правила  + error_customers (error_reason) + customers ( cursor.execute("""INSERT INTO + ON CONFLICT (id) DO UPDATE)




схемой слоёв данных.

далее SQL
таблицы + структура

-------STAGING TABLE-----

staging_customers (без PK)
staging_orders (без PK)

------ERROR TABLE-----

error_customers ( + error_reason ) (без PK)
error_orders ( + error_reason ) (без PK)

-----TEMP TABLE-----

---TEMP TABLE validated_orders / validated_customers----(бизнес правила + колонка error_reason )

-----TABLE-----

customers (PK + check + NOT NULL)
orders (PK + check + NOT NULL)

-----CORE-----

core_customers (PK + check + NOT NULL) + (FOREIGN KEY + UNIQUE) + [customers  --- INSERT into --- core_customers] 

core_orders (PK + check + NOT NULL) + (FOREIGN KEY + UNIQUE) + [orders/inner join customers (Сироты остаются в orders) ---- INSERT into ---- core_orders ]





error_orders --->> добавляем сирот [orders/LEFT JOIN core_customers ---- WHERE core_customers.id IS NULL ----INSERT INTO error_orders]


------DATA MART (аналитика)-----

CREATE VIEW dm_customer_sales as 


