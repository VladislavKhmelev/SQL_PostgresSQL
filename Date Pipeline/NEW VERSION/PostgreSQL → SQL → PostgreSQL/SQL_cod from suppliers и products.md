~~~


source_suppliers

# открывать через пандас не обязательно, смотрим на строки и понимаем какой тип и прописываем в структуре, после \copy он преобразует весь файл или сделает ОТКАТ всего файла!!!!!!!!!!!!!



для TEXT потому что преобразование я делаю в валидации с trim(), иначе там все надо менять!!!!!


CREATE table source_suppliers (

supplier_id text,
supplier_name text,
country text,
email text,
active text,
contract_start text
);


source_products

CREATE table source_products (

product_id text,
product_name text,
category text,
supplier_id text,
unit_price text,
stock_quantity text,
updated_at text
);




\copy source_products FROM 'C:\Users\Vladislav X\Desktop\proj\ver9\data\source_products_40.csv' WITH (FORMAT CSV, HEADER);


\copy source_suppliers FROM 'C:\Users\Vladislav X\Desktop\proj\ver9\data\source_suppliers_40.csv' WITH (FORMAT CSV, HEADER);


Он работает примерно так:

Берёт значение из CSV.
Пытается привести его к типу столбца.
Если значение корректное — загружает.
Если значение пустое — обычно загружает NULL.
Если значение не соответствует типу — \copy выдаёт ошибку и останавливает загрузку делает ПОЛНЫЙ ОТКАТ ВСЕГО ФАЙЛА.

# CSV
#   │
#   ├── корректное значение ─────→ нужный тип → ✅ загрузка
#   │
#   ├── пустое значение ──────────→ NULL      → ✅ если разрешён NULL (NOT NULL)
#   │
#   └── несовместимое значение ───→ ошибка    → ❌ загрузка





Если ты хочешь, чтобы плохие значения не ломали загрузку, тогда лучше сначала загружать CSV в RAW-таблицу, где всё TEXT:

И уже после \copy:

# CSV
#   ↓
# \copy
#   ↓
# RAW (всё TEXT)
#   ↓
# VALIDATION
#   ├──→ CORE
#   └──→ ERROR




SELECT
updated_at

from source_products;



SELECT
contract_start

from source_suppliers;






#-----------

trim() 
INITCAP() 
lower() 
NULLIF(  ,'') 

NULLIF(  ,'')  trim() 
#---------


 ~ '^\d+$'    (для int)---- проверка только для целых чисел (для product_id,  stock_quantity)
 1250     → подходит ✅
1250.50  → НЕ подходит ❌


~ '^\d+(\.\d+)?$'    (для numeric)  ---(для unit_price, supplier_id потому что дробное у него значение )
1250       → 1250.00  ✅
1250.50    → 1250.50  ✅
0          → NULL      ❌
-10        → NULL      ❌
abc        → NULL      ❌
пусто      → NULL      ❌


CREATE TABLE validated_products AS
SELECT
     case when  NULLIF(trim(product_id)   ,'') ~ '^\d+$'      
     and  NULLIF(trim(product_id)   ,'')::int > 0 
     then NULLIF(trim(product_id)   ,''):: int
     else NULL end as product_id,
           
   NULLIF( trim(product_name)  ,'') as  product_name   ,  

    NULLIF(  trim(category)   ,'') as  category,    

    case when  NULLIF( trim(supplier_id)   ,'') ~ '^\d+(\.\d+)?$'  
    and    NULLIF( trim(supplier_id)   ,'')::numeric::int > 0
    then NULLIF( trim(supplier_id)   ,'')::numeric::int
    else NULL end as supplier_id,

    case when   NULLIF( trim(unit_price)   ,'') ~ '^\d+(\.\d+)?$'   
    and NULLIF( trim(unit_price)   ,'') ::numeric > 0
    then NULLIF( trim(unit_price)   ,'') ::NUMERIC
    else null end as unit_price,

      case when  NULLIF( trim(stock_quantity)  ,'')  ~ '^\d+$'  
      and            NULLIF( trim(stock_quantity)  ,'') ::int >= 0
      then NULLIF( trim(stock_quantity)  ,'')::int 
      else null end as    stock_quantity,

          NULLIF( trim(updated_at)    ,'')::TIMESTAMP as  updated_at,



     NULLIF(
    CONCAT_WS(
        '; ',
        CASE
            WHEN  NULLIF(TRIM(product_id),'') IS NULL
                THEN 'product_id is NULL'

            WHEN NULLIF(TRIM(product_id), '')  !~ '^\d+$' 
                    THEN 'product_id error format'

            WHEN  NULLIF(TRIM(product_id),'')::int <= 0
                THEN 'product_id <= 0'


        END,

        CASE
            WHEN  NULLIF(initcap(trim(product_name)),'' ) IS NULL
                THEN 'product_name is NULL'
        END,

        CASE
            WHEN   NULLIF(INITCAP(trim(category) ),'') IS NULL
                THEN 'category is NULL'
        END,

        CASE
            WHEN  NULLIF(trim(supplier_id),''  )  IS NULL
                THEN 'supplier_id is NULL'
            
            WHEN  NULLIF(trim(supplier_id),''  )  !~ '^\d+(\.\d+)?$'
                THEN 'supplier_id error format'

            WHEN  NULLIF(trim(supplier_id),''  )::numeric::int <= 0
                THEN 'supplier_id <= 0'
             
        END,

        CASE
            WHEN  NULLIF(TRIM(unit_price),'') is null 
            then 'unit_price is null'

            WHEN  NULLIF(TRIM(unit_price),'') !~ '^\d+(\.\d+)?$' 
            then 'unit_price error format'

            WHEN  NULLIF(TRIM(unit_price),'')::numeric  <=0
            then 'unit_price <=0'
          
        END,

        CASE
            WHEN  NULLIF(TRIM(stock_quantity),'') is NULL
            then 'stock_quantity is null'

             WHEN  NULLIF(TRIM(stock_quantity),'') !~ '^\d+$'
            then 'stock_quantity error format'

             WHEN  NULLIF(TRIM(stock_quantity),'')::int <0
            then 'stock_quantity < 0'
         
        END,

        CASE
            WHEN  NULLIF(trim(updated_at),'')    IS NULL
                THEN 'updated_at is NULL'
        END

        
    ),
    ''
) AS error_reason

FROM source_products;



-- По твоей логике

-- Она сейчас такая:
для SELECT:
-- product_id → положительное целое → int, иначе NULL
-- product_name → убираем пробелы, пустое → NULL
-- category → убираем пробелы, пустое → NULL
-- supplier_id → цифры и > 0 → int, иначе NULL
-- unit_price → цифры и > 0 → numeric, иначе NULL
-- stock_quantity → положительное целое → int, иначе NULL
-- updated_at → пустое → NULL, иначе преобразуем в timestamp


ставим когда условие проверяем валидации!!!:

!~ '^\d+$'    (! отрицание ставим, для int)
!~ '^\d+(\.\d+)?$'    (для numeric)



CREATE table error_products (
error_id SERIAL PRIMARY key,
product_id int,         
product_name text,     
category text, 
supplier_id int, 
unit_price NUMERIC, 
stock_quantity int,          
updated_at TIMESTAMP,
error_reason text
);


CREATE table products (

product_id int PRIMARY KEY,         
product_name text,     
category text, 
supplier_id int, 
unit_price NUMERIC, 
stock_quantity int,          
updated_at TIMESTAMP
);



INSERT INTO error_products (

  product_id,
    product_name,
    category,
    supplier_id,
    unit_price,
    stock_quantity,
    updated_at,
    error_reason
)
SELECT

  product_id,
    product_name,
    category,
    supplier_id,
    unit_price,
    stock_quantity,
    updated_at,
    error_reason


FROM validated_products


WHERE error_reason IS NOT NULL;







INSERT INTO products (
product_id,
    product_name,
    category,
    supplier_id,
    unit_price,
    stock_quantity,
    updated_at

)
SELECT
product_id,
    product_name,
    category,
    supplier_id,
    unit_price,
    stock_quantity,
    updated_at

FROM (
    select
    product_id,
    ROW_NUMBER() over(PARTITION BY product_id ORDER BY updated_at DESC ) as r,
    product_name,
    category ,
    supplier_id ,
    unit_price ,
    stock_quantity ,
    updated_at,
    error_reason


    from validated_products

) tt 

where r = 1 and error_reason is null



on conflict(product_id)

DO UPDATE SET

product_name = EXCLUDED.product_name,
category = EXCLUDED.category,
supplier_id = EXCLUDED.supplier_id,
unit_price = EXCLUDED.unit_price,
stock_quantity = EXCLUDED.stock_quantity,
updated_at = EXCLUDED.updated_at;














для suppliers теперь аналогично 

select 
contract_start

from source_suppliers;






#-----------


# ┌──────────────────────────────────────────────────────────────┐
# │              БИЗНЕС-ПРАВИЛА source_suppliers                │
# ├──────────────────────────────────────────────────────────────┤
# │                                                              │
# │  1. supplier_id                                              │
# │     • не NULL                                                │
# │     • целое число                                             │
# │     • > 0                                                    │
# │     • уникальный                                              │
# │                                                              │
# │  2. supplier_name                                            │
# │     • не NULL                                                │
# │     • после TRIM() не пустой                                 │
# │                                                              │
# │  3. country                                                  │
# │     • не NULL                                                │
# │     • после TRIM() не пустой                                 │
# │                                                              │
# │  4. email                                                    │
# │     • не NULL                                                │
# │     • после TRIM() не пустой                                 │
# │     • должен содержать @                                     │
# │                                                              │
# │  5. active                                                   │
# │     • не NULL                                                │
# │     • только True / False                                    │
# │                                                              │
# │  6. contract_start                                           │
# │     • не NULL                                                │
# │     • корректная дата                                        │
# │     • YYYY-MM-DD                                             │
# │     • DD.MM.YYYY                                             │
# │     • YYYY/MM/DD                                             │
# │                                                              │
# │  7. Дубликаты supplier_id                                    │
# │     • если supplier_id встречается > 1 раза → ошибка        │
# │     • при дедупликации оставляем самую свежую запись         │
# │       по contract_start                                      │
# │                                                              │
# └──────────────────────────────────────────────────────────────┘




trim() 
INITCAP() 
lower() 
NULLIF(  ,'') 

NULLIF(  ,'')  trim() 
#---------


 ~ '^\d+$'    (для int)---- проверка только для целых чисел (для product_id,  stock_quantity)
 1250     → подходит ✅
1250.50  → НЕ подходит ❌


~ '^\d+(\.\d+)?$'    (для numeric)  ---(для unit_price, supplier_id потому что дробное у него значение )
1250       → 1250.00  ✅
1250.50    → 1250.50  ✅
0          → NULL      ❌
-10        → NULL      ❌
abc        → NULL      ❌
пусто      → NULL      ❌


NULLIF(  ,'')  trim() 

CREATE TABLE validated_suppliers AS
SELECT
     case when NULLIF(   trim(supplier_id  ) ,'')  ~ '^\d+$'
     and  NULLIF(   trim(supplier_id  ) ,'')::int > 0
     then     NULLIF(   trim(supplier_id  ) ,'')::int
     else null end as supplier_id,

     NULLIF(  trim(supplier_name) ,'')  as   supplier_name,

     NULLIF(  trim(country)  ,'') as      country,

     NULLIF(  trim(email) ,'')    as email,

     case when NULLIF(  lower(trim(active))   ,'') in ('true','false')
     then NULLIF(  lower(trim(active))   ,'') ::BOOLEAN
     else null end as active,
     
   CASE
        WHEN TRIM(contract_start) ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TRIM(contract_start)::date

        WHEN TRIM(contract_start) ~ '^\d{2}\.\d{2}\.\d{4}$'
        THEN TO_DATE(
            TRIM(contract_start),
            'DD.MM.YYYY'
        )

        WHEN TRIM(contract_start) ~ '^\d{4}/\d{2}/\d{2}$'
        THEN TO_DATE(
            TRIM(contract_start),
            'YYYY/MM/DD'
        )

        ELSE NULL
    END as contract_start, 



     NULLIF(
    CONCAT_WS(
        '; ',
        CASE
            WHEN  NULLIF(TRIM(supplier_id),'') IS NULL
                THEN 'supplier_id is NULL'

            WHEN NULLIF(TRIM(supplier_id), '')  !~ '^\d+$' 
                    THEN 'supplier_id error format'

            WHEN  NULLIF(TRIM(supplier_id),'')::int <= 0
                THEN 'supplier_id <= 0'


        END,

        CASE
            WHEN  NULLIF(initcap(trim(supplier_name)),'' ) IS NULL
                THEN 'supplier_name is NULL'
        END,

        CASE
            WHEN   NULLIF(Upper(trim(country) ),'') IS NULL
                THEN 'country is NULL'
        END,

        CASE
            WHEN  NULLIF(trim(email),''  )  IS NULL
                THEN 'email is NULL'
            
            WHEN  NULLIF(trim(email),''  ) not like '%@%'
                THEN 'email error format'
             
        END,

        CASE
            WHEN  NULLIF(TRIM(active),'') is null 
            then 'active is null'

            WHEN  NULLIF(lower(TRIM(active)) ,'') not in ('true','false')
            then 'active error format'
          
        END,

        CASE
        WHEN NULLIF(TRIM(contract_start), '') IS NULL
            THEN 'contract_start is NULL'

        WHEN NULLIF(TRIM(contract_start), '') !~ '^\d{4}-\d{2}-\d{2}$'
            AND NULLIF(TRIM(contract_start), '') !~ '^\d{2}\.\d{2}\.\d{4}$'
            AND NULLIF(TRIM(contract_start), '') !~ '^\d{4}/\d{2}/\d{2}$'
            THEN 'contract_start error format'
        END

     



      
            ),
            ''
        ) AS error_reason

        FROM source_suppliers;



-- По твоей логике

-- Она сейчас такая:
для SELECT:
-- product_id → положительное целое → int, иначе NULL
-- product_name → убираем пробелы, пустое → NULL
-- category → убираем пробелы, пустое → NULL
-- supplier_id → цифры и > 0 → int, иначе NULL
-- unit_price → цифры и > 0 → numeric, иначе NULL
-- stock_quantity → положительное целое → int, иначе NULL
-- updated_at → пустое → NULL, иначе преобразуем в timestamp


ставим когда условие проверяем валидации!!!:

!~ '^\d+$'    (! отрицание ставим, для int)
!~ '^\d+(\.\d+)?$'    (для numeric)



CREATE table error_suppliers  (
error_id SERIAL PRIMARY key,
supplier_id int,    
supplier_name text, 
country      text,             
email  text,
active  BOOLEAN,
contract_start date,
error_reason text
);


CREATE table suppliers (

supplier_id int PRIMARY KEY,    
supplier_name text, 
country      text,             
email  text,
active  BOOLEAN,
contract_start date
);



INSERT INTO error_suppliers (


supplier_id ,
supplier_name ,
country    ,          
email  ,
active ,
contract_start ,
error_reason
)
SELECT


supplier_id ,
supplier_name ,
country      ,        
email  ,
active ,
contract_start ,
error_reason


FROM validated_suppliers


WHERE error_reason IS NOT NULL;








INSERT INTO suppliers (
supplier_id ,
supplier_name ,
country      ,        
email  ,
active ,
contract_start 

)
SELECT
supplier_id ,
supplier_name ,
country      ,        
email  ,
active ,
contract_start 

FROM (
    select
    supplier_id,
    ROW_NUMBER() over(PARTITION BY supplier_id ORDER BY contract_start DESC nulls LAST) as r,
    supplier_name,
    country ,
    email ,
    active ,
    contract_start,
    error_reason             -- надо добавить!!!!
   


    from validated_suppliers

) tt 

where r = 1 AND error_reason IS NULL



on conflict(supplier_id)

DO UPDATE SET

supplier_name = EXCLUDED.supplier_name,
country = EXCLUDED.country,
email = EXCLUDED.email,
active = EXCLUDED.active,
contract_start = EXCLUDED.contract_start;







# Следующий этап после этих проверок — связь products → suppliers и проверка orphan-строк через LEFT JOIN.


Задание

Сделай проверку orphan-товаров:

взять products;
соединить с suppliers через supplier_id;
найти товары, у которых нет соответствующего поставщика;
вывести product_id, product_name, supplier_id.

Используй LEFT JOIN.

Пока ничего не удаляй и не переносись в error_products — только найди orphan-строки.


SELECT
products.supplier_id,
products.product_id


from products
left join suppliers
on suppliers.supplier_id = products.supplier_id

where suppliers.supplier_id is null;


Следующее задание

Если запрос нашёл orphan-строки:

Удали их из products.
Добавь эти же строки в error_products.
В error_reason установи значение orphan.

Делай это через DELETE (из products where = orhan ) и INSERT (в error_products из validated_products где products.supplier_id = orphan)






Дальше — аналитическая таблица.

Задание

Создай products_mart, объединив:

products
suppliers

через:

products.supplier_id = suppliers.supplier_id

В аналитической таблице должны быть:

supplier_id
supplier_name
country
количество товаров поставщика
общий остаток товаров
общая стоимость остатков (unit_price * stock_quantity)
средняя цена товара
количество категорий

Используй LEFT JOIN от suppliers к products



CREATE TABLE products_mart AS
SELECT
    suppliers.supplier_id AS supplier_id,
    suppliers.supplier_name AS supplier_name,
    suppliers.country AS country,
    COUNT(products.supplier_id) AS count_products,
    COALESCE(SUM(products.stock_quantity), 0) AS stock,
    COALESCE(SUM(products.unit_price * products.stock_quantity), 0) AS total_cost,
    ROUND(COALESCE(AVG(products.unit_price), 0), 2) AS avg_price,
    COUNT(DISTINCT products.category) AS count_categories
FROM suppliers
LEFT JOIN products
    ON products.supplier_id = suppliers.supplier_id
GROUP BY
    suppliers.supplier_id,
    suppliers.supplier_name,
    suppliers.country;


~~~
