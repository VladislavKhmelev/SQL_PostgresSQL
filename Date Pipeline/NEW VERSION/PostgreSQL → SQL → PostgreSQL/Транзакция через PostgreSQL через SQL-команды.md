~~~
-----------------DDL-------------------


CREATE table source_suppliers (

supplier_id text,
supplier_name text,
country text,
email text,
active text,
contract_start text
);



CREATE table source_products (

product_id text,
product_name text,
category text,
supplier_id text,
unit_price text,
stock_quantity text,
updated_at text
);

-----------------------------------------------

PSQL:


begin;


TRUNCATE source_products, source_suppliers;

\copy source_products FROM 'C:\Users\Vladislav X\Desktop\proj\ver9\data\source_products_40.csv' WITH (FORMAT CSV, HEADER);


\copy source_suppliers FROM 'C:\Users\Vladislav X\Desktop\proj\ver9\data\source_suppliers_40.csv' WITH (FORMAT CSV, HEADER);


commit;




begin;



CREATE TABLE validated_products AS
SELECT
    CASE
        WHEN NULLIF(TRIM(product_id), '') ~ '^\d+$'
             AND NULLIF(TRIM(product_id), '')::INT > 0
        THEN NULLIF(TRIM(product_id), '')::INT
        ELSE NULL
    END AS product_id,

    NULLIF(TRIM(product_name), '') AS product_name,

    NULLIF(TRIM(category), '') AS category,

    CASE
        WHEN NULLIF(TRIM(supplier_id), '') ~ '^\d+(\.\d+)?$'
             AND NULLIF(TRIM(supplier_id), '')::NUMERIC::INT > 0
        THEN NULLIF(TRIM(supplier_id), '')::NUMERIC::INT
        ELSE NULL
    END AS supplier_id,

    CASE
        WHEN NULLIF(TRIM(unit_price), '') ~ '^\d+(\.\d+)?$'
             AND NULLIF(TRIM(unit_price), '')::NUMERIC > 0
        THEN NULLIF(TRIM(unit_price), '')::NUMERIC
        ELSE NULL
    END AS unit_price,

    CASE
        WHEN NULLIF(TRIM(stock_quantity), '') ~ '^\d+$'
             AND NULLIF(TRIM(stock_quantity), '')::INT >= 0
        THEN NULLIF(TRIM(stock_quantity), '')::INT
        ELSE NULL
    END AS stock_quantity,

    NULLIF(TRIM(updated_at), '')::TIMESTAMP AS updated_at,

    NULLIF(
        CONCAT_WS(
            '; ',

            CASE
                WHEN NULLIF(TRIM(product_id), '') IS NULL
                    THEN 'product_id is NULL'

                WHEN NULLIF(TRIM(product_id), '') !~ '^\d+$'
                    THEN 'product_id error format'

                WHEN NULLIF(TRIM(product_id), '')::INT <= 0
                    THEN 'product_id <= 0'
            END,

            CASE
                WHEN NULLIF(INITCAP(TRIM(product_name)), '') IS NULL
                    THEN 'product_name is NULL'
            END,

            CASE
                WHEN NULLIF(INITCAP(TRIM(category)), '') IS NULL
                    THEN 'category is NULL'
            END,

            CASE
                WHEN NULLIF(TRIM(supplier_id), '') IS NULL
                    THEN 'supplier_id is NULL'

                WHEN NULLIF(TRIM(supplier_id), '') !~ '^\d+(\.\d+)?$'
                    THEN 'supplier_id error format'

                WHEN NULLIF(TRIM(supplier_id), '')::NUMERIC::INT <= 0
                    THEN 'supplier_id <= 0'
            END,

            CASE
                WHEN NULLIF(TRIM(unit_price), '') IS NULL
                    THEN 'unit_price is null'

                WHEN NULLIF(TRIM(unit_price), '') !~ '^\d+(\.\d+)?$'
                    THEN 'unit_price error format'

                WHEN NULLIF(TRIM(unit_price), '')::NUMERIC <= 0
                    THEN 'unit_price <=0'
            END,

            CASE
                WHEN NULLIF(TRIM(stock_quantity), '') IS NULL
                    THEN 'stock_quantity is null'

                WHEN NULLIF(TRIM(stock_quantity), '') !~ '^\d+$'
                    THEN 'stock_quantity error format'

                WHEN NULLIF(TRIM(stock_quantity), '')::INT < 0
                    THEN 'stock_quantity < 0'
            END,

            CASE
                WHEN NULLIF(TRIM(updated_at), '') IS NULL
                    THEN 'updated_at is NULL'
            END
        ),
        ''
    ) AS error_reason

FROM source_products;





CREATE TABLE validated_suppliers AS
SELECT
    CASE
        WHEN NULLIF(TRIM(supplier_id), '') ~ '^\d+$'
             AND NULLIF(TRIM(supplier_id), '')::INT > 0
        THEN NULLIF(TRIM(supplier_id), '')::INT
        ELSE NULL
    END AS supplier_id,

    NULLIF(TRIM(supplier_name), '') AS supplier_name,

    NULLIF(TRIM(country), '') AS country,

    NULLIF(TRIM(email), '') AS email,

    CASE
        WHEN NULLIF(LOWER(TRIM(active)), '') IN ('true', 'false')
        THEN NULLIF(LOWER(TRIM(active)), '')::BOOLEAN
        ELSE NULL
    END AS active,

    CASE
        WHEN TRIM(contract_start) ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TRIM(contract_start)::DATE

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
    END AS contract_start,

    NULLIF(
        CONCAT_WS(
            '; ',

            CASE
                WHEN NULLIF(TRIM(supplier_id), '') IS NULL
                    THEN 'supplier_id is NULL'

                WHEN NULLIF(TRIM(supplier_id), '') !~ '^\d+$'
                    THEN 'supplier_id error format'

                WHEN NULLIF(TRIM(supplier_id), '')::INT <= 0
                    THEN 'supplier_id <= 0'
            END,

            CASE
                WHEN NULLIF(INITCAP(TRIM(supplier_name)), '') IS NULL
                    THEN 'supplier_name is NULL'
            END,

            CASE
                WHEN NULLIF(UPPER(TRIM(country)), '') IS NULL
                    THEN 'country is NULL'
            END,

            CASE
                WHEN NULLIF(TRIM(email), '') IS NULL
                    THEN 'email is NULL'

                WHEN NULLIF(TRIM(email), '') NOT LIKE '%@%'
                    THEN 'email error format'
            END,

            CASE
                WHEN NULLIF(TRIM(active), '') IS NULL
                    THEN 'active is null'

                WHEN NULLIF(LOWER(TRIM(active)), '') NOT IN ('true', 'false')
                    THEN 'active error format'
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




commit;


--------------DDL----------------------

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

--------------------------------------------------


begin;

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



commit;





# Следующий этап после этих проверок — связь products → suppliers и проверка orphan-строк через LEFT JOIN.



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




------------Дальше — аналитическая таблица-------------------

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
