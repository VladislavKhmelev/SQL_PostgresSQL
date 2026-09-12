~~~
CREATE TABLE raw_products (

product_id   text,      
product_name     text,
category  text,
supplier_id  text,
unit_price  text,
stock_quantity   text,        
updated_at text

);



# пишим в одной строке + '' вместо " "

\copy raw_products FROM 'C:\Users\Vladislav X\Desktop\proj\psql_copy\data\products.csv' WITH (FORMAT csv, HEADER true);


### смотрим дату как выглядит ее форматы
SELECT
    product_id,
    updated_at
FROM raw_products;



# смотрим как будет выглядеть после преобразования даты

# для timestamp

SELECT
    product_id,
    updated_at,

    CASE
        WHEN updated_at ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
            THEN updated_at::timestamp

        WHEN updated_at ~ '^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}$'
            THEN to_timestamp(updated_at, 'DD.MM.YYYY HH24:MI:SS')

        WHEN updated_at ~ '^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$'
            THEN to_timestamp(updated_at, 'YYYY/MM/DD HH24:MI:SS')

        ELSE NULL
    END AS updated_at_ts

FROM raw_products;






# ┌──────────────────────────────────────────────────────────────┐
# │                    VALIDATION                                │
# │                                                              │
# │ product_id      → не должен быть NULL                        │
# │ product_name    → не должен быть NULL                        │
# │ category        → не должна быть NULL                        │
# │ supplier_id     → не должен быть NULL                        │
# │ unit_price      → должен быть > 0                            │
# │ stock_quantity  → должен быть >= 0                           │
# │ updated_at      → не должен быть NULL                        │
# └──────────────────────────────────────────────────────────────┘

#-----------

trim() 
INITCAP() 
lower() 
NULLIF(  ,'') 


#---------


CREATE TABLE validated_products AS
SELECT
    NULLIF( TRIM(product_id),'')::int  AS product_id,

    NULLIF(INITCAP(TRIM(product_name))  ,'')::text as  product_name,

     NULLIF( INITCAP(TRIM(category)) ,'')::text as category,

    NULLIF( TRIM(supplier_id),'')::NUMERIC::int  as supplier_id,

    NULLIF(TRIM(unit_price) ,'')::NUMERIC(10,2)  as unit_price,

    NULLIF(TRIM(stock_quantity) ,'')::int  as stock_quantity,

    CASE
        WHEN updated_at ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
            THEN updated_at::TIMESTAMP

        WHEN updated_at ~ '^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}$'
            THEN to_timestamp(updated_at, 'DD.MM.YYYY HH24:MI:SS')

        WHEN updated_at ~ '^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$'
            THEN to_timestamp(updated_at, 'YYYY/MM/DD HH24:MI:SS')

        ELSE NULL
    END AS updated_at,


     NULLIF(
    CONCAT_WS(
        '; ',
        CASE
            WHEN  NULLIF(TRIM(product_id),'') IS NULL
                THEN 'product_id is NULL'
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
        END,

        CASE
            WHEN  NULLIF(TRIM(unit_price),'') ::numeric(10,2) <= 0
            OR NULLIF(TRIM(unit_price), '') IS NULL
                THEN 'unit_price <= 0 or NULL'
        END,

        CASE
            WHEN  NULLIF(TRIM(stock_quantity),'') ::int  < 0
            OR NULLIF(TRIM(stock_quantity), '') IS NULL
                THEN 'stock_quantity < 0 or NULL'
        END,

        CASE
            WHEN  NULLIF(trim(updated_at),'')    IS NULL
                THEN 'updated_at is NULL'
        END
    ),
    ''
) AS error_reason

FROM raw_products;





CREATE TABLE error_products (
product_id int primary key,  
product_name text,
category text,
supplier_id int,
unit_price numeric(10,2),
stock_quantity int,
updated_at TIMESTAMP,
error_reason text


);


INSERT into error_products  (
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

WHERE error_reason IS NOT NULL
on conflict (product_id)
do nothing;



CREATE TABLE products (

product_id int primary key,  
product_name text,
category text,
supplier_id int,
unit_price numeric(10,2),
stock_quantity int,
updated_at TIMESTAMP

);



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
FROM validated_products
WHERE error_reason IS NULL
ON CONFLICT (product_id)
DO UPDATE SET
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    supplier_id = EXCLUDED.supplier_id,
    unit_price = EXCLUDED.unit_price,
    stock_quantity = EXCLUDED.stock_quantity,
    updated_at = EXCLUDED.updated_at;




# делаем проверку по кличеству 
#-------------------------------------------------------
SELECT COUNT(*) AS total
FROM validated_products;

SELECT COUNT(*) AS valid
FROM validated_products
WHERE error_reason IS NULL;

SELECT COUNT(*) AS errors
FROM validated_products
WHERE error_reason IS NOT NULL;






SELECT
    (SELECT COUNT(*) FROM validated_products) AS validated,
    (SELECT COUNT(*) FROM products) AS products,
    (SELECT COUNT(*) FROM error_products) AS error_products;


#-------------------------------------------------------

~~~
