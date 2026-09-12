~~~
suppliers                   

raw_suppliers                       




CREATE TABLE raw_suppliers (

supplier_id   text,  
supplier_name  text,
country             text,      
email text,
contract_start text, 
active text

);


# пишим в одной строке + '' вместо " "

\copy raw_suppliers FROM 'C:\Users\Vladislav X\Desktop\proj\psql_copy\data\suppliers.csv' WITH (FORMAT csv, HEADER true);


### смотрим дату как выглядит ее форматы
SELECT
    supplier_id,
    contract_start
FROM raw_suppliers;





# смотрим как будет выглядеть после преобразования даты

# для date
SELECT
    supplier_id,
    contract_start,

    CASE
        WHEN contract_start ~ '^\d{4}-\d{2}-\d{2}$'
            THEN contract_start::date

        WHEN contract_start ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN to_date(contract_start, 'DD.MM.YYYY')

        WHEN contract_start ~ '^\d{4}/\d{2}/\d{2}$'
            THEN to_date(contract_start, 'YYYY/MM/DD')

        ELSE NULL
    END AS contract_start_date

FROM raw_suppliers;

#-----------

trim() 
INITCAP() 
upper()
lower() 
NULLIF(  ,'') 

   
#---------
#!!!!!ДОБАВЛЯЕМ ПРАВИЛО УБРАТЬ СИРОТ


CREATE TABLE validated_suppliers AS
SELECT
NULLIF(   trim(supplier_id)   ,'')::int as     supplier_id,       
NULLIF(   INITCAP( trim(supplier_name)   )  ,'')::text as         supplier_name, 
NULLIF(  upper(trim(country) )  ,'') ::text as      country,                     
NULLIF( lower(trim(email)  )   ,'') ::text as  email ,       
NULLIF( INITCAP(trim(active)  )  ,'') ::boolean as  active ,     

     CASE
        WHEN contract_start ~ '^\d{4}-\d{2}-\d{2}$'
            THEN contract_start::date

        WHEN contract_start ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN to_date(contract_start, 'DD.MM.YYYY')

        WHEN contract_start ~ '^\d{4}/\d{2}/\d{2}$'
            THEN to_date(contract_start, 'YYYY/MM/DD')

        ELSE NULL

    END AS contract_start,


     NULLIF(
    CONCAT_WS(
        '; ',
        CASE
            WHEN  NULLIF(   trim(supplier_id)   ,'')  is null or NULLIF(   trim(supplier_id)   ,'')::int <=0
                THEN 'supplier_id <= 0 or supplier_id is null'
        END,

        CASE
            WHEN NULLIF(   INITCAP( trim(supplier_name)   )  ,'') is null  
                THEN 'supplier_name is NULL'
        END,

        CASE
            WHEN  NULLIF(UPPER(TRIM(country)), '')  is null
                THEN 'country is NULL'
        END,

        CASE
            WHEN  NULLIF( lower(trim(email)  )   ,'') is null or NULLIF( lower(trim(email)  )   ,'') not like '%@%'
                THEN 'email is NULL or email not @'
        END,

        CASE
            WHEN   NULLIF( trim(contract_start)    ,'') is null 
            THEN 'contract_start is null'

            
            when   NULLIF( trim(contract_start)    ,'') !~ '^\d{4}-\d{2}-\d{2}$'
            and   NULLIF( trim(contract_start)    ,'') !~ '^\d{2}\.\d{2}\.\d{4}$'
            and NULLIF( trim(contract_start)    ,'') !~ '^\d{4}/\d{2}/\d{2}$'
             
                THEN 'contract_start error format'
        END,

        CASE
            WHEN  NULLIF( INITCAP(trim(active)  )  ,'') is null or NULLIF( INITCAP(trim(active)  )  ,'') not in ('True','False')
                THEN 'active is null or not True/False'
        END

        

        
    ),
    ''
) AS error_reason

FROM raw_suppliers;





CREATE TABLE error_suppliers (

supplier_id int primary key,  
supplier_name  text,
country  text,      
email text,
contract_start date, 
active boolean,
error_reason text
);




INSERT into error_suppliers  (
supplier_id,    
supplier_name,
country,                   
email, 
contract_start, 
active,
error_reason
)
SELECT
supplier_id,    
supplier_name,
country,                   
email, 
contract_start, 
active,
error_reason

FROM validated_suppliers

WHERE error_reason IS NOT NULL
on conflict (supplier_id)
do nothing;




CREATE TABLE suppliers (

supplier_id int primary key,    
supplier_name text,
country text,                   
email text, 
contract_start date, 
active boolean

);


INSERT INTO suppliers (
supplier_id,    
supplier_name,
country,                   
email, 
contract_start, 
active
)
SELECT
supplier_id,    
supplier_name,
country,                   
email, 
contract_start, 
active


FROM validated_suppliers

WHERE error_reason IS NULL

ON CONFLICT (supplier_id)
DO UPDATE SET
    supplier_name = EXCLUDED.supplier_name,
    country = EXCLUDED.country,
    email = EXCLUDED.email,
    contract_start = EXCLUDED.contract_start,
    active = EXCLUDED.active;


# забыл написать условие для сирот в валидации. (ребенок есть родителя нет), в селекте надо смотреть номер ребенка, родителя он не покажет, будет пусто


SELECT
products.product_id,
products.product_name,
suppliers.supplier_id

from products
left join suppliers
on suppliers.supplier_id = products.supplier_id

where suppliers.supplier_id is null;

#сирота
 product_id
------------
        108
(1 строка)


DELETE FROM products
WHERE product_id = 108;


# добавляю сироту в ошибки

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

WHERE product_id = 108

on conflict (product_id)
do nothing;



UPDATE error_products
SET error_reason = 'orphan'
WHERE product_id = 108;




# делаем проверку по кличеству 
#-------------------------------------------------------

SELECT
    (SELECT COUNT(*) FROM raw_suppliers) AS raw_suppliers,
    (SELECT COUNT(*) FROM validated_suppliers) AS validated_suppliers,
    (SELECT COUNT(*) FROM suppliers) AS suppliers,
    (SELECT COUNT(*) FROM error_suppliers) AS error_suppliers;


 raw_suppliers | validated_suppliers | suppliers | error_suppliers
---------------+---------------------+-----------+-----------------
             7 |                   7 |         4 |               3
(1 строка)



CREATE TABLE mart_table AS
SELECT
    suppliers.supplier_id AS supplier_id,
    suppliers.supplier_name AS supplier_name,
    suppliers.country AS country,

    COUNT(products.product_id) AS product_count,

    COALESCE(SUM(products.stock_quantity), 0) AS total_stock,

    COALESCE(
        SUM(products.unit_price * products.stock_quantity),
        0
    ) AS total_stock_value,

    ROUND(COALESCE(AVG(products.unit_price), 0), 2) AS avg_product_price,

    COUNT(DISTINCT products.category) AS categories_count,

    CASE
        WHEN COALESCE(SUM(products.stock_quantity), 0) = 0
            THEN 'OUT_OF_STOCK'

        WHEN COALESCE(SUM(products.stock_quantity), 0) < 50
            THEN 'LOW_STOCK'

        ELSE 'IN_STOCK'
    END AS stock_status

FROM suppliers

LEFT JOIN products
    ON products.supplier_id = suppliers.supplier_id

GROUP BY
    suppliers.supplier_id,
    suppliers.supplier_name,
    suppliers.country;




#забыл добавить внешний ключ

ALTER TABLE products
ADD CONSTRAINT fk
FOREIGN KEY (supplier_id)
REFERENCES suppliers(supplier_id);

~~~
