~~~

CREATE TABLE raw_customers (
customer_id   text,         
name           text, 
email  text,
age     text, 
city text
);




trim() 
INITCAP() 
lower() 
NULLIF(  ,'') 

NULLIF(  ,'')  trim() 
#---------

 ~ '^\d+$'    (для int)
 ~ '^\d+(\.\d+)?$'    (для numeric)


CREATE Table validated_customers as


SELECT

 case when NULLIF(trim(customer_id)     ,'')    ~ '^\d+$'
 then NULLIF(trim(customer_id)     ,'')::int 
 else null end as    customer_id,

 NULLIF( trim(name)    ,'')  as      name  ,   

 NULLIF( trim(email)  ,'')      as email,

 case when NULLIF(  trim(age)  ,'')  ~ '^\d+$'
 then NULLIF(  trim(age)  ,'')::int 

 else null end as  age,

 NULLIF(  trim(city) ,'')  as city,

NULLIF(
    CONCAT_WS(
        '; ',
        CASE
            WHEN  NULLIF(trim(customer_id)     ,'') is null
                THEN 'customer_id is NULL'
            
            WHEN NULLIF(TRIM(customer_id), '')  !~ '^\d+$' 
                    THEN 'customer_id error format'
            
            WHEN NULLIF(TRIM(customer_id), '')::int <= 0
                    THEN 'customer_id <=0'
     
        END,

        CASE
            WHEN  NULLIF( trim(name)    ,'') is null
                THEN 'name is NULL'
        END,

        CASE
            WHEN    NULLIF( trim(email)  ,'') IS NULL
                THEN 'email is NULL'
            
             WHEN    NULLIF( trim(email)  ,'') not like '%@%'
                THEN 'email not @'

            
        END,

        case 

        when NULLIF(  trim(age)  ,'') is null
            then 'age is null'

        when NULLIF(  trim(age)  ,'')  !~ '^\d+$'
            then 'age error format'

        when NULLIF(  trim(age)  ,'')::int < 18
            then 'age < 18'

        end,

             CASE
            WHEN  NULLIF(  trim(city) ,'') is null
                THEN 'city is NULL'
            
        END
        
    ),
    ''
) AS error_reason

FROM raw_customers;


CREATE table core_customers (

customer_id int PRIMARY KEY,         
name     text,       
email text, 
age   int,   
city text

);




CREATE table error_customers (

error_id serial PRIMARY key,
customer_id int,         
name     text,       
email text, 
age   int,   
city text,
error_reason text

);




------------------------------------------------------------
заказы 


CREATE table raw_orders (

order_id  text,
customer_id text, 
car_id text, 
days text, 
daily_price text,  
total_amount text,    
status text, 
order_date text

);







trim() 
INITCAP() 
lower() 
NULLIF(  ,'') 

NULLIF(  ,'')  trim() 
#---------

 ~ '^\d+$'    (для int)
 ~ '^\d+(\.\d+)?$'    (для numeric)


CREATE TABLE validated_orders AS

SELECT

    CASE
        WHEN NULLIF(TRIM(order_id), '') ~ '^\d+$'
            THEN NULLIF(TRIM(order_id), '')::int
        ELSE NULL
    END AS order_id,


    CASE
        WHEN NULLIF(TRIM(customer_id), '') ~ '^\d+$'
            THEN NULLIF(TRIM(customer_id), '')::int
        ELSE NULL
    END AS customer_id,


    CASE
        WHEN NULLIF(TRIM(car_id), '') ~ '^\d+$'
            THEN NULLIF(TRIM(car_id), '')::int
        ELSE NULL
    END AS car_id,


    CASE
        WHEN NULLIF(TRIM(days), '') ~ '^\d+$'
            THEN NULLIF(TRIM(days), '')::int
        ELSE NULL
    END AS days,


    CASE
        WHEN NULLIF(TRIM(daily_price), '') ~ '^\d+$'
            THEN NULLIF(TRIM(daily_price), '')::int
        ELSE NULL
    END AS daily_price,


    CASE
        WHEN NULLIF(TRIM(total_amount), '') ~ '^\d+(\.\d+)?$'
            THEN NULLIF(TRIM(total_amount), '')::numeric
        ELSE NULL
    END AS total_amount,


    NULLIF(TRIM(status), '') AS status,


    CASE
        WHEN NULLIF(TRIM(order_date), '') ~ '^\d{4}-\d{2}-\d{2}$'
            THEN NULLIF(TRIM(order_date), '')::date

        WHEN NULLIF(TRIM(order_date), '') ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_DATE(
                NULLIF(TRIM(order_date), ''),
                'DD.MM.YYYY'
            )

        WHEN NULLIF(TRIM(order_date), '') ~ '^\d{4}/\d{2}/\d{2}$'
            THEN TO_DATE(
                NULLIF(TRIM(order_date), ''),
                'YYYY/MM/DD'
            )

        ELSE NULL
    END AS order_date,


    NULLIF(
        CONCAT_WS(
            '; ',

            CASE
                WHEN NULLIF(TRIM(order_id), '') IS NULL
                    THEN 'order_id is NULL'

                WHEN NULLIF(TRIM(order_id), '') !~ '^\d+$'
                    THEN 'order_id error format'

                WHEN NULLIF(TRIM(order_id), '')::int <= 0
                    THEN 'order_id <=0'
            END,


            CASE
                WHEN NULLIF(TRIM(customer_id), '') IS NULL
                    THEN 'customer_id is NULL'

                WHEN NULLIF(TRIM(customer_id), '') !~ '^\d+$'
                    THEN 'customer_id error format'

                WHEN NULLIF(TRIM(customer_id), '')::int <= 0
                    THEN 'customer_id <=0'
            END,


            CASE
                WHEN NULLIF(TRIM(car_id), '') IS NULL
                    THEN 'car_id is NULL'

                WHEN NULLIF(TRIM(car_id), '') !~ '^\d+$'
                    THEN 'car_id error format'

                WHEN NULLIF(TRIM(car_id), '')::int <= 0
                    THEN 'car_id <=0'
            END,


            CASE
                WHEN NULLIF(TRIM(days), '') IS NULL
                    THEN 'days is NULL'

                WHEN NULLIF(TRIM(days), '') !~ '^\d+$'
                    THEN 'days error format'

                WHEN NULLIF(TRIM(days), '')::int <= 0
                    THEN 'days <=0'
            END,


            CASE
                WHEN NULLIF(TRIM(daily_price), '') IS NULL
                    THEN 'daily_price is NULL'

                WHEN NULLIF(TRIM(daily_price), '') !~ '^\d+$'
                    THEN 'daily_price error format'

                WHEN NULLIF(TRIM(daily_price), '')::int <= 0
                    THEN 'daily_price <=0'
            END,


            CASE
                WHEN NULLIF(TRIM(total_amount), '') IS NULL
                    THEN 'total_amount is NULL'

                WHEN NULLIF(TRIM(total_amount), '') !~ '^\d+(\.\d+)?$'
                    THEN 'total_amount error format'

                WHEN NULLIF(TRIM(total_amount), '')::numeric <= 0
                    THEN 'total_amount <=0'

                WHEN NULLIF(TRIM(total_amount), '')::numeric
                     != NULLIF(TRIM(days), '')::int
                        * NULLIF(TRIM(daily_price), '')::int
                    THEN 'total_amount != days * daily_price'
            END,


            CASE
                WHEN NULLIF(TRIM(status), '') IS NULL
                    THEN 'status is NULL'

                WHEN NULLIF(TRIM(status), '')
                     NOT IN ('completed', 'cancelled', 'pending')
                    THEN 'status is error'
            END,


            CASE
                WHEN NULLIF(TRIM(order_date), '') IS NULL
                    THEN 'order_date is NULL'

                WHEN NULLIF(TRIM(order_date), '') !~ '^\d{4}-\d{2}-\d{2}$'
                 AND NULLIF(TRIM(order_date), '') !~ '^\d{2}\.\d{2}\.\d{4}$'
                 AND NULLIF(TRIM(order_date), '') !~ '^\d{4}/\d{2}/\d{2}$'
                    THEN 'order_date error format'

                WHEN NULLIF(TRIM(order_date), '')::date > CURRENT_DATE
                    THEN 'order_date is future'
            END

        ), 
        ''
    ) AS error_reason 

FROM raw_orders;




CREATE Table error_orders (
error_id serial PRIMARY KEY,
order_id int, 
customer_id  int,
car_id  int,
days  int,
daily_price  int,
total_amount  numeric,   
status  text,
order_date date,
error_reason text

);


CREATE TABLE core_orders (


order_id int PRIMARY key, 
customer_id  int,
car_id  int,
days  int,
daily_price  int,
total_amount  numeric,   
status  text,
order_date date


);



CREATE TABLE dm_customer_orders AS

SELECT
    core_customers.customer_id AS номер,
    core_customers.name AS имя,
    core_customers.city AS город,

    COUNT(order_id) AS order_count,

    COALESCE(SUM(total_amount), 0) AS total_amount,

    ROUND(
        COALESCE(AVG(total_amount), 0),
        2
    ) AS avg_order_amount

FROM core_customers

LEFT JOIN core_orders
    ON core_orders.customer_id = core_customers.customer_id

GROUP BY
    core_customers.customer_id,
    core_customers.name,
    core_customers.city;


    

~~~
