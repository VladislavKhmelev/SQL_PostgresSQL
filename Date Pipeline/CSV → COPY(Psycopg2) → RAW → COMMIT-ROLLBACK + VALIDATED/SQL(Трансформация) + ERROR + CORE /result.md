~~~

                    Список отношений
 Схема  |         Имя         |      Тип      | Владелец
--------+---------------------+---------------+----------
 public | customers           | таблица       | postgres
 public | error_customers     | таблица       | postgres
 public | error_orders        | таблица       | postgres
 public | inner_table         | представление | postgres
 public | orders              | таблица       | postgres
 public | raw_customers       | таблица       | postgres
 public | raw_orders          | таблица       | postgres
 public | validated_customers | таблица       | postgres
 public | validated_orders    | таблица       | postgres
(9 строк)




RAW:


hh=# select * from raw_orders;
 order_id | customer_id |     order_date      | product_category | quantity | unit_price |  status
----------+-------------+---------------------+------------------+----------+------------+-----------
 1001     | 1           | 2025-09-01 10:15:00 | electronics      | 2        | 1500.00    | completed
 1002     | 2           | 01.09.2025 11:30:00 | clothing         | 1        | 2500.00    | completed
 1003     | 3           | 2025-09-03 09:20:00 | books            | 3        | 500.00     | pending
 1004     | 99          | 2025-09-04 15:45:00 | electronics      | 1        | 1200.00    | completed
 1005     | 4           | 2025-09-05 18:10:00 | clothing         | -2       | 1000.00    | completed
 1006     | 5           | 2025-09-06 12:00:00 | books            | 2        | -500.00    | completed
 1007     | 6           | 2025-09-07 14:25:00 | electronics      | 1        | 3500.00    | done
 1008     | 7           | 2025-09-08 16:40:00 | clothing         | 1        | 1800.00    | cancelled
 1009     | 8           | 2025-09-09 19:15:00 | books            | 0        | 700.00     | pending
 1010     | 10          | 2025-09-10 20:30:00 | electronics      | 2        | 999.99     | completed
 1011     | 2           | 2025/09/11 10:00:00 | clothing         | 1        | 1500.00    | completed
 1012     | 3           | 2025-09-12 13:15:00 | books            | 2        | 450.00     | CANCELLED
(12 строк)




validated:

hh=# select * from validated_customers;
 customer_id |      name      |         email          |    city     | registration_date |       error_reason
-------------+----------------+------------------------+-------------+-------------------+---------------------------
           1 | Ivan Petrov    | ivan.petrov@mail.com   | Ufa         | 2025-01-15        |
           2 | Anna Smirnova  | anna.smirnova@mail.com | Kazan       | 2025-02-15        |
           3 | Peter Ivanov   | peter@mail.com         |             | 2025-03-10        |
           4 |                | olga@mail.com          | Moscow      | 2025-04-21        | name is null
           5 | Sergey Volkov  | sergey@mail.com        | Samara      |                   | registration_date is null
           6 | Maria Petrova  | maria@mail.com         | Spb         | 2025-05-12        |
           7 | Alex Brown     | alex@mail.com          | Moscow      |                   | registration_date is null
           8 | John Smith     | john@mail.com          | Kazan       | 2025-06-01        |
           9 | Elena Orlova   | elena@mail.com         | Ufa         | 2025-07-01        |
          10 | Nikolay Ivanov | nikolay@mail.com       | Novosibirsk | 2025-08-15        |
(10 строк)


CORE:

hh=# select * from orders;
 order_id | customer_id | order_date | product_category | quantity | unit_price |  status
----------+-------------+------------+------------------+----------+------------+-----------
     1001 |           1 | 2025-09-01 | electronics      |        2 |    1500.00 | completed
     1003 |           3 | 2025-09-03 | books            |        3 |     500.00 | pending
     1010 |          10 | 2025-09-10 | electronics      |        2 |     999.99 | completed
     1012 |           3 | 2025-09-12 | books            |        2 |     450.00 | cancelled
(4 строки)



error_orders:

hh=# select * from error_orders;
 order_id | customer_id | order_date | product_category | quantity | unit_price |  status   |              error_reason
----------+-------------+------------+------------------+----------+------------+-----------+----------------------------------------
     1002 |           2 |            | clothing         |        1 |    2500.00 | completed | order_date  is null
     1004 |          99 | 2025-09-04 | electronics      |        1 |    1200.00 | completed | customer_id is OPRHAN
     1005 |           4 | 2025-09-05 | clothing         |       -2 |    1000.00 | completed | quantity <= 0; customer_id is OPRHAN
     1006 |           5 | 2025-09-06 | books            |        2 |    -500.00 | completed | unit_price <= 0; customer_id is OPRHAN
     1007 |           6 | 2025-09-07 | electronics      |        1 |    3500.00 | done      | status error
     1008 |           7 | 2025-09-08 | clothing         |        1 |    1800.00 | cancelled | customer_id is OPRHAN
     1009 |           8 | 2025-09-09 | books            |        0 |     700.00 | pending   | quantity <= 0
     1011 |           2 |            | clothing         |        1 |    1500.00 | completed | order_date  is null
(8 строк)



ПРОВЕРКА БЕЗ СИРОТ, НО ЕСТЬ КЛИЕНТЫ БЕЗ ЗАКАЗОВ.

hh=# select * from inner_table;
 customers_customer_id |      name      |    city     | order_id | orders_customer_id | order_date | product_category | quantity | unit_price |  status
-----------------------+----------------+-------------+----------+--------------------+------------+------------------+----------+------------+-----------
                     1 | Ivan Petrov    | Ufa         |     1001 |                  1 | 2025-09-01 | electronics      |        2 |    1500.00 | completed
                     3 | Peter Ivanov   |             |     1003 |                  3 | 2025-09-03 | books            |        3 |     500.00 | pending
                    10 | Nikolay Ivanov | Novosibirsk |     1010 |                 10 | 2025-09-10 | electronics      |        2 |     999.99 | completed
                     3 | Peter Ivanov   |             |     1012 |                  3 | 2025-09-12 | books            |        2 |     450.00 | cancelled
                     2 | Anna Smirnova  | Kazan       |          |                    |            |                  |          |            |
                     8 | John Smith     | Kazan       |          |                    |            |                  |          |            |
                     6 | Maria Petrova  | Spb         |          |                    |            |                  |          |            |
                     9 | Elena Orlova   | Ufa         |          |                    |            |                  |          |            |
(8 строк)












~~~
