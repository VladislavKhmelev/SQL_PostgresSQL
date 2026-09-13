~~~
CREATE TABLE raw_suppliers (
    supplier_id TEXT,
    supplier_name TEXT,
    country TEXT,
    email TEXT,
    active TEXT,
    contract_start TEXT
);



--загружаем CSV через серверный PostgreSQL COPY FROM.


TRUNCATE TABLE raw_suppliers;


-- COPY файл находится на сервере и путь не такой должен быть, я прописал локальный для моего файла
COPY raw_suppliers FROM 'C:\Users\Vladislav X\Desktop\proj\ver8\data\suppliers.csv' WITH (FORMAT CSV, HEADER );



# ┌────────────────────────────────────────────────────────────┐
# │                    КТО ЧИТАЕТ ФАЙЛ?                       │
# │                                                            │
# │  suppliers.csv                                             │
# │       ↓                                                    │
# │  PostgreSQL SERVER                                         │
# │       ↓                                                    │
# │  COPY FROM                                                 │
# │       ↓                                                    │
# │  raw_suppliers                                             │
# └────────────────────────────────────────────────────────────┘

SELECT COUNT(*)
FROM raw_suppliers;


--Создаём таблицу для валидации.
-- нужные типы указываем, это нормально, при INSERT надо делать проверку на безопасность. Нужно только правильно написать INSERT, чтобы некорректные значения не уронили весь процесс.

CREATE TABLE validated_suppliers (
    supplier_id INT,
    supplier_name TEXT,
    country TEXT,
    email TEXT,
    active BOOLEAN,
    contract_start DATE,
    error_reason TEXT
);






--NULLIF помогает только если строка пустая, а если в числовой колонке стоит - 'abc' или '501.5' и мы хотим сделать ::int то будет ошибка
-------------------------------
# supplier_id
# ───────────
# '501'       → 501   
# '   '       → NULL
# 'abc'       → ❌ ошибка преобразования
# '501.5'     → ❌ ошибка преобразования
-------------------------------------------
То порядок проверки должен быть таким:

1. Сначала убираем пробелы .......TRIM(supplier_id)
2. Затем проверяем пустоту.......NULLIF(TRIM(supplier_id), '')
3. Если значение не NULL, проверяем формат........TRIM(supplier_id) ~ '^\d+$'
--Это проверяет: состоит ли значение только из цифр.
4. И только после этого делаем ::int

















INSERT INTO validated_suppliers (
    supplier_id,
    supplier_name,
    country,
    email,
    active,
    contract_start,
    error_reason
)
SELECT

    -- supplier_id
    CASE
        WHEN NULLIF(TRIM(supplier_id), '') ~ '^\d+$'
             AND NULLIF(TRIM(supplier_id), '')::int > 0
        THEN NULLIF(TRIM(supplier_id), '')::int
        ELSE NULL
    END,

    -- supplier_name
    NULLIF(TRIM(supplier_name), ''),

    -- country
    NULLIF(TRIM(country), ''),

    -- email
    NULLIF(TRIM(email), ''),

    -- active
    CASE
        WHEN LOWER(TRIM(active)) IN ('true', 'false')
        THEN TRIM(active)::boolean
        ELSE NULL
    END,

    -- contract_start
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
    END,

    -- error_reason
    NULLIF(
        CONCAT_WS(
            '; ',

            -- supplier_id
            CASE
                WHEN NULLIF(TRIM(supplier_id), '') IS NULL
                    THEN 'supplier_id is NULL'

                WHEN NULLIF(TRIM(supplier_id), '') !~ '^\d+$'
                    THEN 'supplier_id is not integer'

                WHEN NULLIF(TRIM(supplier_id), '')::int <= 0
                    THEN 'supplier_id <= 0'
            END,

            -- DUPLICATE supplier_id
            CASE
                WHEN COUNT(*) OVER (
                    PARTITION BY supplier_id
                ) > 1
                    THEN 'supplier_id DUPLICATE'
            END,

            -- supplier_name
            CASE
                WHEN NULLIF(TRIM(supplier_name), '') IS NULL
                    THEN 'supplier_name is NULL'
            END,

            -- country
            CASE
                WHEN NULLIF(TRIM(country), '') IS NULL
                    THEN 'country is NULL'
            END,

            -- email
            CASE
                WHEN NULLIF(TRIM(email), '') IS NULL
                     OR TRIM(email) NOT LIKE '%@%'
                    THEN 'email is NULL or invalid'
            END,

            -- active
            CASE
                WHEN NULLIF(TRIM(active), '') IS NULL
                     OR LOWER(TRIM(active))
                        NOT IN ('true', 'false')
                    THEN 'active is NULL or invalid'
            END,

            -- contract_start
            CASE
                WHEN NULLIF(TRIM(contract_start), '') IS NULL
                    THEN 'contract_start is NULL'

                WHEN TRIM(contract_start) !~ '^\d{4}-\d{2}-\d{2}$'
                 AND TRIM(contract_start) !~ '^\d{2}\.\d{2}\.\d{4}$'
                 AND TRIM(contract_start) !~ '^\d{4}/\d{2}/\d{2}$'
                    THEN 'contract_start invalid format'
            END

        ),
        ''
    ) AS error_reason

FROM raw_suppliers;








и вот тут возьмем кусок кода:
---------------------------------
SELECT
    CASE
        WHEN NULLIF(TRIM(supplier_id), '') ~ '^\d+$'
             AND NULLIF(TRIM(supplier_id), '')::int > 0
        THEN NULLIF(TRIM(supplier_id), '')::int
        ELSE NULL
    END

Идёт так:

1.TRIM(supplier_id) — убираем пробелы.
2.NULLIF(..., '') — пустую строку превращаем в NULL.
3.  ~ '^\d+$' — проверяем, что значение состоит только из цифр.
4.  ::int > 0 — преобразуем в int и проверяем, что число положительное.

Если всё хорошо → возвращаем supplier_id как int.
Если что-то не подходит → ELSE NULL.

----------------------------------------------------------
# '501'       → TRIM → '501' → число → > 0 → 501
# ' 501 '     → TRIM → '501' → число → > 0 → 501
# '   '       → TRIM → '' → NULL → ELSE → NULL
# NULL        → NULL → ELSE → NULL
# 'abc'       → не число → ELSE → NULL
# '501.5'     → не подходит под ^\d+$ → ELSE → NULL
# '-5'        → не подходит под ^\d+$ → ELSE → NULL
# '0'         → число → 0 > 0 = FALSE → ELSE → NULL
---------------------------------------------------------------------

--это уже хороший вариант защиты + проверки.




--Создаём ERROR и CORE


CREATE TABLE error_suppliers (
    error_id SERIAL PRIMARY KEY,
    supplier_id INT,
    supplier_name TEXT,
    country TEXT,
    email TEXT,
    active BOOLEAN,
    contract_start DATE,
    error_reason TEXT
);

CREATE TABLE suppliers (
    supplier_id INT PRIMARY KEY,
    supplier_name TEXT,
    country TEXT,
    email TEXT,
    active BOOLEAN,
    contract_start DATE
);


INSERT INTO error_suppliers (
    supplier_id,
    supplier_name,
    country,
    email,
    active,
    contract_start,
    error_reason
)
SELECT
    supplier_id,
    supplier_name,
    country,
    email,
    active,
    contract_start,
    error_reason
FROM validated_suppliers
WHERE error_reason IS NOT NULL;



INSERT INTO suppliers (
    supplier_id,
    supplier_name,
    country,
    email,
    active,
    contract_start
)
SELECT
    supplier_id,
    supplier_name,
    country,
    email,
    active,
    contract_start
FROM validated_suppliers

WHERE error_reason IS NULL

on conflict(supplier_id)

do UPDAT set
supplier_name = EXCLUDED.supplier_name,
country = EXCLUDED.country,
email = EXCLUDED.email,
active = EXCLUDED.active,
contract_start = EXCLUDED.contract_start;





DROP TABLE IF EXISTS suppliers_mart;

CREATE TABLE suppliers_mart AS
SELECT
    country,
    COUNT(supplier_id) AS supplier_count,
    COUNT(
        CASE
            WHEN active = TRUE
            THEN 1
        END
    ) AS active_supplier_count,
    COUNT(
        CASE
            WHEN active = FALSE
            THEN 1
        END
    ) AS inactive_supplier_count
FROM suppliers
GROUP BY country;






-- 11. POST LOAD CHECK ERROR / CORE
-- ============================================================

SELECT COUNT(*)
FROM error_suppliers;

SELECT COUNT(*)
FROM suppliers;


SELECT *
FROM error_suppliers
ORDER BY error_id;


SELECT *
FROM suppliers
ORDER BY supplier_id;



-- ============================================================
-- 12. ПРОВЕРКА ДУБЛИКАТОВ В ERROR
-- ============================================================

SELECT
    supplier_id,
    COUNT(*) AS cnt
FROM error_suppliers
GROUP BY supplier_id
HAVING COUNT(*) > 1
ORDER BY supplier_id;






~~~
