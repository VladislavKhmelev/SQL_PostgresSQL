~~~

-- ============================================================
-- 2. RAW
-- ============================================================

CREATE TABLE raw_suppliers (
    supplier_id TEXT,
    supplier_name TEXT,
    country TEXT,
    email TEXT,
    active TEXT,
    contract_start TEXT
);


-- ============================================================
-- 3. VALIDATED
-- ============================================================

CREATE TABLE validated_suppliers (
    supplier_id INT,
    supplier_name TEXT,
    country TEXT,
    email TEXT,
    active BOOLEAN,
    contract_start DATE,
    error_reason TEXT
);


-- ============================================================
-- 4. ERROR
-- ============================================================

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


-- ============================================================
-- 5. CORE
-- ============================================================

CREATE TABLE suppliers (
    supplier_id INT PRIMARY KEY,
    supplier_name TEXT,
    country TEXT,
    email TEXT,
    active BOOLEAN,
    contract_start DATE
);


-- ============================================================
-- 6. TRANSACTION
-- ============================================================







BEGIN;



COPY raw_suppliers FROM 'C:/data/suppliers.csv' WITH (FORMAT CSV, HEADER);




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

            -- duplicate supplier_id
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

ON CONFLICT (supplier_id)

DO UPDATE SET
    supplier_name = EXCLUDED.supplier_name,
    country = EXCLUDED.country,
    email = EXCLUDED.email,
    active = EXCLUDED.active,
    contract_start = EXCLUDED.contract_start;




COMMIT;




# BEGIN
#   ↓
# COPY
#   ↓
# ❌ ошибка: файла нет
#   ↓
# транзакция становится ABORTED
#   ↓
# INSERT validated → игнорируется
#   ↓
# INSERT error → игнорируется
#   ↓
# INSERT suppliers → игнорируется
#   ↓
# COMMIT
#   ↓
# ROLLBACK


Да. В psql приглашение:
--------------
hh=!#
---------------
показывает, что текущая транзакция находится в состоянии ошибки (aborted).

Условно:

# hh=#   → обычный режим транзакции нет
# hh=*#  → внутри транзакции
# hh=!#  → внутри транзакции, но она aborted (ошибка)


-----------------
hh=# BEGIN;
BEGIN
hh=*#
hh=*#
------------------
Разберём:

hh=# — обычный режим, транзакции нет.
BEGIN; — ты начал транзакцию.
BEGIN — PostgreSQL подтвердил запуск транзакции.
hh=*# — теперь * показывает: ты находишься внутри активной транзакции.


-------------
hh=!#
hh=!#
hh=!# INSERT INTO validated_suppliers (
hh(!#     supplier_id,
hh(!#     supplier_name,
------------------------------

Что означает hh=!#

! означает, что текущая транзакция уже была прервана из-за ошибки.
! = транзакция сломана
( = команда ещё не закончена


Сейчас правильное действие
Сначала не продолжай INSERT. Выйди из незавершённой команды:

\r


После этого выполни:

ROLLBACK;

Должно стать:

hh=#





-----------------------
hh=!#
hh=!# COMMIT;
ROLLBACK
hh=#
-------------------------
# ┌──────────────────────────────────────────────────────────────────────┐
# │ 1. hh=!#                                                            │
# │    Транзакция уже была прервана из-за предыдущей ошибки.             │
# │                                                                      │
# │ 2. COMMIT;                                                          │
# │    Ты попросил PostgreSQL завершить транзакцию и сохранить изменения.│
# │                                                                      │
# │ 3. ROLLBACK                                                         │
# │    PostgreSQL ответил, что сохранить её нельзя, поэтому изменения   │
# │    были отменены.                                                    │
# │                                                                      │
# │ 4. hh=#                                                             │
# │    Транзакция закончилась, PostgreSQL вернулся в обычное состояние.  │
# └──────────────────────────────────────────────────────────────────────┘

-----------------------
hh=!#
hh=!# COMMIT;
ROLLBACK
hh=# \r
Буфер запроса сброшен (очищен).
hh=#
--------------------------------
\r → очистила буфер ввода psql, то есть незавершённый SQL-текст, который ты набирал.


Нет, \r делать не обязательно.

Если твоя цель — именно отменить транзакцию после ошибки, достаточно:

ROLLBACK;

Он выполняет главную функцию — откатывает транзакцию и возвращает PostgreSQL в обычное состояние:

# ┌─────────────────────────────────────────────┐
# │ hh=!#                                       │
# │                                             │
# │ ROLLBACK;                                   │
# │                                             │
# │ hh=#                                        │
# │                                             │
# │ → транзакция отменена                       │
# └─────────────────────────────────────────────┘

А \r — это совсем другое. Он нужен только если у тебя остался незаконченный текст SQL-команды в буфере.

Например:

# ┌─────────────────────────────────────────────┐
# │ hh=!# INSERT INTO validated_suppliers (     │
# │ hh(!#     supplier_id,                      │
# │ hh(!#     supplier_name,                    │
# │                                             │
# │ → SQL-команда ещё не закончена              │
# └─────────────────────────────────────────────┘

Тогда можно нажать Ctrl+C или выполнить:

\r

чтобы просто очистить набранный SQL.

Запомни
# ┌─────────────────────────────────────────────┐
# │ ROLLBACK; → отменяет транзакцию             │
# │                                             │
# │ \r         → очищает незаконченный SQL      │
# │                                             │
# │ Это две разные функции.                     │
# │                                             │
# │ Для отмены транзакции достаточно ROLLBACK.  │
# └─────────────────────────────────────────────┘


# ┌──────────────────────────────────────────────────────────────────────────┐
# │ «Приглашение» — это перевод английского термина prompt.                  │
# │                                                                          │
# │ В psql это символы, которые показываются слева и приглашают тебя         │
# │ ввести следующую команду.                                                │
# │                                                                          │
# │ Например:                                                                │
# │                                                                          │
# │ hh=#     → psql приглашает ввести команду                                │
# │ hh=*#    → приглашает продолжить работу внутри транзакции                │
# │ hh=!#    → приглашает ввести команду, но транзакция уже прервана          │
# │ hh(!#    → приглашает продолжить незавершённую SQL-команду                │
# │                                                                          │
# │ Это именно строка-индикатор, которую psql показывает тебе перед вводом.  │
# └──────────────────────────────────────────────────────────────────────────┘




# ┌──────────────────────────────────────────────────────┐
# │                       BEGIN                          │
# │                         ↓                            │
# │                 COPY FROM → RAW                     │
# │                         ↓                            │
# │                    VALIDATION                        │
# │                         ↓                            │
# │                  ERROR / CORE                        │
# │                         ↓                            │
# │                   CHECKS                              │
# │                         ↓                            │
# │                      COMMIT                           │
# └──────────────────────────────────────────────────────┘

~~~
