~~~

from datetime import datetime
import pandas as pd
import os
from sqlalchemy import create_engine, text


from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator




user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
)

# -------------проверка----------------------
with engine.connect() as connection:
    result = connection.execute(text("SELECT 1"))

    if result.fetchone() == (1,):
        print("Подключен к PostgresSQL (SQLAlchemy)")


# -----------------------------------



#---------------====1.ПРОЧИТАТЬ (open) FILE  + ТЕХНИЧЕСКАЯ ПОДГОТОВКА=====-------------------------

#---------CSV-----------FILE 1------------------------------------------
def open_transforms_csv():


    df = pd.read_csv("/opt/airflow/dags/data/customers.csv")  #---берет из контейнера  docker файл (в data лежит оригинал)

    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.where(df.notna(), None)

    print("Количество строк:", len(df))
    print(df.to_string())



#----------JSON плоский------------FILE 2------------------------------------------
def open_transforms_json():

    
    df = pd.read_json(
        "/opt/airflow/dags/data/orders.json"
    )

    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.where(df.notna(), None)


    print("Количество строк:", len(df))
    print(df.to_string())







#-================================2.LOAD в RAW слой===========================================







#---------CSV-----------FILE 1------------------------------------------
def load_csv():

        df = pd.read_csv(
            "/opt/airflow/dags/data/save/save_customers.csv"
        )

        df.to_sql(
            "raw_customers",
            engine,
            if_exists="replace",    # ----PostgresSQL удаляет существующую таблицу и создаёт её заново на основании структуры и типов DataFrame, то есть ее можно не создавать через create table ...
            index=False
        )





#----------JSON плоский------------FILE 2------------------------------------------


def load_json():

        df = pd.read_json(
            "/opt/airflow/dags/data/save/save_orders.json"
        )

        df.to_sql(
            "raw_orders",
            engine,
            if_exists="replace",   # ----PostgresSQL удаляет существующую таблицу и создаёт её заново на основании структуры и типов DataFrame, то есть ее можно не создавать через create table ...
            index=False
        )





#-================================2. из RAW в Validated слой===========================================

#----------------------FILE 1 из RAW в Validated слой------------------------------------------

def raw_validated_csv():

    with engine.begin() as connection:


        
        connection.execute(
            text("DROP TABLE IF EXISTS validated_customers")
        )


        connection.execute(
            text("""CREATE table validated_customers 
            (
                   id int,
                   name text,
                   email text,
                   age int,
                   status text,
                   error_reason text
                                    );
                            """)
        )


        connection.execute(
                text("""
               INSERT INTO validated_customers (
                   id, 
                   name, 
                   email,
                   age, 
                   status,
                   error_reason
                )
                                
                SELECT
                    id::int,
                    name::text,
                    email::text,
                    age::int,
                    status::text,
                
                    NULLIF(
                        CONCAT_WS(
                            '; ',
                
                            CASE
                                WHEN id IS NULL
                                    THEN 'id is NULL'
                
                                WHEN id <= 0
                                    THEN 'id <= 0'
                
                                WHEN COUNT(*) OVER (
                                    PARTITION BY id
                                ) > 1
                                    THEN 'id is duplicate'
                            END,
                
                            CASE
                                WHEN name IS NULL
                                    THEN 'name is NULL'
                            END,
                
                            CASE
                                WHEN email IS NULL
                                    THEN 'email is NULL'
                
                                WHEN email NOT LIKE '%@%'
                                    THEN 'email error format'
                            END,
                
                            CASE
                                WHEN age IS NULL
                                    THEN 'age is NULL'
                
                                WHEN age <= 0
                                    THEN 'age <= 0'
                            END,
                
                         
                
                              CASE
                                WHEN status IS NULL
                                    THEN 'status is NULL'
                
                                    WHEN status not in ('active','inactive')
                                    THEN 'status error format'
                            END,
                
                               CASE
                                WHEN COUNT(*) OVER (
                            PARTITION BY id, name, email, age, status) > 1
                                            
                                        THEN 'full duplicate'
                
                                  
                            END
                        ),
                        ''
                    ) AS error_reason
                
                FROM raw_customers;
                
                                """)
                )

        # POST LOAD CHECK
        # ---------------------------------------------------------

        result = connection.execute(
            text("SELECT COUNT(*) FROM validated_customers")
        )

        result_error = connection.execute(
            text("SELECT COUNT(case when error_reason is not null then 1 end)  FROM validated_customers")
        )

        rows = result.scalar()

        rows_error = result_error.scalar()

        print(f"VALIDATED_table: загружено строк", rows, "из них error:", f"{rows_error} / {rows}", "\n")
        # ---------------------------------------------------------






#--------------------------ERROR TABLE FILE 1-----------------------------------------------

def error_table_csv():

    with engine.begin() as connection:

        connection.execute(
            text("""
                CREATE TABLE IF NOT EXISTS etl_loaded_files (
                    file_name text PRIMARY KEY,
                    loaded_at timestamp DEFAULT CURRENT_TIMESTAMP
                );
            """)
        )

        connection.execute(
            text("""
                CREATE TABLE IF NOT EXISTS error_customers (
                    id_key serial PRIMARY KEY,
                    id int,
                    name text,
                    email text,
                    age int,
                    status text,
                    error_reason text
                );
            """)
        )

        loaded = connection.execute(
            text("""
                SELECT 1
                FROM etl_loaded_files
                WHERE file_name = 'customers.csv';
            """)
        ).scalar()

        if loaded:
            print("ERROR_TABLE: customers.csv уже загружен")
            return

        connection.execute(
            text("""
                INSERT INTO error_customers (
                    id,
                    name,
                    email,
                    age,
                    status,
                    error_reason
                )
                SELECT
                    id,
                    name,
                    email,
                    age,
                    status,
                    error_reason
                FROM validated_customers
                WHERE error_reason IS NOT NULL;
            """)
        )

        # POST LOAD CHECK
        result = connection.execute(
            text("SELECT COUNT(*) FROM error_customers")
        )

        rows = result.scalar()

        print("ERROR_TABLE: загружено строк:", rows, '\n')

        connection.execute(
            text("""
                INSERT INTO etl_loaded_files (
                file_name
                )
                VALUES (
                'customers.csv'
                );
            """)
        )


#----------------------FILE 2 из RAW в Validated слой------------------------------------------

def raw_validated_json():

    with engine.begin() as connection:



        connection.execute(
            text("""
            DROP TABLE IF EXISTS validated_orders
                    """)
        )

        connection.execute(
            text("""
           CREATE table validated_orders (
                    order_id int,
                    customer_id int,
                    product text,
                    amount NUMERIC,
                    status text,
                    created_at TIMESTAMP,
                    error_reason text
                    );
                    """)
                                        )


        connection.execute(
            text("""
                    INSERT INTO validated_orders (
                        order_id, 
                        customer_id, 
                        product, 
                        amount, 
                        status,
                        created_at,
                        error_reason 
                        )
                        
                    SELECT
                        order_id::int,
                        customer_id::int,
                        product::text,
                        amount::numeric,
                        status::text,
                        created_at::timestamp,
                
                        NULLIF(
                            CONCAT_WS(
                                '; ',
                        
                                CASE
                            WHEN order_id IS NULL
                                THEN 'order_id is NULL'
                
                            WHEN COUNT(*) OVER (
                                PARTITION BY order_id
                            ) > 1
                                THEN 'order_id is duplicate'
                        END,
                
                        CASE
                            WHEN customer_id IS NULL
                                THEN 'customer_id is NULL'
                
                            WHEN NOT EXISTS (
                                SELECT 1
                                FROM validated_customers
                                WHERE validated_customers.id = raw_orders.customer_id
                            )
                                THEN 'customer_id is orphan'
                        END,
                
                        CASE
                            WHEN product IS NULL
                                THEN 'product is NULL'
                        END,
                
                        CASE
                            WHEN amount IS NULL
                                THEN 'amount is NULL'
                
                            WHEN amount < 0
                                THEN 'amount < 0'
                        END,
                
                        CASE
                            WHEN status IS NULL
                                THEN 'status is NULL'
                
                            WHEN status NOT IN ('completed', 'cancelled')
                                THEN 'status error format'
                        END,
                
                        CASE
                            WHEN COUNT(*) OVER (
                                PARTITION BY
                                    order_id,
                                    customer_id,
                                    product,
                                    amount,
                                    status,
                                    created_at
                            ) > 1
                                THEN 'full duplicate'
                        END,
                
                        CASE
                            WHEN created_at IS NULL
                                THEN 'created_at is NULL'
                
                            WHEN created_at > CURRENT_TIMESTAMP
                                THEN 'created_at is future'
                        END
                    ),
                    ''
                ) AS error_reason
                
                FROM raw_orders;

                                """)
        )

        # POST LOAD CHECK
        # ---------------------------------------------------------

        result = connection.execute(
            text("SELECT COUNT(*) FROM validated_orders")
        )

        result_error = connection.execute(
            text("SELECT COUNT(case when error_reason is not null then 1 end)  FROM validated_orders")
        )

        rows = result.scalar()

        rows_error = result_error.scalar()

        print(f"VALIDATED_table: загружено строк", rows, "из них error:", f"{rows_error} / {rows}", "\n")
        # ---------------------------------------------------------






#--------------------------ERROR TABLE FILE 2-----------------------------------------------

def error_table_json():

    with engine.begin() as connection:

        # --------------------------------------------------
        # Таблица контроля загруженных файлов
        # --------------------------------------------------
        connection.execute(
            text("""
                CREATE TABLE IF NOT EXISTS etl_loaded_files (
                    file_name text PRIMARY KEY,
                    loaded_at timestamp DEFAULT CURRENT_TIMESTAMP
                );
            """)
        )


        connection.execute(
            text("""
                CREATE TABLE IF NOT EXISTS error_orders (
                    order_id_key serial PRIMARY KEY,
                    order_id int,
                    customer_id int,
                    product text,
                    amount NUMERIC,
                    status text,
                    created_at TIMESTAMP,
                    error_reason text
                );
            """)
        )


        loaded = connection.execute(
            text("""
                SELECT 1
                FROM etl_loaded_files
                WHERE file_name = 'orders.json';
            """)
        ).scalar()

        if loaded:
            print("ERROR_TABLE: orders.json уже загружен")
            return



        connection.execute(
            text("""
                INSERT INTO error_orders (
                    order_id,
                    customer_id,
                    product,
                    amount,
                    status,
                    created_at,
                    error_reason
                )
                SELECT
                    order_id,
                    customer_id,
                    product,
                    amount,
                    status,
                    created_at,
                    error_reason
                FROM validated_orders
                WHERE error_reason IS NOT NULL;
            """)
        )



        # --------------------------------------------------
        # POST LOAD CHECK
        # --------------------------------------------------
        result = connection.execute(
            text("""
                SELECT COUNT(*)
                FROM error_orders;
            """)
        )

        rows = result.scalar()

        print(
            "ERROR_TABLE: загружено строк:",
            rows,
            '\n'
        )

        # --------------------------------------------------
        # Фиксируем orders.json как загруженный
        # --------------------------------------------------
        connection.execute(
            text("""
                INSERT INTO etl_loaded_files (
                file_name
                )
                VALUES 
                ('orders.json');
            """)
        )





 # ------------------CORE File1 --------------------------------
 
def core_table_csv():

    with engine.begin() as connection:
        
        connection.execute(
            text("""
                CREATE TABLE IF NOT EXISTS core_customers (
                id int PRIMARY KEY,
                name text NOT NULL,
                email text UNIQUE NOT NULL,
                age int NOT NULL,
                status text not NULL
                                );
                                
                                                 """)
                                    )


        connection.execute(
            text("""
                    INSERT INTO core_customers (
                    id, 
                    name, 
                    email, 
                    age, 
                    status
                    )
                    SELECT
                    id, 
                    name, 
                    email, 
                    age, 
                    status
                    
                    from validated_customers
                    
                    where error_reason is NULL
                    
                    on conflict (id)
                    do update SET
                    name = excluded.name,
                    email = excluded.email,
                    age = excluded.age,
                    status = excluded.status;
                                
                                             """)
                                )

        # POST LOAD CHECK
        # ---------------------------------------------------------

        result_core = connection.execute(
            text("SELECT COUNT(*) FROM core_customers")
        )

        rows2 = result_core.scalar()

        print("CORE_table загружено строк:", rows2)

        # ---------------------------------------------------------




 # ------------------CORE File 2 --------------------------------
 
def core_table_json():

    
    with engine.begin() as connection:
        
        connection.execute(
            text("""
              CREATE TABLE IF NOT EXISTS core_orders (
                order_id int PRIMARY KEY,
                customer_id int NOT NULL,
                product text NOT NULL,
                amount numeric NOT NULL,
                status text NOT NULL,
                created_at TIMESTAMP NOT NULL,
                
                CONSTRAINT fk FOREIGN KEY (customer_id) REFERENCES core_customers (id)
                );

        
                                             """)
                                )

        connection.execute(
            text("""
                INSERT INTO core_orders (
                order_id, 
                customer_id, 
                product, 
                amount,
                status, 
                created_at
                )
                SELECT
                order_id, 
                customer_id, 
                product, 
                amount,
                status, 
                created_at
                
                from validated_orders
                
                where error_reason is NULL
                
                on conflict (order_id)
                do update SET
                customer_id = excluded.customer_id,
                product = excluded.product,
                amount = excluded.amount,
                status = excluded.status,
                created_at = excluded.created_at;
                    
                                         """)
                            )



        # POST LOAD CHECK
        # ---------------------------------------------------------

        result_core = connection.execute(
            text("SELECT COUNT(*) FROM core_orders")
        )

        rows2 = result_core.scalar()

        print("CORE_table загружено строк:", rows2)

        # ---------------------------------------------------------




#-----------------DATA MART---------------------------------


def data_mart_table():

    with engine.begin() as connection:
        connection.execute(
            text("""

        CREATE VIEW dm_customer_orders AS

            SELECT
            customer_id,
            name,
            
            COUNT(core_orders.order_id) AS order_count,
            COALESCE(sum(amount),0) as total_amount,
            round(COALESCE(avg(amount),0),2) as avg_order_amount                           
            
            from core_customers
            LEFT JOIN core_orders
            on core_orders.customer_id =core_customers.id 
            
            GROUP BY
            customer_id,
            name;

                    
                                         """)
                            )

        print("представление VIEW создано")





#------создаем DAG обьект--------------------------

with DAG(
    dag_id="ETL",
    start_date=datetime(2026, 9, 22),
    schedule=None,
    catchup=False,
) as dag:

#--------------------------------------------



    task_1 = PythonOperator(
        task_id="open_transforms_csv",           # ---имя задачи внутри Airflow, поэтому они разные должны быть
        python_callable = open_transforms_csv,    # ---название функции
    )

    task_2 = PythonOperator(
        task_id="open_transforms_json",
        python_callable = open_transforms_json,
    )


    task_3 = PythonOperator(
        task_id="load_csv",
        python_callable = load_csv,
    )

    task_4 = PythonOperator(
        task_id="load_json",
        python_callable = load_json,
    )
    
    task_5 = PythonOperator(
        task_id="raw_validated_csv",
        python_callable=raw_validated_csv,
    )

    task_6 = PythonOperator(
        task_id="raw_validated_json",
        python_callable=raw_validated_json,
    )

    task_7 = PythonOperator(
        task_id="error_table_csv",
        python_callable=error_table_csv,
    )

    task_8 = PythonOperator(
        task_id="error_table_json",
        python_callable=error_table_json,
    )

    task_9 = PythonOperator(
        task_id="core_table_csv",
        python_callable=core_table_csv,
    )

    task_10 = PythonOperator(
        task_id="core_table_json",
        python_callable=core_table_json,
    )

    task_11 = PythonOperator(
        task_id="data_mart_table",
        python_callable=data_mart_table,
    )


#----------------последовательность задач -----------------
 #---CSV:
    task_1>>task_3>>task_5>>task_7>>task_9
#---JSON:
    task_2>>task_4>>task_6>>task_8>>task_10
#---validated_orders зависит от validated_customers (так как проверка для orphan):
task_5 >> task_6
#---core_orders зависит от core_customers (FK подключаем)
task_9 >> task_10>>task_11





~~~
