~~~

from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine



from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

#--------1---------

def extract():
    df = pd.read_csv("/opt/airflow/dags/data/customers.csv")

    print("Количество строк:", len(df))
    print(df.to_string())

    df.to_csv(
        "/opt/airflow/dags/data/save/save_customers.csv",
        index=False
    )

    return len(df)




#--------2---------

def transform():
    df = pd.read_csv(
        "/opt/airflow/dags/data/save/save_customers.csv"
    )

    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.where(df.notna(), None)


    df.to_csv(
        "/opt/airflow/dags/data/save/save_customers.csv",
        index=False
    )




#--------3---------
def load():
    df = pd.read_csv(
        "/opt/airflow/dags/data/save/save_customers.csv"
    )

    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@host.docker.internal:5432/hh"
    )

    df.to_sql(
        "raw_customers",
        engine,
        if_exists="append",
        index=False
    )

    print("Загружено строк:", len(df))











#------создаем DAG---------------------
with DAG(
    dag_id="ETL_DAG",
    start_date=datetime(2026, 9, 22),
    schedule=None,
    catchup=False,
) as dag:


    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_task >> transform_task >> load_task



CREATE TABLE raw_customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    age INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at DATE NOT NULL
);






~~~
