~~~
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

import sys

# для того, чтобы Python внутри Airflow мог найти наш файл
sys.path.append("/opt/airflow/dags/ver2")

# тут прописываем название функции (например: def read_database(): ) которые импортируем сюда для запуска:


from scratch_2 import read_database, create_tables, write_database, upsert_sqlalhemy





with DAG(
    dag_id="Project_2",
    start_date=datetime(2026, 10, 6),
    schedule=None,
    catchup=False,
) as dag:

    read_database_task = PythonOperator(
        task_id="read_database",
        python_callable=read_database,
    )

    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    write_database_task = PythonOperator(
        task_id="write_database",
        python_callable=write_database,
    )

    upsert_sqlalhemy_task = PythonOperator(
        task_id="upsert_sqlalhemy",
        python_callable=upsert_sqlalhemy,
    )

    read_database_task>>create_tables_task>>write_database_task >> upsert_sqlalhemy_task




~~~
