~~~
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

import sys

sys.path.append("/opt/airflow/dags/ver1")

# тут прописываем функции которые импортируем сюда для запуска:


from scratch_1 import open_file, create_tables, load_staging, upsert_raw





with DAG(
    dag_id="Project_1",
    start_date=datetime(2026, 10, 6),
    schedule=None,
    catchup=False,
) as dag:

    open_file_task = PythonOperator(
        task_id="open_file",
        python_callable=open_file,
    )

    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    load_staging_task = PythonOperator(
        task_id="load_staging",
        python_callable=load_staging,
    )

    upsert_raw_task = PythonOperator(
        task_id="upsert_raw",
        python_callable=upsert_raw,
    )


    open_file_task >> create_tables_task >> load_staging_task>>upsert_raw_task

~~~
