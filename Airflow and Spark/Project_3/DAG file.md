~~~
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

import sys

# для того, чтобы Python внутри Airflow мог найти наш файл
sys.path.append("/opt/airflow/dags/ver3")

# тут прописываем название функции (например: def read_database(): ) которые импортируем сюда для запуска:


from scratch import read_database_and_create_table, save_raw_hdfs,validated_table_and_save_hdfs, error_table_hdfs, core_today_and_iceberg_table,create_view_data_mart





with DAG(
    dag_id="Project_3",
    start_date=datetime(2026, 10, 6),
    schedule=None,
    catchup=False,
) as dag:

    read_database_and_create_table_task = PythonOperator(
        task_id="read_database_and_create_table",
        python_callable=read_database_and_create_table,
    )

    save_raw_hdfs_task = PythonOperator(
        task_id="save_raw_hdfs",
        python_callable=save_raw_hdfs,
    )

    validated_table_and_save_hdfs_task = PythonOperator(
        task_id="validated_table_and_save_hdfs",
        python_callable=validated_table_and_save_hdfs,
    )

    error_table_hdfs_task = PythonOperator(
        task_id="error_table_hdfs",
        python_callable=error_table_hdfs,
    )

    core_today_and_iceberg_table_task = PythonOperator(
        task_id="core_today_and_iceberg_table",
        python_callable=core_today_and_iceberg_table,
    )

    create_view_data_mart_task = PythonOperator(
        task_id="create_view_data_mart",
        python_callable=create_view_data_mart,
    )

    # -----последовательность:


    read_database_and_create_table_task>>save_raw_hdfs_task>>validated_table_and_save_hdfs_task>>error_table_hdfs_task>>core_today_and_iceberg_table_task>> create_view_data_mart_task



~~~
