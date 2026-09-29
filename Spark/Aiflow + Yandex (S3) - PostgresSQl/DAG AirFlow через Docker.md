~~~
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator




with DAG(
    dag_id="Google_Cloud_S3_SPARK",
    start_date=datetime(2026, 9, 28),
    schedule=None,
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id="run_etl",
        bash_command='python "/opt/airflow/dags/Project2/Google Cloud (S3)+SPARK.py"'
    )

~~~
