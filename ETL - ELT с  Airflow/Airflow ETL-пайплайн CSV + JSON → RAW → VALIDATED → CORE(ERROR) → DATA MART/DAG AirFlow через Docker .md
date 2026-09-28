~~~
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="Spark_HDFS_PostgresSQL",
    start_date=datetime(2026, 9, 28),
    schedule=None,
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id="open_project_Hadoop_HDFS_Spark",
        bash_command='python "/opt/airflow/dags/Project1/Hadoop HDFS + Spark.py"'
    )

~~~
