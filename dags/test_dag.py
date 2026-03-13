from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="test_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task1 = BashOperator(
        task_id="print_date",
        bash_command="date"
    )

    task2 = BashOperator(
        task_id="say_hello",
        bash_command="echo Hello Data Engineering"
    )

    task1 >> task2
