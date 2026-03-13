from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "vasudev",
    "start_date": datetime(2024,1,1),
    "retries": 1
}

with DAG(
    dag_id="hn_data_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="HackerNews Bronze Silver Gold Pipeline"
) as dag:

    bronze_job = BashOperator(
        task_id="bronze_stream",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        /opt/spark/spark_jobs/bronze_stream.py
        """
    )

    silver_job = BashOperator(
        task_id="silver_transform",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        /opt/spark/spark_jobs/silver_transform.py
        """
    )

    gold_job = BashOperator(
        task_id="gold_analytics",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        /opt/spark/spark_jobs/gold_batch_analytics.py
        """
    )

    bronze_job >> silver_job >> gold_job
