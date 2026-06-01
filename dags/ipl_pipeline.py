from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import sys

# Default arguments for the DAG
default_args = {
    'owner': 'darshanpc',
    'retries': 2,  
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='ipl_data_pipeline',
    default_args=default_args,
    description='IPL Analytics End-to-End Pipeline',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ipl', 'analytics', 'de']
) as dag:
    # Task 1 - Ingest from Kaggle to S3 Bronze
    ingest_bronze = BashOperator(
        task_id='kaggle_to_s3_bronze',
        bash_command='python3 /opt/airflow/dags/../ingestion/kaggle_to_s3.py',
    )

    # Task 2 - PySpark Bronze to Silver
    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver_pyspark',
        bash_command='docker exec ipl-analytics-pipeline-pyspark-1 /bin/bash -c "cd /opt/spark_jobs && python3 bronze_to_silver.py"',
    )

    # Task 3 - dbt Gold models
    dbt_run = BashOperator(
        task_id='dbt_gold_models',
        bash_command='cd /opt/airflow/dags/../dbt/ipl_analytics && dbt run',
    )

    # Define task dependencies
    ingest_bronze >> bronze_to_silver >> dbt_run