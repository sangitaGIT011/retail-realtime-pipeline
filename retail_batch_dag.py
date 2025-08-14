from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='retail_batch_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 7, 15),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    ingest = BashOperator(task_id='ingest', bash_command='python /path/to/project/src/ingest.py')
    transform = BashOperator(task_id='transform', bash_command='python /path/to/project/src/transform.py')
    load = BashOperator(task_id='load', bash_command='python /path/to/project/src/load.py')

    ingest >> transform >> load
