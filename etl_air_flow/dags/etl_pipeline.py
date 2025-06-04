from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/scripts')
from transform import run_etl

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id='sales_data_etl',
        default_args=default_args,
        start_date=datetime(2024, 1, 1),
        schedule_interval='@daily',
        catchup=False
) as dag:
    run_etl_task = PythonOperator(
        task_id='run_etl',
        python_callable=run_etl
    )
