from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from temperature_data_transformer import transform_data

with DAG(
    dag_id='temperature_transform',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    transform = PythonOperator(
        task_id='transform_temperature_data',
        python_callable=transform_data
    )

    transform