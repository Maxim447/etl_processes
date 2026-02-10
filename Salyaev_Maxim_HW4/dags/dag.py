from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from temperature_data_transformer import transform_data, load_full_history, load_increment

with DAG(
        dag_id="etl_temperature_load",
        start_date=datetime(2026, 2, 1),
        catchup=False,
        tags=["etl", "load"],
        params={
            "load_date": "2026-02-01"
        }
) as dag:
    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load_history = PythonOperator(
        task_id="load_full_history",
        python_callable=load_full_history
    )

    load_incremental = PythonOperator(
        task_id="load_incremental",
        python_callable=load_increment
    )

    transform >> [load_history, load_incremental]
