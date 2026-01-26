from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from json_linearization import process_json
from xml_linearization import process_xml

with DAG(
    dag_id="linearization",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    json_task = PythonOperator(
        task_id="process_json",
        python_callable=process_json
    )

    xml_task = PythonOperator(
        task_id="process_xml",
        python_callable=process_xml
    )

    json_task >> xml_task