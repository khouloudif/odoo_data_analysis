from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.extract_odoo import extract_odoo_data
from include.load_snowflake import load_to_snowflake

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1
}

dag = DAG("odoo_to_snowflake", default_args=default_args, schedule_interval="@daily", catchup=False)

extract_task = PythonOperator(
    task_id="extract_odoo",
    python_callable=extract_odoo_data,
    dag=dag
)

load_task = PythonOperator(
    task_id="load_snowflake",
    python_callable=load_to_snowflake,
    dag=dag
)

extract_task >> load_task
