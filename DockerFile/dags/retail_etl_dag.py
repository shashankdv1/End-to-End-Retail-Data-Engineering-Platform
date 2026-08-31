from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="retail_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract_task = BashOperator(
        task_id="extract",
        bash_command="python /opt/airflow/src/scripts/extract.py",
    )

    transform_task = BashOperator(
        task_id="transform",
        bash_command="python /opt/airflow/src/scripts/transform.py",
    )

    load_task = BashOperator(
        task_id="load",
        bash_command="python /opt/airflow/src/scripts/load.py",
    )

    extract_task >> transform_task >> load_task