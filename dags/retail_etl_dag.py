from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from scripts.extract import extract
from scripts.transform import transform
from scripts.load import load

with DAG(

    dag_id="retail_etl",

    start_date=datetime(2026,1,1),

    schedule=None,

    catchup=False

) as dag:

    extract_task = PythonOperator(

        task_id="extract",

        python_callable=extract

    )

    transform_task = PythonOperator(

        task_id="transform",

        python_callable=transform

    )

    load_task = PythonOperator(

        task_id="load",

        python_callable=load

    )

    extract_task >> transform_task >> load_task