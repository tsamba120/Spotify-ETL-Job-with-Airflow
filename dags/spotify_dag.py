from datetime import timedelta
from datetime import datetime as dt
from airflow import DAG # DAG object
from airflow.operators.python import PythonOperator # Python operator
from airflow.utils.dates import days_ago
from Python.etl_spotify import spotify_etl_func


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['terencerustia@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retry_delay': timedelta(minutes=5),
    'retries': 1
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='Spotify ETL Job',
    schedule_interval = timedelta(days=1)
)

execute_spotify_job = PythonOperator(
    task_id='spotify_etl_pgsql',
    python_callable='spotify_etl_func',
    dag=dag
)