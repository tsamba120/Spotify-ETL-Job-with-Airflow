from datetime import timedelta
from datetime import datetime as dt
import datetime
from airflow import DAG # DAG object
from airflow.operators.python import PythonOperator # Python operator
from airflow.utils.dates import days_ago
from email_script import send_weekly_email

# Considering using the email operator to send

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt(2021, 7, 18),
    'email': ['terencerustia@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'weekly_email_dag',
    default_args=default_args,
    description='Weekly Spotify Metrics Email Job',
    schedule_interval='@weekly'
)

execute_email_job = PythonOperator(
    task_id='send_spotify_metrics_email',
    python_callable=send_weekly_email,
    dag=dag
)

execute_email_job