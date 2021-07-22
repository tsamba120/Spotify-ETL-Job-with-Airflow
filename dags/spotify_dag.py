from datetime import timedelta
from datetime import datetime as dt
import datetime
from airflow import DAG # DAG object
from airflow.operators.python import PythonOperator # Python operator
from airflow.utils.dates import days_ago
from etl_spotify import spotify_etl_func, test_func

# CHANGE AIRFLOW DIRECTORIES ALWAYS: 
#   https://stackoverflow.com/questions/52698704/how-to-change-the-dag-bag-folder-for-airflow-web-ui
# check $airflow config list
# CHANGE TO PROJECT DIRECTORY

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
    'spotify_dag',
    default_args=default_args,
    description='Spotify ETL Job',
    schedule_interval = '@daily'
)

execute_spotify_job = PythonOperator(
    task_id='spotify_etl_pgsql',
    python_callable=spotify_etl_func,
    dag=dag
)

execute_spotify_job