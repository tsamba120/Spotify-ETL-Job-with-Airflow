from datetime import timedelta
from datetime import datetime as dt
from airflow import DAG # DAG object
from airflow.operators.python import PythonOperator # Python operator
from airflow.utils.dates import days_ago
from etl_spotify import extract_stage_data, transform_validate_load_data, spotify_etl_func

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

TASK_1 = PythonOperator(
    task_id='extract_stage_to_s3',
    python_callable=extract_stage_data,
    dag=dag
)

TASK_2 = PythonOperator(
    task_id='transform_validate_load',
    python_callable=transform_validate_load_data,
    dag=dag
)

execute_spotify_job = PythonOperator(
    task_id='spotify_etl_pgsql',
    python_callable=spotify_etl_func,
    dag=dag
)

execute_spotify_job
# TASK_1 >> TASK_2