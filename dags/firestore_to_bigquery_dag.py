from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from etl.pipeline import etl_pipeline_user_register

#default arguments
default_args = {
    'owner' : 'phc',
    'start_date' : days_ago(0),
    'email' :[],
    'email_on_failure' : True,
    'email_on_reply' : True,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

#dag definition
dag = DAG(
    dag_id = '',
    default_args = default_args,
    description = '',
    schedule_interval = timedelta(day=1)
)

user_register_data = PythonOperator(
    task_id = "user_register_data",
    python_callable = etl_pipeline_user_register,
    dag = dag
)

#task pipeline
user_register_data 