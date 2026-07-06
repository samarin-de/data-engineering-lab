from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def say_hello():
    print("Мой первый DAG работает! Всё настроено правильно!")

default_args = {
    'owner': 'me',
    'start_date': datetime(2026, 6, 27),
    'retries': 1
}

dag = DAG(
    'first_dag',
    default_args=default_args,
    description='Мой первый DAG',
    schedule_interval='@once',
    catchup=False
)

hello_task = PythonOperator(
    task_id='say_hello',
    python_callable=say_hello,
    dag=dag
)

hello_task