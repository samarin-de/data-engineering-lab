from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def start_process():
    print("Начинаем обработку данных...")
    return "Данные получены"

def finish_process():
    print("Обработка завершена! Данные сохранены.")

default_args = {
    'owner': 'me',
    'start_date': datetime(2026, 6, 27),
    'retries': 1
}

dag = DAG(
    'second_dag',
    default_args=default_args,
    description='DAG с двумя шагами',
    schedule_interval='@once',
    catchup=False
)

task1 = PythonOperator(
    task_id='start_task',
    python_callable=start_process,
    dag=dag
)

task2 = PythonOperator(
    task_id='finish_task',
    python_callable=finish_process,
    dag=dag
)

task1 >> task2