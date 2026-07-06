from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def start_process():
    print("Начинаем загрузку данных...")

def start_process_X000s(ti):
    rows = 99999
    print(f"Начинаем загрузку {rows} строк данных...")
    ti.xcom_push(key='rows_loaded', value=rows)
    return f"Данные {rows} получены"

def finish_process(ti):
    rows = ti.xcom_pull(key='rows_loaded', task_ids='start_process_X000s')
    print(f"Обработка завершена! Загружено {rows} строк данных.")
    
def download_X000s(ti):
    rows = ti.xcom_pull(key='rows_loaded', task_ids='start_process_X000s')
    print(f"Загружено {rows} строк данных")   

default_args = {
    'owner': 'me',
    'start_date': datetime(2026, 6, 27),
    'retries': 1
}

dag = DAG(
    'third_dag',
    default_args=default_args,
    description='DAG с четыремя шагами и XCom',
    schedule_interval='@once',
    catchup=False
)

task1 = PythonOperator(
    task_id='start_process',
    python_callable=start_process,
    dag=dag
)

task2 = PythonOperator(
    task_id='start_process_X000s',
    python_callable=start_process_X000s,
    dag=dag
)

task3 = PythonOperator(
    task_id='finish_process',
    python_callable=finish_process,
    dag=dag
)

task4 = PythonOperator(
    task_id='download_X000s',
    python_callable=download_X000s,
    dag=dag
)

task1 >> task2 >> task3 >> task4