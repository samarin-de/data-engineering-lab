from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'me',
    'start_date': datetime(2026, 7, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'numbers_gen_dag',
    default_args=default_args,
    description='Генератор чисел',
    schedule_interval='@once',
    catchup=False
)

create_table=PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE TABLE IF NOT EXISTS numbers (
            id SERIAL PRIMARY KEY,
            rows_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    dag=dag
)

clear_table=PostgresOperator(
    task_id='clear_table',
    postgres_conn_id='postgres_default',
    sql="TRUNCATE TABLE numbers",
    dag=dag
)

insert_data=PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='postgres_default',
    sql="""
        INSERT INTO numbers (rows_count)
        SELECT generate_series(1, 100);
        """,
    dag=dag
)

def read_numbers():
    hook = PostgresHook(postgres_conn_id='postgres_default')   # Подключаемся к базе
    sql = "SELECT SUM(rows_count) FROM numbers;" # Выполняем запрос на сумму
    result = hook.get_records(sql) # вернёт список кортежей, например [(5050,)]
    total_sum = result[0][0] if result else 0 # Забираем число
    logger.info(f"Сумма чисел в таблице numbers: {total_sum}") # Пишем в лог
    return total_sum # Можно ещё вернуть, чтобы передать в XCom (опционально)

read_table=PythonOperator(
    task_id='read_table',
    python_callable=read_numbers,
    dag=dag
)

create_table >> clear_table >> insert_data >> read_table