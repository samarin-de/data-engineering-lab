from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'me',
    'start_date': datetime(2026, 6, 27),
    'retries': 1
}

dag = DAG(
    'postgres_dag',
    default_args=default_args,
    description='Создание таблицы и вставка данных',
    schedule_interval='@once',
    catchup=False
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            rows_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    dag=dag
)

insert_data = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='postgres_default',
    sql="""
        INSERT INTO test_table (rows_count) VALUES (99999);
    """,
    dag=dag
)

create_table >> insert_data