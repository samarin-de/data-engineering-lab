from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator  # <-- добавил импорт
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'me',
    'start_date': datetime(2026, 6, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Функция вставки данных (вынесена на уровень DAG)
def insert_many():
    hook = PostgresHook(postgres_conn_id='pg_local')  # или 'postgres_default'
    rows = [
        (1, 'Alice', 30, 'alice@example.com'),
        (2, 'Bob', 25, 'bob@example.com'),
        (3, 'Charlie', None, 'charlie@example.com'),
    ]
    hook.insert_rows(
        table='staging_users',
        rows=rows,
        target_fields=['id', 'name', 'age', 'email']  # <-- исправлено
    )

with DAG(
    dag_id='users_staging_load',
    start_date=datetime(2026, 6, 30),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    create_table = PostgresOperator(
        task_id='create_staging_table',
        postgres_conn_id='pg_local',  # или 'postgres_default'
        sql="""
            CREATE TABLE IF NOT EXISTS staging_users (
                id INTEGER,
                name VARCHAR(100),
                age INTEGER,
                email VARCHAR(150),
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    truncate_table = PostgresOperator(
        task_id='truncate_staging',
        postgres_conn_id='pg_local',  # или 'postgres_default'
        sql="TRUNCATE TABLE staging_users;",
    )

    insert_task = PythonOperator(
        task_id='insert_users_staging',
        python_callable=insert_many,
        dag=dag  # <-- добавил привязку к DAG
    )

    create_table >> truncate_table >> insert_task