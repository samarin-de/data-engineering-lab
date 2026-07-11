from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'me',
    'start_date': datetime(2026, 7, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'crypto_etl_dag',
    default_args=default_args,
    description='Запуск crypto-etl-project через Docker Compose',
    schedule_interval='*/10 * * * *',
    catchup=False,
)

run_crypto_etl = BashOperator(
    task_id='run_crypto_etl',
    bash_command='docker run --rm --network host -e DB_HOST=host.docker.internal crypto-etl',
    dag=dag,
)

run_crypto_etl
