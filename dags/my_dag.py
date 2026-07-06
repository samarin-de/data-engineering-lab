from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import requests

logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = '8897904426:AAFrVNCSzsC9PSRa0cwfJqeVEtYBglU-UBE'
TELEGRAM_CHAT_ID = '1355480201'

def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'HTML'
    }
    try:
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code == 200:
            logger.info("Сообщение в Telegram отправлено")
        else:
            logger.warning(f"Ошибка отправки в Telegram: {response.text}")
    except Exception as e:
        logger.error(f"Ошибка при отправке в Telegram: {e}")

default_args = {
    'owner': 'me',
    'start_date': datetime(2026, 6, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='Мой первый DAG с суммированием',
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

clear_table = PostgresOperator(
    task_id='clear_table',
    postgres_conn_id='postgres_default',
    sql="TRUNCATE TABLE test_table;",
    dag=dag
)

insert_data = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='postgres_default',
    sql="""
        INSERT INTO test_table (rows_count) VALUES (1);
        INSERT INTO test_table (rows_count) VALUES (2);
        INSERT INTO test_table (rows_count) VALUES (3);
        INSERT INTO test_table (rows_count) VALUES (4);
        INSERT INTO test_table (rows_count) VALUES (5);
    """,
    dag=dag
)

def calculate_sum(ti):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = "SELECT SUM(rows_count) FROM test_table;"
    result = pg_hook.get_records(sql)
    total_sum = result[0][0] if result else 0
    logger.info(f"Сумма чисел в таблице: {total_sum}")
    ti.xcom_push(key='total_sum', value=total_sum)
    return total_sum

calculate_sum_task = PythonOperator(
    task_id='calculate_sum',
    python_callable=calculate_sum,
    dag=dag
)

def send_sum(ti):
    total_sum = ti.xcom_pull(key='total_sum', task_ids='calculate_sum')
    message = f"✅ Сумма чисел в таблице: <b>{total_sum}</b>"
    send_telegram_message(message)
    logger.info(f"Отправлена сумма {total_sum} в Telegram")

send_sum_task = PythonOperator(
    task_id='send_sum',
    python_callable=send_sum,
    dag=dag
)

create_table >> clear_table >> insert_data >> calculate_sum_task >> send_sum_task