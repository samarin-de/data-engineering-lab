from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import logging

logger = logging.getLogger(__name__)

# Твои данные для Telegram
TELEGRAM_TOKEN = ''
TELEGRAM_CHAT_ID = ''

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

def on_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    error = context.get('exception', 'Неизвестная ошибка')
    message = f"❌ <b>Ошибка в DAG</b>\n\nDAG: {dag_id}\nЗадача: {task_id}\nОшибка: {error}"
    send_telegram_message(message)

default_args = {
    'owner': 'me',
    'start_date': datetime(2026, 7, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure_callback
}

dag = DAG(
    'spark_etl_dag',
    default_args=default_args,
    description='Запуск ETL на Spark через BashOperator',
    schedule_interval='@once',
    catchup=False
)

# 1. Задача: запустить Spark-скрипт через bash
spark_task = BashOperator(
    task_id='run_spark_etl',
    bash_command='python /opt/airflow/dags/spark_etl.py',
    dag=dag
)

# 2. Задача: отправить уведомление об успехе
def send_success_notification():
    message = "✅ <b>Spark ETL завершён успешно!</b>\n\nДанные записаны в test_table_spark."
    send_telegram_message(message)

success_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag
)

# Порядок выполнения
spark_task >> success_notification
