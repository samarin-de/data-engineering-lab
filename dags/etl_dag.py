from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import requests

logger = logging.getLogger(__name__)

# ВСТАВЬТЕ СВОИ ДАННЫЕ ↓↓↓
TELEGRAM_TOKEN = '8897904426:AAFrVNCSzsC9PSRa0cwfJqeVEtYBglU-UBE'
TELEGRAM_CHAT_ID = '1355480201'
# ↑↑↑ ВСТАВЬТЕ СВОИ ДАННЫЕ

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
    'start_date': datetime(2026, 6, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure_callback
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL-пайплайн с уведомлениями в Telegram',
    schedule_interval='0 10 * * *',
    catchup=False
)

# Задача 0: Создать таблицу для логов запусков
create_log_table = PostgresOperator(
    task_id='create_log_table',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE TABLE IF NOT EXISTS dag_run_log (
            id SERIAL PRIMARY KEY,
            run_id VARCHAR(255),
            start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(50)
        );
    """,
    dag=dag
)

# Задача 1: Создать целевую таблицу
create_target_table = PostgresOperator(
    task_id='create_target_table',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE TABLE IF NOT EXISTS test_table_transformed (
            id SERIAL PRIMARY KEY,
            original_id INTEGER,
            original_rows_count INTEGER,
            transformed_rows_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    dag=dag
)

# Задача 2: Чтение и трансформация
def extract_transform(ti):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = "SELECT id, rows_count FROM test_table;"
    rows = pg_hook.get_records(sql)
    
    if not rows:
        logger.warning("Таблица test_table пуста. Пропускаем трансформацию.")
        ti.xcom_push(key='skip', value=True)
        return []
    
    logger.info(f"Извлечено {len(rows)} записей")
    
    transformed_data = []
    for row in rows:
        original_id = row[0]
        original_rows_count = row[1]
        new_rows_count = int(original_rows_count * 1.1)
        transformed_data.append((original_id, original_rows_count, new_rows_count))
        logger.info(f"ID: {original_id}, Было: {original_rows_count}, Стало: {new_rows_count}")
    
    ti.xcom_push(key='transformed_data', value=transformed_data)
    return transformed_data

extract_transform_task = PythonOperator(
    task_id='extract_transform',
    python_callable=extract_transform,
    dag=dag
)

# Задача 3: Загрузка данных
def load_data(ti):
    skip = ti.xcom_pull(key='skip', task_ids='extract_transform')
    if skip:
        logger.info("Пропускаем загрузку, так как данных нет.")
        return
    
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='extract_transform')
    
    if not transformed_data:
        logger.error("Нет данных для загрузки!")
        return
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    for record in transformed_data:
        original_id, original_rows_count, new_rows_count = record
        sql = """
            INSERT INTO test_table_transformed (original_id, original_rows_count, transformed_rows_count)
            VALUES (%s, %s, %s);
        """
        pg_hook.run(sql, parameters=(original_id, original_rows_count, new_rows_count))
    
    logger.info(f"Загружено {len(transformed_data)} записей в test_table_transformed")

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

# Задача 4: Записать в лог запуска
def log_run(ti):
    run_id = ti.dag_run.run_id if ti.dag_run else "manual"
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = "INSERT INTO dag_run_log (run_id, status) VALUES (%s, 'success');"
    pg_hook.run(sql, parameters=(run_id,))
    logger.info(f"Лог запуска сохранён: {run_id}")

log_task = PythonOperator(
    task_id='log_run',
    python_callable=log_run,
    dag=dag
)

# Задача 5: Проверка результата и уведомление
def check_result(ti): # <-- добавили параметр ti (чтобы читать XCom)
    
    # 1. Забираем данные из XCom
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='extract_transform')
    
    # 2. Считаем количество строк
    if transformed_data:
        rows_count = len(transformed_data)
        total_sum = sum([row[2] for row in transformed_data])  # Суммируем все transformed_rows_count
        # Бонус: можно взять первое число для примера
        first_row = transformed_data[0] if transformed_data else None
        if first_row:
            original, transformed = first_row[1], first_row[2]
            message = (
                f"✅ <b>ETL-пайплайн успешно завершён!</b>\n\n"
                f"📊 Обработано строк: <b>{rows_count}</b>\n"                
                f"📈 Пример трансформации: {original} → {transformed}\n"
                f"💰 Суммарное значение: <b>{total_sum}</b>\n"
                f"⏱️ Запуск: {datetime.now().strftime('%H:%M:%S')}"
            )
        else:
            message = "✅ ETL-пайплайн завершён, но данные не были обработаны."
    else:
        # Если данных нет (таблица была пуста)
        message = "⚠️ ETL-пайплайн завершён, но новых данных для обработки не найдено."
    
    # 3. Отправляем в Telegram
    send_telegram_message(message)
    logger.info("Уведомление с данными отправлено в Telegram!")
    

check_task = PythonOperator(
    task_id='check_result',
    python_callable=check_result,
    dag=dag
)

# Порядок выполнения
create_log_table >> create_target_table >> extract_transform_task >> load_task >> log_task >> check_task