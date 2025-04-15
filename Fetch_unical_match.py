from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
import json
from datetime import datetime


# Функция для получения уникальных матчей из базы данных
def get_unique_matches():
    # Подключение к базе данных через PostgresHook
    hook = PostgresHook(postgres_conn_id='Postgres_ROZA')  # Убедитесь, что правильный Conn ID
    conn = hook.get_conn()
    cursor = conn.cursor()

    # SQL запрос для получения уникальных матчей
    cursor.execute("SELECT DISTINCT match_id FROM raw_matches;")
    match_ids = cursor.fetchall()

    # Закрываем соединение
    cursor.close()
    conn.close()

    # Извлекаем только значения match_id
    return [match_id[0] for match_id in match_ids]


# Функция для сохранения матчей в JSON файл
def save_matches_to_json(matches_data):
    with open('/opt/airflow/dags/matches_data.json', 'w') as f:
        json.dump(matches_data, f, indent=4)


# Основная логика в DAG
def process_matches():
    match_ids = get_unique_matches()

    # Просто сохраняем список match_id
    save_matches_to_json(match_ids)


# Определение DAG
dag = DAG(
    'download_unique_matches',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 4, 14),
        'retries': 3,
    },
    schedule_interval=None,  # Запуск вручную
    catchup=False,
)

# Операторы
download_and_save_task = PythonOperator(
    task_id='download_and_save_matches',
    python_callable=process_matches,
    dag=dag,
)
