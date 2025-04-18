from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import json
import os

# Пути к файлам
FULL_PATH = '/opt/airflow/dags/matches_data.json'
NEW_PATH = '/opt/airflow/dags/new_matches.json'

def get_unique_matches():
    hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    records = hook.get_records("SELECT DISTINCT match_id FROM raw_matches;")
    return [r[0] for r in records]

def save_matches_to_json():
    current_ids = get_unique_matches()

    # Загружаем уже сохранённые ID
    if os.path.exists(FULL_PATH):
        with open(FULL_PATH, 'r') as f:
            existing_ids = set(json.load(f))
    else:
        existing_ids = set()

    # Определяем новые ID
    new_ids = [mid for mid in current_ids if mid not in existing_ids]

    # Обновляем основной файл
    all_ids = list(existing_ids.union(current_ids))
    with open(FULL_PATH, 'w') as f:
        json.dump(all_ids, f, indent=4)

    # Перезаписываем файл с новыми матчами
    with open(NEW_PATH, 'w') as f:
        json.dump(new_ids, f, indent=4)

    print(f"✅ Всего уникальных match_id: {len(all_ids)}")
    print(f"➕ Новых match_id за запуск: {len(new_ids)}")
    print(f"📦 Обновлён: {FULL_PATH}")
    print(f"🆕 Новый: {NEW_PATH}")

dag = DAG(
    'download_unique_matches',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 4, 14),
        'retries': 3,
    },
    schedule_interval=None,
    catchup=False,
)

download_and_save_task = PythonOperator(
    task_id='download_and_save_matches',
    python_callable=save_matches_to_json,
    dag=dag,
)

trigger_next_dag = TriggerDagRunOperator(
    task_id='trigger_download_unique_matches',
    trigger_dag_id='process_and_save_faceit_stats',  # ID DAG-а, который нужно запустить
    wait_for_completion=True,  # Ждать завершения следующего DAG (опционально)
    dag=dag
)

# Зависимости: сначала загрузка матчей, потом запуск следующего DAG-а
download_and_save_task >> trigger_next_dag