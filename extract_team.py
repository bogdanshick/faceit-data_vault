from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import json
import os

def extract_team_ids(**context):
    hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    sql = """
        SELECT DISTINCT team_id 
        FROM championship_results 
        WHERE team_id IS NOT NULL
    """
    records = hook.get_records(sql)
    current_ids = [row[0] for row in records]

    full_path = '/opt/airflow/dags/team_ids.json'
    new_path = '/opt/airflow/dags/new_team.json'

    # Загружаем уже существующие ID
    if os.path.exists(full_path):
        with open(full_path, 'r') as f:
            existing_ids = set(json.load(f))
    else:
        existing_ids = set()

    # Определяем новые команды
    new_ids = [team_id for team_id in current_ids if team_id not in existing_ids]

    # Обновляем основной файл
    all_ids = list(existing_ids.union(current_ids))
    with open(full_path, 'w') as f:
        json.dump(all_ids, f, indent=4)

    # Сохраняем только новые команды
    with open(new_path, 'w') as f:
        json.dump(new_ids, f, indent=4)

    print(f"✅ Извлечено {len(current_ids)} team_id.")
    print(f"➕ Найдено новых: {len(new_ids)}.")
    print(f"📦 Обновлён файл: {full_path}")
    print(f"🆕 Новый файл создан: {new_path}")

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='extract_team_ids_to_json',
    schedule_interval=None,
    default_args=default_args,
    description='Извлечение уникальных team_id из championship_results и сохранение в JSON',
    tags=['faceit', 'team', 'raw']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_team_ids',
        python_callable=extract_team_ids,
        provide_context=True
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_load_teams_dag',
        trigger_dag_id='load_teams_to_postgres',
        wait_for_completion=True  # можно убрать, если не нужно ждать
    )

    extract_task >> trigger_next_dag


