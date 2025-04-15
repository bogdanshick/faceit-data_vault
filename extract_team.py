from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import json
import os

# Путь до JSON-файла
OUTPUT_PATH = '/opt/airflow/dags/team_ids.json'  # поменяй путь при необходимости

def extract_team_ids(**context):
    hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    sql = """
        SELECT DISTINCT team_id 
        FROM championship_results 
        WHERE team_id IS NOT NULL
    """
    records = hook.get_records(sql)
    team_ids = [row[0] for row in records]

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(team_ids, f, indent=4)

    print(f"✅ Извлечено {len(team_ids)} уникальных team_id.")
    print(f"📦 Сохранено в: {OUTPUT_PATH}")

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

    extract_task

