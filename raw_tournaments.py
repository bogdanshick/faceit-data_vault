from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import requests
import json
import logging

def fetch_and_store_tournament_info():
    api_key = '3f7d70c4-f8ca-42c9-98cb-3d1bdcc34ba7'
    headers = {'Authorization': f'Bearer {api_key}'}
    new_champ_path = '/opt/airflow/dags/new_champ.json'

    with open(new_champ_path, 'r') as f:
        championship_ids = json.load(f)

    pg_hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for championship_id in championship_ids:
        url = f"https://open.faceit.com/data/v4/championships/{championship_id}"
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            logging.error(f"Не удалось получить данные чемпионата {championship_id}: {response.status_code}")
            continue

        data = response.json()

        try:
            cursor.execute("""
                INSERT INTO raw_tournaments (
                    championship_id, description, faceit_url, game_id, name, region,
                    status, total_groups, total_prizes, total_rounds, load_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (championship_id) DO NOTHING
            """, (
                data.get("championship_id"),
                data.get("description"),
                data.get("faceit_url"),
                data.get("game_id"),
                data.get("name"),
                data.get("region"),
                data.get("status"),
                data.get("total_groups"),
                data.get("total_prizes"),
                data.get("total_rounds"),
                datetime.utcnow()
            ))
        except Exception as e:
            logging.error(f"Ошибка при вставке чемпионата {championship_id}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("✅ Загрузка информации о чемпионатах завершена")

# Определение DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='load_championship_details',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Загрузка детальной информации по чемпионатам в raw_tournaments',
    tags=['faceit', 'championships', 'raw']
) as dag:

    load_championships_task = PythonOperator(
        task_id='fetch_and_insert_championships',
        python_callable=fetch_and_store_tournament_info
    )

    trigger_task = TriggerDagRunOperator(
        task_id='trigger_Hub_dag',
        trigger_dag_id='load_hub_championship',
        dag=dag,
        wait_for_completion=True,
    )

    load_championships_task >> trigger_task
