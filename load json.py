# first_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import json
import requests
import logging
import time
import os

def save_unique_players_to_json():
    api_key = '3f7d70c4-f8ca-42c9-98cb-3d1bdcc34ba7'
    headers = {'Authorization': f'Bearer {api_key}'}
    file_path = '/opt/airflow/dags/unique_players.json'

    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            existing_ids = set(json.load(f))
    else:
        existing_ids = set()

    with open('/opt/airflow/dags/championships_data.json', 'r') as f:
        championships_data = json.load(f)

    new_ids = set()
    total_matches = 0

    for i, championship_id in enumerate(championships_data):
        logging.info(f"[{i+1}/{len(championships_data)}] Обработка чемпионата {championship_id}")
        offset = 0
        limit = 100
        type = 'past'

        while True:
            url = f'https://open.faceit.com/data/v4/championships/{championship_id}/matches?type={type}&offset={offset}&limit={limit}'
            response = requests.get(url, headers=headers)
            time.sleep(0.1)

            if response.status_code != 200:
                logging.error(f"Ошибка запроса матчей для чемпионата {championship_id}: {response.status_code}")
                break

            matches = response.json().get('items', [])
            if not matches:
                break

            total_matches += len(matches)

            for match in matches:
                for team in match.get('teams', {}).values():
                    for player in team.get('roster', []):
                        player_id = player.get('player_id')
                        if player_id and player_id not in existing_ids:
                            new_ids.add(player_id)

            logging.info(f"  -> {len(matches)} матчей обработано, найдено уникальных игроков: {len(new_ids)}")

            if len(matches) < limit:
                break
            offset += limit

    combined_ids = list(existing_ids.union(new_ids))

    with open(file_path, 'w') as f:
        json.dump(combined_ids, f, indent=4)

    logging.info(f"Добавлено {len(new_ids)} новых player_id. Всего в файле: {len(combined_ids)}. Обработано матчей: {total_matches}")


# === DAG Definition ===
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'save_unique_players_to_json',
    description='Fetch unique players and save them to JSON',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
)

save_players_task = PythonOperator(
    task_id='save_unique_players_to_json',
    python_callable=save_unique_players_to_json,
    dag=dag,
    execution_timeout=timedelta(minutes=30),
)

# Добавляем триггер для второго DAG
trigger_task = TriggerDagRunOperator(
    task_id='trigger_second_dag',
    trigger_dag_id='load_players_to_postgres',  # Указываем ID второго DAG
    dag=dag,
    wait_for_completion=True,  # Ожидание завершения второго DAG
)

save_players_task >> trigger_task
