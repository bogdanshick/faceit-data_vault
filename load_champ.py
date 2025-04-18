from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta
import requests
import json
import logging
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Пути к JSON-файлам
ALL_CHAMPIONSHIPS_PATH = '/opt/airflow/dags/championships_data.json'
NEW_CHAMPIONSHIPS_PATH = '/opt/airflow/dags/new_champ.json'

API_URL = "https://open.faceit.com/data/v4/championships"
API_KEY = '3f7d70c4-f8ca-42c9-98cb-3d1bdcc34ba7'
GAME = 'cs2'

def update_championship_ids(**context):
    headers = {'Authorization': f'Bearer {API_KEY}'}
    limit = 100
    max_new = 50
    offset = 0
    total_seen = 0
    just_new_ids = []

    # Загружаем уже известные championship_id
    if os.path.exists(ALL_CHAMPIONSHIPS_PATH):
        with open(ALL_CHAMPIONSHIPS_PATH, 'r') as f:
            existing_ids = set(json.load(f))
    else:
        existing_ids = set()

    logging.info(f"✅ Уже загружено {len(existing_ids)} championship_id")

    # Проходим по 100 за раз, пока не пропустим 1001 чемпионат
    while len(just_new_ids) < max_new:
        url = f"{API_URL}?game={GAME}&offset={offset}&limit={limit}&type=past"
        logging.info(f"🔄 Запрос: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        items = data.get('items', [])
        if not items:
            logging.info("📭 Пустой ответ — выходим.")
            break

        for item in items:
            total_seen += 1
            if total_seen <= 1001:
                continue  # просто пропускаем
            champ_id = item.get('championship_id')
            if champ_id and champ_id not in existing_ids:
                just_new_ids.append(champ_id)
                logging.info(f"🆕 Новый championship_id: {champ_id}")
            if len(just_new_ids) >= max_new:
                break

        offset += limit

    if just_new_ids:
        # Обновим all_ids
        updated_ids = list(existing_ids.union(just_new_ids))
        with open(ALL_CHAMPIONSHIPS_PATH, 'w') as f:
            json.dump(updated_ids, f, indent=2)
        with open(NEW_CHAMPIONSHIPS_PATH, 'w') as f:
            json.dump(just_new_ids, f, indent=2)
        context['ti'].xcom_push(key='new_ids_found', value=True)
        logging.info(f"✅ Найдено и записано {len(just_new_ids)} новых championship_id")
    else:
        context['ti'].xcom_push(key='new_ids_found', value=False)
        logging.info("🟡 Новых championship_id не найдено.")


# === Определение DAG ===
with DAG(
    dag_id='update_championship_ids_dag',
    description='Обновляет JSON с ID чемпионатов CS2 и запускает зависимые DAG\'и при наличии новых',
    schedule_interval='0 0 */3 * *',  # раз в 3 дня в 00:00
    start_date=datetime(2025, 4, 17),
    catchup=False,
    default_args=default_args,
    tags=['faceit', 'championships', 'cs2']
) as dag:

    check_and_trigger_results = PythonOperator(
        task_id='update_championship_ids',
        python_callable=update_championship_ids,
        provide_context=True,
    )


    trigger_results_dag = TriggerDagRunOperator(
        task_id='trigger_championship_results_loader',
        trigger_dag_id='load_championship_results',
        wait_for_completion=False,
        poke_interval=60,
        reset_dag_run=True,
        trigger_rule='all_done'
    )

    trigger_matches_dag = TriggerDagRunOperator(
        task_id='trigger_faceit_matches_loader',
        trigger_dag_id='load_faceit_matches',
        wait_for_completion=False,
        poke_interval=60,
        reset_dag_run=True,
        trigger_rule='all_done'
    )

    trigger_save_players_dag = TriggerDagRunOperator(
        task_id='trigger_save_players_to_json',
        trigger_dag_id='save_unique_players_to_json',
        wait_for_completion=False,
        poke_interval=60,
        reset_dag_run=True,
        trigger_rule='all_done'
    )

    trigger_championship_details_dag = TriggerDagRunOperator(
        task_id='trigger_load_championship_details',
        trigger_dag_id='load_championship_details',
        wait_for_completion=False,
        poke_interval=60,
        reset_dag_run=True,
        trigger_rule='all_done'
    )

    check_and_trigger_results >> [
        trigger_results_dag,
        trigger_matches_dag,
        trigger_save_players_dag,
        trigger_championship_details_dag
    ]
