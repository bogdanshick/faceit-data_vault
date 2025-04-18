from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import json
import requests
import logging
import time
import os

def load_championship_results():
    file_path = '/opt/airflow/dags/new_champ.json'
    base_url = "https://open.faceit.com/data/v4/championships/{champ_id}/results"
    api_key = '3f7d70c4-f8ca-42c9-98cb-3d1bdcc34ba7'
    headers = {'Authorization': f'Bearer {api_key}'}

    if not os.path.exists(file_path):
        logging.warning("Файл new_champ.json не найден.")
        return

    with open(file_path, 'r') as f:
        championship_ids = json.load(f)

    hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    conn = hook.get_conn()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO championship_results (
            championship_id,
            team_id,
            team_name,
            team_type,
            placement_range,
            loaded_at
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """

    for idx, champ_id in enumerate(championship_ids):
        offset = 0
        limit = 100
        has_more = True
        total_inserted = 0

        logging.info(f"▶️ Начинаем загрузку чемпионата {champ_id}")

        while has_more:
            url = f"{base_url.format(champ_id=champ_id)}?offset={offset}&limit={limit}"

            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                data = response.json()

                items = data.get("items", [])
                if not items:
                    logging.warning(f"⚠️ Нет данных для чемпионата: {champ_id} — вставляем NULL-строку.")
                    cursor.execute(insert_query, (
                        champ_id,
                        None,
                        None,
                        None,
                        None,
                        datetime.now().date()
                    ))
                    total_inserted += 1
                    break

                for item in items:
                    left = item.get("bounds", {}).get("left")
                    right = item.get("bounds", {}).get("right")
                    placement_range = f"{left}" if left == right else f"{left}-{right}"

                    placements = item.get("placements", [])
                    for placement in placements:
                        team_id = placement.get("id")
                        team_name = placement.get("name")
                        team_type = placement.get("type")

                        if team_id and team_name:
                            cursor.execute(insert_query, (
                                champ_id,
                                team_id,
                                team_name,
                                team_type,
                                placement_range,
                                datetime.now().date()
                            ))
                            total_inserted += 1

                has_more = len(items) == limit
                offset += limit
                time.sleep(0.3)

            except Exception as e:
                logging.error(f"❌ Ошибка при обработке {champ_id} (offset={offset}): {e}")
                break

        logging.info(f"[{idx+1}/{len(championship_ids)}] ✅ Чемпионат обработан: {champ_id}, вставлено: {total_inserted} строк")

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("🏁 Все чемпионаты успешно загружены.")


# === DAG definition ===
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='load_championship_results',
    description='Загрузка результатов чемпионатов FACEIT в базу данных',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=['faceit', 'championships']
) as dag:

    load_results = PythonOperator(
        task_id='load_championship_results',
        python_callable=load_championship_results,
        execution_timeout=timedelta(hours=1),
    )

    trigger_extract_team_ids = TriggerDagRunOperator(
        task_id='trigger_extract_team_ids_to_json',
        trigger_dag_id='extract_team_ids_to_json',
        wait_for_completion=False,
        trigger_rule='all_success',
    )

    load_results >> trigger_extract_team_ids
