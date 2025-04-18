from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import json
import requests
import logging
import time
import os

# === Функция загрузки игроков ===
def load_player_data_to_postgres():
    api_key = '3f7d70c4-f8ca-42c9-98cb-3d1bdcc34ba7'
    headers = {'Authorization': f'Bearer {api_key}'}
    file_path = '/opt/airflow/dags/new_players.json'

    if not os.path.exists(file_path):
        logging.warning("⚠️ Файл с player_id не найден.")
        return

    with open(file_path, 'r') as f:
        player_ids = json.load(f)

    hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    conn = hook.get_conn()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO players (
            player_id,
            country,
            faceit_elo,
            game_player_id,
            game_player_name,
            region,
            skill_level,
            nickname
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (player_id) DO NOTHING;
    """

    def fetch_with_retries(url, headers, retries=5, base_delay=2):
        for i in range(retries):
            try:
                response = requests.get(url, headers=headers, timeout=(5, 10))
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", base_delay))
                    logging.warning(f"429 Too Many Requests. Retry after {retry_after}s")
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                wait = base_delay * (2 ** i)
                logging.warning(f"[Retry {i+1}] Ошибка при запросе {url}: {e}. Повтор через {wait} сек.")
                time.sleep(wait)
        raise Exception(f"Все {retries} попыток исчерпаны для {url}")

    batch_size = 100
    buffer = []

    for idx, player_id in enumerate(player_ids):
        url = f"https://open.faceit.com/data/v4/players/{player_id}"
        try:
            start_time = time.time()
            data = fetch_with_retries(url, headers)

            nickname = data.get("nickname")
            country = data.get("country")
            games = data.get("games", {}).get("cs2", {})

            faceit_elo = games.get("faceit_elo")
            game_player_id = games.get("game_player_id")
            game_player_name = games.get("game_player_name")
            region = games.get("region")
            skill_level = games.get("skill_level")

            buffer.append((
                player_id,
                country,
                faceit_elo,
                game_player_id,
                game_player_name,
                region,
                skill_level,
                nickname
            ))

            if (idx + 1) % 20 == 0:
                logging.info(f"[{idx+1}/{len(player_ids)}] ✅ игрок обработан: {nickname} за {time.time() - start_time:.2f} сек")

            if len(buffer) >= batch_size:
                cursor.executemany(insert_query, buffer)
                conn.commit()
                buffer.clear()
                logging.info(f"✅ Вставлено {batch_size} игроков в базу.")

            time.sleep(0.2)  # осторожная пауза между запросами

        except Exception as e:
            logging.error(f"❌ Не удалось обработать игрока {player_id}: {e}")

    if buffer:
        cursor.executemany(insert_query, buffer)
        conn.commit()

    cursor.close()
    conn.close()
    logging.info("🎉 Все игроки успешно загружены в базу данных.")

# === DAG Определение ===
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_players_to_postgres',
    description='Загрузка уникальных игроков в Postgres с Faceit API',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
)

load_players_task = PythonOperator(
    task_id='load_players_to_postgres',
    python_callable=load_player_data_to_postgres,
    dag=dag,
    execution_timeout=timedelta(hours=2),
)

trigger_task = TriggerDagRunOperator(
    task_id='trigger_hub_dag',
    trigger_dag_id='load_hub_player',
    dag=dag,
    wait_for_completion=True,
)

load_players_task >> trigger_task



