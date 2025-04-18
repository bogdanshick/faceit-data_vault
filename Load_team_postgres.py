from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import json
import requests
import logging
import time
import os


# === Функция загрузки информации о командах ===
def load_team_data_to_postgres():
    api_key = '3f7d70c4-f8ca-42c9-98cb-3d1bdcc34ba7'  # Пример ключа API
    headers = {'Authorization': f'Bearer {api_key}'}
    file_path = '/opt/airflow/dags/new_team.json'  # Путь к JSON файлу с уникальными team_id

    if not os.path.exists(file_path):
        logging.warning("⚠️ Файл с team_id не найден.")
        return

    with open(file_path, 'r') as f:
        team_ids = json.load(f)

    hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    conn = hook.get_conn()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO raw_team_members  (
            team_id,
            game,
            leader_id,
            player_id,
            nickname,
            load_date
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (team_id, player_id) DO NOTHING;
    """

    # Функция для обработки запросов с одной попыткой
    def fetch_with_one_attempt(url, headers):
        try:
            response = requests.get(url, headers=headers, timeout=(5, 10))
            response.raise_for_status()  # Если статус не 200, будет исключение
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.warning(f"Ошибка при запросе {url}: {e}")
            return None  # Возвращаем None, если запрос не удался

    # Обработка каждой команды
    for idx, team_id in enumerate(team_ids):
        url = f"https://open.faceit.com/data/v4/teams/{team_id}"  # URL для получения информации о команде
        start_time = time.time()

        # Получаем данные с одной попытки
        data = fetch_with_one_attempt(url, headers)

        if data:
            # Если данные есть, обрабатываем команду и вставляем игроков
            team_id = data.get("team_id")
            game = data.get("game")
            leader = data.get("leader")
            members = data.get("members", [])

            for member in members:
                player_id = member.get("user_id")
                nickname = member.get("nickname")

                cursor.execute(insert_query, (
                    team_id,
                    game,
                    leader,
                    player_id,
                    nickname,
                    time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())  # Время вставки данных
                ))

            logging.info(
                f"[{idx + 1}/{len(team_ids)}] ✅ Команда обработана и данные вставлены в базу за {time.time() - start_time:.2f} сек")
        else:
            # Если данных нет, вставляем только team_id и оставляем другие поля NULL
            cursor.execute(insert_query, (
                team_id,
                None,  # game
                None,  # leader_id
                None,  # player_id
                None,  # nickname
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())  # Время вставки данных
            ))
            logging.warning(f"❌ Не удалось получить данные для команды {team_id}. Вставлен только team_id.")

        conn.commit()  # Коммитим каждую команду сразу
        time.sleep(0.2)  # осторожная пауза между запросами

    cursor.close()
    conn.close()
    logging.info("🎉 Все команды успешно загружены в базу данных.")


# === DAG Определение ===
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_teams_to_postgres',
    description='Загрузка уникальных команд в Postgres с Faceit API',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
)

load_teams_task = PythonOperator(
    task_id='load_teams_to_postgres',
    python_callable=load_team_data_to_postgres,
    dag=dag,
    execution_timeout=timedelta(hours=2),
)
