from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator# Обновлено с airflow.operators.python_operator
import json
import requests
import time
from datetime import datetime
import psycopg2
import psycopg2.extras


# Чтение match_id из JSON файла
def read_match_ids_from_json():
    with open('/opt/airflow/dags/new_matches.json', 'r') as f:
        match_ids = json.load(f)
    return match_ids


# Функция для выполнения запроса к Faceit API
def fetch_match_data(match_id):
    url = f'https://open.faceit.com/data/v4/matches/{match_id}/stats'
    headers = {
        'Authorization': 'Bearer 3f7d70c4-f8ca-42c9-98cb-3d1bdcc34ba7',  # Замените на свой API ключ
    }

    # Пытаемся сделать запрос с повтором в случае ошибки
    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                time.sleep(0.2)  # Ожидание перед повтором запроса
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for match {match_id}: {e}")
            time.sleep(0.2)  # Ожидание перед повтором запроса
    return None  # Возвращаем None, если не удалось получить данные


# Функция для обработки каждого матча и сохранения статистики в базе данных
def save_match_player_stats(match_ids):
    hook = PostgresHook(postgres_conn_id='Postgres_ROZA')  # Убедитесь, что правильный Conn ID
    conn = hook.get_conn()
    cursor = conn.cursor()

    for match_id in match_ids:
        match_data = fetch_match_data(match_id)

        # Если данных нет, пропускаем матч
        if not match_data:
            print(f"Skipping match {match_id} due to missing data.")
            continue  # Переходим к следующему матчу, если данных нет

        # Собираем данные по матчу
        match_stats = []
        for round_data in match_data.get('rounds', []):
            map_name = round_data.get('round_stats', {}).get('Map', None)
            region = round_data.get('round_stats', {}).get('Region', None)
            winner_team_id = round_data.get('round_stats', {}).get('Winner', None)

            for team in round_data.get('teams', []):
                team_id = team.get('team_id')
                team_name = team.get('team_stats', {}).get('Team', None)

                for player in team.get('players', []):
                    player_stats = player.get('player_stats', {})
                    player_id = player.get('player_id')
                    nickname = player.get('nickname')

                    # Добавляем данные о матче и игроке
                    match_stats.append((
                        match_id, player_id, nickname, team_id, team_name,
                        map_name, region, winner_team_id,
                        player_stats.get('Kills', None), player_stats.get('Deaths', None),
                        player_stats.get('Assists', None), player_stats.get('Headshots', None),
                        player_stats.get('Headshots %', None), player_stats.get('K/D Ratio', None),
                        player_stats.get('K/R Ratio', None), player_stats.get('MVPs', None),
                        player_stats.get('Triple Kills', None), player_stats.get('Quadro Kills', None),
                        player_stats.get('Penta Kills', None)
                    ))

        # Вставляем все данные по матчу за один запрос
        if match_stats:
            query = """
            INSERT INTO match_player_stats (
                match_id, player_id, nickname, team_id, team_name, 
                map_name, region, winner_team_id, kills, deaths, assists, headshots, 
                headshot_percentage, kills_deaths_ratio, kills_per_round, mvps, triple_kills, 
                quadro_kills, penta_kills
            ) 
            VALUES %s
            ON CONFLICT (match_id, player_id) DO NOTHING;
            """
            # Вставляем все строки за один запрос
            psycopg2.extras.execute_values(cursor, query, match_stats)
            conn.commit()

        time.sleep(0.2)  # Задержка перед запросом следующего матча

    cursor.close()
    conn.close()


# Основная логика для чтения данных и заполнения таблицы
def process_and_save_stats():
    match_ids = read_match_ids_from_json()  # Чтение match_id из файла
    save_match_player_stats(match_ids)  # Сохранение статистики в таблицу SQL



# Определение DAG
dag = DAG(
    'process_and_save_faceit_stats',
default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 4, 14),
        'retries': 3,
    },
    schedule_interval=None,  # Запуск вручную
    catchup=False,
)

# Операторы
process_match_stats_task = PythonOperator(
    task_id='process_and_save_match_stats',
    python_callable=process_and_save_stats,
    dag=dag
)
trigger_hub_match = TriggerDagRunOperator(
    task_id='trigger_hub_match',
    trigger_dag_id='load_hub_match',
    wait_for_completion=False,
    reset_dag_run=True,
    trigger_rule='all_done',
    dag=dag,
)

trigger_hub_team = TriggerDagRunOperator(
    task_id='trigger_hub_team',
    trigger_dag_id='load_hub_team',
    wait_for_completion=False,
    reset_dag_run=True,
    trigger_rule='all_done',
    dag=dag
)

process_match_stats_task >> [trigger_hub_match, trigger_hub_team]

