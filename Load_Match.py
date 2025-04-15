from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
import json
import requests
import logging
from datetime import datetime


def fetch_and_load_matches_by_championships():
    api_key = '3f7d70c4-f8ca-42c9-98cb-3d1bdcc34ba7'
    headers = {'Authorization': f'Bearer {api_key}'}

    with open('/opt/airflow/dags/championships_data.json', 'r') as f:
        championship_ids = json.load(f)

    pg_hook = PostgresHook(postgres_conn_id="Postgres_ROZA")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for championship_id in championship_ids:
        offset = 0
        limit = 50
        match_type = 'past'  # Можно изменить в зависимости от типа матчей
        while True:
            url = f"https://open.faceit.com/data/v4/championships/{championship_id}/matches?type={match_type}&offset={offset}&limit={limit}"
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                logging.error(f"Failed to fetch matches for championship {championship_id}: {response.status_code}")
                break

            data = response.json()
            matches = data.get('items', [])
            if not matches:
                break

            for match in matches:
                try:
                    match_id = match['match_id']
                    region = match.get('region', '')
                    game = match.get('game', '')
                    status = match.get('status', '')
                    winner = match.get('results', {}).get('winner', '')
                    map_played = match.get('voting', {}).get('map', {}).get('pick', [None])[0]

                    # Проверяем наличие команд и их ID
                    teams = match.get('teams', {})
                    faction1 = teams.get('faction1', {})
                    faction2 = teams.get('faction2', {})

                    team_1_name = faction1.get('name', '')
                    team_2_name = faction2.get('name', '')
                    team_1_id = faction1.get('faction_id', '')
                    team_2_id = faction2.get('faction_id', '')

                    if not team_1_id or not team_2_id:
                        logging.warning(f"Match {match_id} missing team IDs. Skipping match.")
                        continue

                    cursor.execute("""
                        INSERT INTO raw_matches 
                        (match_id, championship_id, region, team_1_name, team_2_name, winner, game, status, map, team_1_id, team_2_id, load_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (match_id) DO NOTHING
                    """, (
                        match_id, championship_id, region, team_1_name, team_2_name,
                        winner, game, status, map_played, team_1_id, team_2_id, datetime.utcnow()
                    ))

                except Exception as e:
                    logging.error(
                        f"Error processing match {match.get('match_id')} in championship {championship_id}: {e}")

            offset += limit

    conn.commit()
    cursor.close()


dag = DAG(
    'load_faceit_matches',
    description='Load FACEIT matches data to raw_matches',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

load_matches_task = PythonOperator(
    task_id='load_matches_from_championships',
    python_callable=fetch_and_load_matches_by_championships,
    dag=dag,
)

