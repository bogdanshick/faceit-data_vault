from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import hashlib

def generate_hash_key(value):
    if value is None:
        return None
    return hashlib.sha256(value.encode()).hexdigest()[:32]

def load_sat_player_match_stats():
    pg_hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Получаем данные из сырой таблицы
    cursor.execute("""
        SELECT match_id, player_id, nickname, team_id, team_name, map_name, region,
               kills, deaths, assists, headshots, headshot_percentage,
               kills_deaths_ratio, kills_per_round, mvps,
               triple_kills, quadro_kills, penta_kills
        FROM public.match_player_stats
        WHERE player_id IS NOT NULL AND match_id IS NOT NULL;
    """)

    rows = cursor.fetchall()

    for row in rows:
        (match_id, player_id, nickname, team_id, team_name, map_name, region,
         kills, deaths, assists, headshots, headshot_percentage,
         kd_ratio, kills_per_round, mvps,
         triple_kills, quadro_kills, penta_kills) = row

        match_hash = generate_hash_key(match_id)
        player_hash = generate_hash_key(player_id)
        team_hash = generate_hash_key(team_id) if team_id else None

        insert_sql = """
            INSERT INTO data_vault.sat_player_match_stats (
                match_hash, player_hash, team_hash,
                nickname, team_name, map_name, region,
                kills, deaths, assists, headshots, headshot_percentage,
                kills_deaths_ratio, kills_per_round, mvps,
                triple_kills, quadro_kills, penta_kills
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        cursor.execute(insert_sql, (
            match_hash, player_hash, team_hash,
            nickname, team_name, map_name, region,
            kills, deaths, assists, headshots, headshot_percentage,
            kd_ratio, kills_per_round, mvps,
            triple_kills, quadro_kills, penta_kills
        ))

    conn.commit()
    cursor.close()
    conn.close()

# Определяем DAG
with DAG(
    dag_id='load_sat_player_match_stats',
    start_date=datetime(2025, 4, 15),
    schedule_interval=None,
    catchup=False,
    default_args={'owner': 'airflow'},
    tags=['dwh', 'data_vault']
) as dag:

    load_satellite = PythonOperator(
        task_id='load_sat_player_match_stats',
        python_callable=load_sat_player_match_stats
    )
