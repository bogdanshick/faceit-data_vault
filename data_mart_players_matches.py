from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def load_fct_player_match_stats():
    pg_hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO data_mart.fct_player_match_stats (
            match_hash, player_hash, team_hash, team1_hash, team2_hash, winner_hash, team_name, map_name, region,
            kills, deaths, assists, headshots, headshot_percentage, kills_deaths_ratio, kills_per_round,
            mvps, triple_kills, quadro_kills, penta_kills, country, faceit_elo, skill_level, nickname, load_date
        )
        SELECT 
            spms.match_hash, spms.player_hash, team_hash, team1_hash, team2_hash, winner_hash, team_name, map_name, 
            spms.region, kills, deaths, assists, headshots, headshot_percentage, kills_deaths_ratio, kills_per_round, 
            mvps, triple_kills, quadro_kills, penta_kills, country, faceit_elo, skill_level, sp.nickname,
            CURRENT_DATE
        FROM data_vault.sat_player_match_stats spms
        INNER JOIN data_vault.match_team_link mtl 
            ON spms.match_hash = mtl.match_hash
        INNER JOIN data_vault.sat_player sp
            ON spms.player_hash = sp.player_hash;
    """

    cursor.execute(insert_sql)
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id='load_fct_player_match_stats',
    start_date=datetime(2025, 4, 14),
    schedule_interval=None,
    catchup=False,
    default_args={'owner': 'airflow'},
    tags=['data_mart', 'player_stats', 'analytics']
) as dag:

    load_task = PythonOperator(
        task_id='load_player_match_stats',
        python_callable=load_fct_player_match_stats
    )
