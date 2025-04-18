from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import hashlib

# Функция генерации хэша
def generate_hash_key(input_str):
    return hashlib.sha256(input_str.encode()).hexdigest()[:32]

# Основная функция загрузки сателлита игроков
def load_sat_player():
    pg_hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Забираем игроков, даже если у них нет полной информации
    cursor.execute("""
        SELECT player_id, country, faceit_elo, region, skill_level, nickname
        FROM public.players
        WHERE player_id IS NOT NULL;
    """)
    rows = cursor.fetchall()

    for row in rows:
        player_id = row[0]
        player_hash = generate_hash_key(player_id)

        # Остальные поля могут быть NULL
        country = row[1] if row[1] else None
        faceit_elo = row[2] if row[2] is not None else None
        region = row[3] if row[3] else None
        skill_level = row[4] if row[4] is not None else None
        nickname = row[5] if row[5] else None

        insert_sql = """
        INSERT INTO data_vault.sat_player (
            player_hash, country, faceit_elo, region, skill_level, nickname
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (player_hash) DO NOTHING;
        """

        cursor.execute(insert_sql, (
            player_hash, country, faceit_elo, region, skill_level, nickname
        ))

    conn.commit()
    cursor.close()
    conn.close()

# DAG
with DAG(
    dag_id='load_sat_player',
    start_date=datetime(2025, 4, 15),
    schedule_interval=None,
    catchup=False,
    default_args={'owner': 'airflow'},
    tags=['dwh', 'data_vault']
) as dag:

    load_satellite = PythonOperator(
        task_id='load_sat_player',
        python_callable=load_sat_player
    )
