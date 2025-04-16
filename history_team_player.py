from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import hashlib

def generate_hash_key(value):
    return hashlib.sha256(value.encode()).hexdigest()[:32]

def load_sat_player_team_history():
    pg_hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Выбираем игроков, сменивших более одной команды
    cursor.execute("""
        SELECT r.team_id, r.leader_id, r.player_id, r.nickname
        FROM public.raw_team_members r
        WHERE r.player_id IN (
            SELECT p.player_id
            FROM public.players p
            LEFT JOIN public.raw_team_members r ON p.player_id = r.player_id
            GROUP BY p.player_id
            HAVING COUNT(DISTINCT team_id) > 1
        );
    """)

    rows = cursor.fetchall()

    for row in rows:
        team_id, leader_id, player_id, nickname = row

        # Хэшируем player_id и team_id
        if not player_id or not team_id:
            continue  # Пропуск если чего-то нет

        player_hk = generate_hash_key(player_id)
        team_hk = generate_hash_key(team_id)

        insert_sql = """
        INSERT INTO data_vault.sat_player_team_history (
            player_hk, team_hk, leader_id, nickname
        ) VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """

        try:
            cursor.execute(insert_sql, (player_hk, team_hk, leader_id, nickname))
        except Exception as e:
            print(f"Error inserting row {player_id} / {team_id}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id='load_sat_player_team_history',
    start_date=datetime(2025, 4, 16),
    schedule_interval=None,  # запуск вручную
    catchup=False,
    default_args={'owner': 'airflow'},
    tags=['dwh', 'data_vault']
) as dag:

    load_sat_task = PythonOperator(
        task_id='load_sat_player_team_history',
        python_callable=load_sat_player_team_history
    )
