from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import hashlib

# Функция для генерации хэш-ключа
def generate_hash_key(value):
    hash_object = hashlib.sha256(value.encode())
    return hash_object.hexdigest()[:32]

def load_link_player_team():
    pg_hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    src_conn = pg_hook.get_conn()
    src_cursor = src_conn.cursor()

    # Запрос игроков и их команд
    src_cursor.execute("""
        SELECT DISTINCT p.player_id::text, r.team_id::text
        FROM public.players p
        LEFT JOIN public.raw_team_members r ON p.player_id = r.player_id;
    """)
    player_team_data = src_cursor.fetchall()

    for row in player_team_data:
        player_id = row[0]
        team_id = row[1]

        player_hash = generate_hash_key(player_id)
        team_hash = generate_hash_key(team_id) if team_id else None

        print(f"Attempt insert: player_id={player_id}, team_id={team_id}, player_hash={player_hash}, team_hash={team_hash}")

        insert_sql = """
        INSERT INTO data_vault.link_player_team (player_hash, team_hash, load_date)
        VALUES (%s, %s, NOW());
        """

        try:
            src_cursor.execute(insert_sql, (player_hash, team_hash))
        except Exception as e:
            print(f"Error inserting ({player_id}, {team_id}): {e}")

    src_conn.commit()
    src_cursor.close()
    src_conn.close()

# DAG
with DAG(
    dag_id='load_link_player_team_no_conflict',
    start_date=datetime(2025, 4, 16),
    schedule_interval=None,
    catchup=False,
    default_args={'owner': 'airflow'},
    tags=['dwh', 'data_vault']
) as dag:

    load_link_player_team_task = PythonOperator(
        task_id='load_link_player_team',
        python_callable=load_link_player_team
    )
