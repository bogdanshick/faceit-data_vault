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

    # Запрос для получения данных о командах и игроках, а также лидере
    src_cursor.execute("""
        SELECT team_id, leader_id, player_id
        FROM public.raw_team_members
        WHERE player_id IS NOT NULL;
    """)
    player_team_data = src_cursor.fetchall()

    # Вставка в link_player_team
    for row in player_team_data:
        team_id = row[0]
        leader_id = row[1]
        player_id = row[2]

        # Хэшируем team_id, leader_id и player_id
        team_hash = generate_hash_key(team_id)
        leader_hash = generate_hash_key(leader_id) if leader_id else None
        player_hash = generate_hash_key(player_id)

        # Вставка в таблицу link_player_team
        insert_sql = """
        INSERT INTO data_vault.link_player_team (team_hash, leader_player_hash, player_hash)
        VALUES (%s, %s, %s);
        """

        try:
            src_cursor.execute(insert_sql, (team_hash, leader_hash, player_hash))
        except Exception as e:
            print(f"Error inserting row {team_hash}, {leader_hash}, {player_hash}: {e}")
            # Продолжаем выполнение, если ошибка вставки

    src_conn.commit()
    src_cursor.close()
    src_conn.close()

# Основной DAG
with DAG(
    dag_id='load_link_player_team',
    start_date=datetime(2025, 4, 15),
    schedule_interval=None,  # Запуск вручную
    catchup=False,
    default_args={'owner': 'airflow'},
    tags=['dwh', 'data_vault']
) as dag:

    load_link_player_team_task = PythonOperator(
        task_id='load_link_player_team',
        python_callable=load_link_player_team
    )
