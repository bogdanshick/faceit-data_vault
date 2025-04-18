from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import hashlib

# Функция для генерации хэш-ключа
def generate_hash_key(value):
    hash_object = hashlib.sha256(value.encode())
    return hash_object.hexdigest()[:32]

# Основная функция для загрузки линка
def load_link_match_team():
    pg_hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    src_conn = pg_hook.get_conn()
    src_cursor = src_conn.cursor()

    # Запрос для получения матчей
    src_cursor.execute("""
        SELECT match_id, 
               team_1_id, 
               team_2_id, 
               CASE 
                   WHEN winner = 'faction1' THEN team_1_id
                   WHEN winner = 'faction2' THEN team_2_id
                   ELSE NULL
               END AS winner
        FROM public.raw_matches
        WHERE team_1_id <> 'bye' 
          AND team_2_id <> 'bye';
    """)
    matches = src_cursor.fetchall()

    # Вставляем в таблицу match_team_link (match_hash, team1_hash, team2_hash, winner_hash)
    for row in matches:
        match_id = row[0]
        team_1_id = row[1]
        team_2_id = row[2]
        winner = row[3]

        match_hash = generate_hash_key(match_id)
        team_1_hash = generate_hash_key(team_1_id)
        team_2_hash = generate_hash_key(team_2_id)
        winner_hash = generate_hash_key(winner) if winner else None

        # Вставка в таблицу match_team_link (match_hash, team1_hash, team2_hash, winner_hash)
        insert_sql = """
               INSERT INTO data_vault.match_team_link (
                   match_hash, team1_hash, team2_hash, winner_hash
               ) VALUES (%s, %s, %s, %s)
               ON CONFLICT (match_hash) DO NOTHING;
               """

        try:
            src_cursor.execute(insert_sql, (match_hash, team_1_hash, team_2_hash, winner_hash))
        except Exception as e:
            print(f"Error inserting row {match_hash}: {e}")
            # Если возникает ошибка, продолжаем, не останавливая выполнение скрипта

    src_conn.commit()
    src_cursor.close()
    src_conn.close()

# DAG
with DAG(
    dag_id='load_link_match_team',
    start_date=datetime(2025, 4, 14),
    schedule_interval=None,
    catchup=False,
    default_args={'owner': 'airflow'},
    tags=['dwh', 'data_vault']
) as dag:

    load_link = PythonOperator(
        task_id='load_link_match_team',
        python_callable=load_link_match_team
    )
