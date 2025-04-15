from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import hashlib

# Функция для генерации хэш-ключа
def generate_hash_key(team_id):
    hash_object = hashlib.sha256(team_id.encode())
    return hash_object.hexdigest()[:32]

# Основная функция
def load_hub_team():
    pg_hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    src_conn = pg_hook.get_conn()
    src_cursor = src_conn.cursor()

    # Получаем уникальные championship_id из RAW слоя
    src_cursor.execute("select distinct team_id from public.raw_team_members;")
    team_ids = src_cursor.fetchall()

    # Вставляем в HUB, пропуская дубликаты (ON CONFLICT)
    for row in team_ids:
        team_id = row[0]
        hash_key = generate_hash_key(team_id)

        insert_sql = """
        INSERT INTO data_vault.hub_team (team_id, hash_key)
        VALUES (%s, %s)
        ON CONFLICT (team_id) DO NOTHING;
        """

        src_cursor.execute(insert_sql, (team_id, hash_key))

    src_conn.commit()
    src_cursor.close()
    src_conn.close()

# DAG
with DAG(
    dag_id='load_hub_team',
    start_date=datetime(2025, 4, 14),
    schedule_interval=None,
    catchup=False,
    default_args={'owner': 'airflow'},
    tags=['dwh', 'data_vault']
) as dag:

    load_hub = PythonOperator(
        task_id='load_team_hub',
        python_callable=load_hub_team
    )
