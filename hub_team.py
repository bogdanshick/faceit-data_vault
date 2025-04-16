from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import hashlib


# Функция генерации хэш-ключа
def generate_hash_key(team_id):
    return hashlib.sha256(team_id.encode()).hexdigest()[:32]


# Основная функция
def load_hub_team():
    pg_hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Новый SQL-запрос: объединяем team_1_id и team_2_id
    cursor.execute("""
        with team as (
            select team_1_id as team_id from public.raw_matches
            union
            select team_2_id as team_id from public.raw_matches
        )
        select distinct team_id from team where team_id is not null;
    """)

    team_ids = cursor.fetchall()

    for row in team_ids:
        team_id = row[0]
        hash_key = generate_hash_key(team_id)

        insert_sql = """
        INSERT INTO data_vault.hub_team (team_id, hash_key)
        VALUES (%s, %s)
        ON CONFLICT (team_id) DO NOTHING;
        """
        cursor.execute(insert_sql, (team_id, hash_key))

    conn.commit()
    cursor.close()
    conn.close()


# DAG
with DAG(
        dag_id='load_hub_team',
        start_date=datetime(2025, 4, 14),
        schedule_interval=None,
        catchup=False,
        default_args={'owner': 'airflow'},
        tags=['dwh', 'data_vault']
) as dag:
    load_team_hub = PythonOperator(
        task_id='load_team_hub',
        python_callable=load_hub_team
    )
