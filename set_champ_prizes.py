from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import hashlib

def generate_hash_key(value):
    if value is None:
        return None
    return hashlib.sha256(value.encode()).hexdigest()[:32]

def load_sat_championship_metadata():
    pg_hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT championship_id, game_id, name, region, status, total_prizes
        FROM public.raw_tournaments
        WHERE championship_id IS NOT NULL;
    """)

    rows = cursor.fetchall()

    for row in rows:
        championship_id, game_id, name, region, status, total_prizes = row

        championship_hash = generate_hash_key(championship_id)

        insert_sql = """
            INSERT INTO data_vault.sat_championship_metadata (
                championship_hash, game_id, name, region, status, total_prizes
            )
            VALUES (%s, %s, %s, %s, %s, %s);
        """

        cursor.execute(insert_sql, (
            championship_hash, game_id, name, region, status, total_prizes
        ))

    conn.commit()
    cursor.close()
    conn.close()

# DAG
with DAG(
    dag_id='load_sat_championship_metadata',
    start_date=datetime(2025, 4, 15),
    schedule_interval=None,
    catchup=False,
    default_args={'owner': 'airflow'},
    tags=['dwh', 'data_vault']
) as dag:

    load_satellite = PythonOperator(
        task_id='load_sat_championship_metadata',
        python_callable=load_sat_championship_metadata
    )
