from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import hashlib

# Генератор хэша с обрезкой
def generate_hash_key(value):
    if value is None:
        return None
    return hashlib.sha256(value.encode()).hexdigest()[:32]

# Основная функция загрузки
def load_sat_championship_result():
    pg_hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Забираем только записи, где team_id указан
    cursor.execute("""
        SELECT championship_id, team_id, team_name, team_type, placement_range
        FROM public.championship_results
        WHERE team_id IS NOT NULL;
    """)

    rows = cursor.fetchall()

    for row in rows:
        championship_id, team_id, team_name, team_type, placement_range = row

        championship_hash = generate_hash_key(championship_id)
        team_hash = generate_hash_key(team_id)

        insert_sql = """
            INSERT INTO data_vault.sat_championship_result (
                championship_hash, team_hash, team_name, team_type, placement_range
            )
            VALUES (%s, %s, %s, %s, %s);
        """

        cursor.execute(insert_sql, (
            championship_hash, team_hash, team_name, team_type, placement_range
        ))

    conn.commit()
    cursor.close()
    conn.close()

# DAG
with DAG(
    dag_id='load_sat_championship_result',
    start_date=datetime(2025, 4, 15),
    schedule_interval=None,
    catchup=False,
    default_args={'owner': 'airflow'},
    tags=['dwh', 'data_vault']
) as dag:

    load_satellite = PythonOperator(
        task_id='load_sat_championship_result',
        python_callable=load_sat_championship_result
    )
