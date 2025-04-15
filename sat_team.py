from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import hashlib


def generate_hash_key(value):
    if value is None:
        return None
    return hashlib.sha256(value.encode()).hexdigest()[:32]


def load_sat_team():
    pg_hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # SQL: получаем уникальные team_id / team_name без 'bye'
    cursor.execute("""
        SELECT DISTINCT team_id, team_name FROM (
            SELECT team_1_id AS team_id, team_1_name AS team_name
            FROM public.raw_matches
            WHERE team_1_id <> 'bye' AND team_1_name <> 'bye'

            UNION

            SELECT team_2_id AS team_id, team_2_name AS team_name
            FROM public.raw_matches
            WHERE team_2_id <> 'bye' AND team_2_name <> 'bye'
        ) AS teams;
    """)

    teams = cursor.fetchall()

    for row in teams:
        team_id, team_name = row
        team_hash = generate_hash_key(team_id)

        insert_sql = """
            INSERT INTO data_vault.sat_team (team_hash, team_id, team_name)
            VALUES (%s, %s, %s);
        """

        cursor.execute(insert_sql, (team_hash, team_id, team_name))

    conn.commit()
    cursor.close()
    conn.close()


# DAG
with DAG(
        dag_id='load_sat_team',
        start_date=datetime(2025, 4, 15),
        schedule_interval=None,
        catchup=False,
        default_args={'owner': 'airflow'},
        tags=['dwh', 'data_vault']
) as dag:
    load_satellite = PythonOperator(
        task_id='load_sat_team',
        python_callable=load_sat_team
    )
