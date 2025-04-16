from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def load_championship_team_result():
    pg_hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO data_mart.fct_championship_team_result (
            championship_hash,
            championship_name,
            region,
            status,
            team_hash,
            team_name,
            team_type,
            game_id,
            placement_range,
            total_prizes,
            load_date
        )
        SELECT 
            scr.championship_hash,
            scm.name AS championship_name,
            scm.region,
            scm.status,
            scr.team_hash,
            scr.team_name,
            scr.team_type,
            scm.game_id,
            scr.placement_range,
            scm.total_prizes,
            CURRENT_DATE
        FROM data_vault.sat_championship_result scr
        INNER JOIN data_vault.sat_championship_metadata scm 
            ON scr.championship_hash = scm.championship_hash;
    """

    cursor.execute(insert_sql)
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id='load_fct_championship_team_result',
    start_date=datetime(2025, 4, 14),
    schedule_interval=None,
    catchup=False,
    default_args={'owner': 'airflow'},
    tags=['data_mart', 'championship', 'analytics']
) as dag:

    load_task = PythonOperator(
        task_id='load_championship_team_result',
        python_callable=load_championship_team_result
    )
