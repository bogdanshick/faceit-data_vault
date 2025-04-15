from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import hashlib

# Функция для генерации хэш-ключа
def generate_hash_key(championship_id):
    hash_object = hashlib.sha256(championship_id.encode())
    return hash_object.hexdigest()[:32]

# Основная функция
def load_hub_championship():
    pg_hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    src_conn = pg_hook.get_conn()
    src_cursor = src_conn.cursor()

    # Получаем уникальные championship_id из RAW слоя
    src_cursor.execute("SELECT DISTINCT championship_id FROM public.raw_tournaments;")
    championship_ids = src_cursor.fetchall()

    # Вставляем в HUB, пропуская дубликаты (ON CONFLICT)
    for row in championship_ids:
        championship_id = row[0]
        hash_key = generate_hash_key(championship_id)

        insert_sql = """
        INSERT INTO data_vault.hub_championship (championship_id, hash_key)
        VALUES (%s, %s)
        ON CONFLICT (championship_id) DO NOTHING;
        """

        src_cursor.execute(insert_sql, (championship_id, hash_key))

    src_conn.commit()
    src_cursor.close()
    src_conn.close()

# DAG
with DAG(
    dag_id='load_hub_championship',
    start_date=datetime(2025, 4, 14),
    schedule_interval=None,
    catchup=False,
    default_args={'owner': 'airflow'},
    tags=['dwh', 'data_vault']
) as dag:

    load_hub = PythonOperator(
        task_id='load_championship_hub',
        python_callable=load_hub_championship
    )
