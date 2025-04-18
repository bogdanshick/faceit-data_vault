import hashlib
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def hash_key(input_string: str) -> str:
    """Функция для хэширования строки через SHA256 и обрезки до 32 символов"""
    hash_object = hashlib.sha256(input_string.encode('utf-8'))
    return hash_object.hexdigest()[:32]  # Обрезаем до 32 символов


def load_link_match_championship():
    """Загрузка линков match_id и championship_id с хэшами в таблицу линков"""

    # Создаем подключение через PostgresHook
    hook = PostgresHook(postgres_conn_id='Postgres_ROZA')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # SQL запрос для извлечения данных match_id и championship_id из raw_matches
    select_sql = """
    SELECT match_id, championship_id 
    FROM public.raw_matches
    """
    cursor.execute(select_sql)
    matches = cursor.fetchall()

    for match in matches:
        match_id, championship_id = match

        # Хэшируем match_id и championship_id
        match_hash = hash_key(str(match_id))
        championship_hash = hash_key(str(championship_id))

        # Склеиваем хэши для получения 64-символьного хэша
        combined_hash = match_hash + championship_hash

        # SQL запрос для вставки в таблицу линка
        insert_sql = """
              INSERT INTO data_vault.link_match_championship (link_hk, match_hk, championship_hk)
              VALUES (%s, %s, %s)
              ON CONFLICT (match_hk) DO NOTHING;
              """
        cursor.execute(insert_sql, (combined_hash, match_hash, championship_hash))
        conn.commit()  # Сохраняем изменения в БД

    cursor.close()
    conn.close()


# Создаем DAG
with DAG(
        dag_id='link_match_championship_dag',
        start_date=days_ago(1),
        schedule_interval=None,  # Запуск по требованию
        catchup=False,
) as dag:
    # Запуск функции загрузки линков
    load_link_match_championship_task = PythonOperator(
        task_id='load_link_match_championship_task',
        python_callable=load_link_match_championship,
        dag=dag,
    )

    trigger_satellites = TriggerDagRunOperator(
        task_id='trigger_satellites_after_links',
        trigger_dag_id='trigger_all_satellites_after_links',
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule='all_done'  # можно заменить на 'success' по желанию
    )

    load_link_match_championship_task >> trigger_satellites