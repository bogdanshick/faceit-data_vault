from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests
import json
from datetime import datetime
from airflow.utils.dates import days_ago
import logging


# Функция для получения всех чемпионатов и сохранения их в JSON
def fetch_all_tournament_ids():
    offset = 0
    limit = 100  # Количество турниров на одну страницу
    game = 'cs2'
    api_key = '3f7d70c4-f8ca-42c9-98cb-3d1bdcc34ba7'
    url = f"https://open.faceit.com/data/v4/championships?game={game}&offset={offset}&limit={limit}&type=past"  # URL для запроса турниров

    headers = {
        'Authorization': f'Bearer {api_key}'
    }

    # Список для хранения всех championship_id
    b = []

    # Ограничение на количество записей
    max_records = 1000

    # Цикл, который будет увеличивать offset для каждой страницы
    while len(b) < max_records:
        # Выполняем запрос
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            tournaments = data.get('items', [])

            # Если турнирные записи закончились, выходим из цикла
            if not tournaments:
                break

            # Добавляем championship_id в список
            for tournament in tournaments:
                championship_id = tournament.get('championship_id')
                if championship_id:
                    b.append(championship_id)

                # Если собрали 1000 записей, выходим из цикла
                if len(b) >= max_records:
                    break

            # Увеличиваем offset для получения следующей страницы
            offset += limit
            # Обновляем URL с новым offset
            url = f"https://open.faceit.com/data/v4/championships?game={game}&offset={offset}&limit={limit}&type=past"

        else:
            print(f"Ошибка {response.status_code}: {response.text}")
            break

    # Сохраняем все championship_id в JSON файл
    with open('/opt/airflow/dags/championships_data.json', 'w') as f:
        json.dump(b, f, indent=4)

    print(f"Всего найдено {len(b)} championship_id. Данные сохранены в 'championships_data.json'.")


# Настройка логирования
logging.basicConfig(level=logging.INFO)

def fetch_and_load_tournaments_from_ids():
    api_key = '3f7d70c4-f8ca-42c9-98cb-3d1bdcc34ba7'
    headers = {
        'Authorization': f'Bearer {api_key}'
    }

    # Загружаем список championship_id из файла
    with open('/opt/airflow/dags/championships_data.json', 'r') as f:
        tournament_ids = json.load(f)

    # Подключение к базе данных через Airflow Hook
    pg_hook = PostgresHook(postgres_conn_id="Postgres_ROZA")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    for tournament_id in tournament_ids:
        url = f"https://open.faceit.com/data/v4/championships/{tournament_id}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            tournament = response.json()

            # Получаем нужные поля
            championship_id = tournament.get('championship_id')
            description = tournament.get('description', '')
            faceit_url = tournament.get('faceit_url', '')
            game_id = tournament.get('game_id')
            name = tournament.get('name')
            region = tournament.get('region')
            status = tournament.get('status')
            total_groups = tournament.get('total_groups', 0)
            total_prizes = tournament.get('total_prizes', 0)
            total_rounds = tournament.get('total_rounds', 0)

            # SQL-запрос с обработкой конфликта
            insert_sql = """
                INSERT INTO raw_tournaments (
                    championship_id, description, faceit_url, game_id,
                    name, region, status, total_groups, total_prizes, total_rounds, load_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (championship_id) DO NOTHING
            """

            try:
                cursor.execute(insert_sql, (
                    championship_id, description, faceit_url, game_id,
                    name, region, status, total_groups, total_prizes, total_rounds,
                    datetime.utcnow()
                ))
            except Exception as e:
                logging.error(f"❌ Ошибка при вставке данных для championship_id {championship_id}: {e}")

        else:
            logging.error(f"⚠️ Ошибка при получении данных для tournament ID {tournament_id}: {response.status_code}")

    connection.commit()
    cursor.close()
    logging.info("✅ Загрузка завершена.")



# Даг для загрузки турниров и их данных
dag = DAG(
    'load_tournaments_to_raw_layer',
    description='Load tournament data into Raw Layer every day and fetch tournament IDs weekly',
    schedule_interval='@daily',  # Запускать каждый день
    start_date=days_ago(1),  # Начать с 1 дня назад
    catchup=False  # Не запускать на предыдущие дни
)

# Операторы для выполнения задач

# Задача 1: Загрузка всех ID турниров раз в неделю
fetch_ids_task = PythonOperator(
    task_id='fetch_all_tournament_ids',
    python_callable=fetch_all_tournament_ids,
    dag=dag,
)

# Задача 2: Загрузка данных о турнире
load_tournaments_task = PythonOperator(
    task_id='load_tournaments_from_ids',
    python_callable=fetch_and_load_tournaments_from_ids,
    dag=dag
)

# Задачи выполняются последовательно
fetch_ids_task >> load_tournaments_task
