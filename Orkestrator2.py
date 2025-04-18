from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.models.baseoperator import cross_downstream

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='trigger_all_satellites_after_links',
    description='Ожидает завершения всех линков и запускает загрузку сателлитов',
    schedule_interval=None,
    start_date=datetime(2025, 4, 17),
    catchup=False,
    default_args=default_args,
    tags=['faceit', 'satellites', 'dependencies']
) as dag:

    wait_for_link_match_team = ExternalTaskSensor(
        task_id='wait_for_link_match_team',
        external_dag_id='load_link_match_team',
        external_task_id=None,
        timeout=3600,
        poke_interval=30,
        mode='poke'
    )

    wait_for_link_match_championship = ExternalTaskSensor(
        task_id='wait_for_link_match_championship',
        external_dag_id='link_match_championship_dag',
        external_task_id=None,
        timeout=3600,
        poke_interval=30,
        mode='poke'
    )

    wait_for_link_player_team = ExternalTaskSensor(
        task_id='wait_for_link_player_team',
        external_dag_id='load_link_player_team_no_conflict',
        external_task_id=None,
        timeout=3600,
        poke_interval=30,
        mode='poke'
    )

    trigger_sat_1 = TriggerDagRunOperator(
        task_id='trigger_sat_match_details',
        trigger_dag_id='load_sat_player_team_history'
    )

    trigger_sat_2 = TriggerDagRunOperator(
        task_id='trigger_sat_team_info',
        trigger_dag_id='load_sat_player'
    )

    trigger_sat_3 = TriggerDagRunOperator(
        task_id='trigger_sat_player_stats',
        trigger_dag_id='load_sat_player_match_stats'
    )

    trigger_sat_4 = TriggerDagRunOperator(
        task_id='trigger_sat_championship_metadata',
        trigger_dag_id='load_sat_championship_result'
    )

    trigger_sat_5 = TriggerDagRunOperator(
        task_id='trigger_sat_match_results',
        trigger_dag_id='load_sat_team'
    )

    trigger_sat_6 = TriggerDagRunOperator(
        task_id='trigger_sat_player_team_history',
        trigger_dag_id='load_sat_championship_metadata'
    )

    # Устанавливаем зависимости
    cross_downstream(
        [wait_for_link_match_team, wait_for_link_match_championship, wait_for_link_player_team],
        [
            trigger_sat_1, trigger_sat_2, trigger_sat_3,
            trigger_sat_4, trigger_sat_5, trigger_sat_6
        ]
    )
