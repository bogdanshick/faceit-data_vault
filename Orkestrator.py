from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import datetime
from datetime import timedelta
from airflow.models.baseoperator import cross_downstream

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='wait_for_hubs_and_trigger_links',
    default_args=default_args,
    description='Ждёт завершения всех DAG-ов хабов и запускает DAG-и линков',
    schedule_interval=None,
    catchup=False,
    tags=['data_vault', 'hub', 'link']
) as dag:

    wait_for_hub_match = ExternalTaskSensor(
        task_id='wait_for_hub_match',
        external_dag_id='load_hub_match',
        external_task_id=None,
        timeout=3600,
        poke_interval=30,
        mode='poke'
    )

    wait_for_hub_team = ExternalTaskSensor(
        task_id='wait_for_hub_team',
        external_dag_id='load_hub_team',
        external_task_id=None,
        timeout=3600,
        poke_interval=30,
        mode='poke'
    )

    wait_for_hub_player = ExternalTaskSensor(
        task_id='wait_for_hub_player',
        external_dag_id='load_hub_player',
        external_task_id=None,
        timeout=3600,
        poke_interval=30,
        mode='poke'
    )

    wait_for_hub_championship = ExternalTaskSensor(
        task_id='wait_for_hub_championship',
        external_dag_id='load_hub_championship',
        external_task_id=None,
        timeout=3600,
        poke_interval=30,
        mode='poke'
    )

    trigger_link_match_team = TriggerDagRunOperator(
        task_id='trigger_link_match_team',
        trigger_dag_id='load_link_match_team',
        wait_for_completion=False
    )

    trigger_link_match_championship = TriggerDagRunOperator(
        task_id='trigger_link_match_championship',
        trigger_dag_id='link_match_championship_dag',
        wait_for_completion=False
    )

    trigger_link_player_team = TriggerDagRunOperator(
        task_id='trigger_link_player_team',
        trigger_dag_id='load_link_player_team_no_conflict',
        wait_for_completion=False
    )

    # Порядок: сначала ждём все хабы, потом одновременно запускаем все линки
    cross_downstream(
        [wait_for_hub_match, wait_for_hub_team, wait_for_hub_player, wait_for_hub_championship],
        [trigger_link_match_team, trigger_link_match_championship, trigger_link_player_team]
    )