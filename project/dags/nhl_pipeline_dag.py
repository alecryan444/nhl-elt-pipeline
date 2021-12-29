from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

from common.nhl_request_to_s3 import extract_game_ids_to_list
from common.nhl_request_to_s3 import stage_game_data_s3
from common.nhl_request_to_s3 import stage_game_metadata_s3
from common.nhl_request_to_s3 import stage_game_play_players_s3
from common.nhl_request_to_s3 import stage_game_play_players_metadata_s3

default_args = {
    'owner':'airflow',
    'start_date':'2021-09-26' #start of preseason,
}

with DAG(
    'nhl_elt', 
    default_args=default_args, 
    description='NHL ELT pipeline for analytics.',
    schedule_interval='@daily',
    catchup=False
    ) as dag:

    start = DummyOperator(
        task_id='start'
    )

    extract_game_ids_to_list = PythonOperator(
        task_id = 'extract_game_ids_to_list',
        python_callable=extract_game_ids_to_list,
        provide_context=True
    )
    with TaskGroup('stage_nhl_data_s3') as stage_nhl_data_s3:

        stage_game_data_s3 = PythonOperator(
            task_id = 'stage_game_data_s3',
            python_callable = stage_game_data_s3,
            provide_context=True
        )

        stage_game_metadata_s3 = PythonOperator(
            task_id = 'stage_game_metadata_s3',
            python_callable = stage_game_metadata_s3,
            provide_context=True
        )

        stage_game_play_players_s3 = PythonOperator(
            task_id = 'stage_game_play_players_s3',
            python_callable = stage_game_play_players_s3,
            provide_context=True
        )

        stage_game_play_players_metadata_s3 = PythonOperator(
            task_id = 'stage_game_play_players_metadata_s3',
            python_callable = stage_game_play_players_metadata_s3,
            provide_context=True
        )

    end = DummyOperator(
        task_id='end'
    )
    
    start >> extract_game_ids_to_list >> stage_nhl_data_s3 >> end