from airflow import DAG
#from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

#Custom Functions
from common.nhl_request import extract_game_ids_to_list
from common.nhl_request import extract_game_data
from common.nhl_request import extract_game_metadata
from common.nhl_request import extract_player_metadata
from common.nhl_request import extract_game_play_players


default_args = {
    'owner':'airflow',
    'start_date':'2021-12-26' #start of preseason
}

with DAG('nhl_elt', default_args=default_args, description='NHL ELT pipeline for analytics.', schedule_interval='@daily') as dag:

    start = DummyOperator(
        task_id='start'
    )

    extract_game_ids_to_list = PythonOperator(
        task_id = 'extract_game_ids_to_list',
        python_callable=extract_game_ids_to_list,
        provide_context=True
    )

    with TaskGroup('request_nhl_data') as request_nhl_data:
        extract_game_data = PythonOperator(
            task_id = 'extract_game_data',
            python_callable=extract_game_data,
            provide_context=True
            )   

        extract_game_metadata = PythonOperator(
            task_id = 'extract_game_metadata',
            python_callable=extract_game_metadata,
            provide_context=True
            )

        extract_game_play_players = PythonOperator(
            task_id = 'extract_game_play_players',
            python_callable=extract_game_play_players,
            provide_context=True
            )

        extract_player_metadata = PythonOperator(
            task_id = 'extract_player_metadata',
            python_callable=extract_player_metadata,
            provide_context=True
            )

    end = DummyOperator(
        task_id='end'
    )

    
start >> extract_game_ids_to_list >> request_nhl_data >> end