from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

from common.nhl.nhl_request_to_s3 import extract_game_ids_to_list
from common.nhl.nhl_request_to_s3 import stage_nhl_game_data

default_args = {
    'owner':'airflow',
    'start_date':'2021-12-14' #start of 2021-22 preseason,
}

with DAG(
    'nhl_elt', 
    default_args=default_args, 
    description='NHL ELT pipeline for analytics.',
    schedule_interval='@daily',
    catchup=True
    ) as dag:

    start = DummyOperator(
        task_id='start'
    )

    extract_game_ids_to_list = PythonOperator(
        task_id = 'extract_game_ids_to_list',
        python_callable=extract_game_ids_to_list,
        provide_context=True
    )
    
    stage_nhl_game_data = PythonOperator(
        task_id = 'stage_nhl_game_data',
        python_callable=stage_nhl_game_data,
        provide_context=True
    )    

    end = DummyOperator(
        task_id='end'
    )
    
    start >> extract_game_ids_to_list >> stage_nhl_game_data >> end