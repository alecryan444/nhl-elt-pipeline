import requests
from datetime import datetime
import pandas as pd
from common.s3.s3_functions import upload_json_to_s3

base_url = 'https://statsapi.web.nhl.com/api/v1'

BUCKET = 'nhl-db-data'

def extract_game_ids_to_list(ds, ti, task):
    """Creates list of game_ids that can be used to get player stats"""
    
    game_id_list = []
    
    request_url = f'{base_url}/schedule?startDate={ds}&endDate={ds}'
    r = requests.get(request_url) 
    j = r.json()
    
    dates = j['dates']
    
    for day in dates:
        games = day['games']
        for game in games:
            game_id = game['gamePk']
            
            game_id_list.append(game_id)

    print(game_id_list)
    
    ti.xcom_push(key = 'game_id_list', value = game_id_list)


def stage_nhl_game_data(ti, task):
    "Requests a JSON object for a list of game_ids using NHL API"

    game_id_list = ti.xcom_pull(task_ids='extract_game_ids_to_list', key = 'game_id_list')
    
    for game_id in game_id_list:
        r = requests.get(f'{base_url}/game/{game_id}/feed/live')
        json = r.json()
        df = pd.DataFrame(json)

        upload_json_to_s3(df, BUCKET, task.task_id, ['gamePk'])