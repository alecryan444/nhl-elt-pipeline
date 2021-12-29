import requests
from datetime import datetime
import pandas as pd
from common.s3.s3_functions import upload_df_to_s3

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


def extract_game_plays(game_id_list):
    "Requests a JSON object for a list of game_ids using NHL API"
    
    json_list = []
    
    for game_id in game_id_list:
        r = requests.get(f'{base_url}/game/{game_id}/feed/live')
        json = r.json()
        json_list.append(json)

    return json_list


def stage_game_data_s3(ti, task):
    '''Extracts/flattens game play data from NHL API JSON response object'''

    game_id_list = ti.xcom_pull(task_ids='extract_game_ids_to_list', key = 'game_id_list')

    json_list = extract_game_plays(game_id_list)

    plays_list = []

    for json in json_list:

        game_id = json['gamePk']

        #Each invidivdual play
        plays = pd.json_normalize(json['liveData']['plays']['allPlays'], sep ='_')
        plays['game_pk'] = game_id
        plays['gamePartition'] = game_id
        
        try:
            #Remove players column from plays DF
            plays.drop('players', axis = 1, inplace = True)

            #Cast datetime to timestamp
            plays['about_dateTime'] = pd.to_datetime(plays['about_dateTime'])
        
        except KeyError:
            #Raised when there are not game plays for game_id
            #Game ids will not have plays if postponed
            pass

        plays_list.append(plays)

    if len(plays_list) == 0:
        print("Nothing to load.")            
    else:
        df = pd.concat(plays_list, ignore_index=True)

        #Write df to s3
        upload_df_to_s3(df, BUCKET, task.task_id, ['gamePartition'])


def stage_game_metadata_s3(ti, task):
    '''Extracts/flattens game metadata from NHL API JSON response object'''

    game_id_list = ti.xcom_pull(task_ids='extract_game_ids_to_list', key = 'game_id_list')

    json_list = extract_game_plays(game_id_list)

    game_metadata_list = []

    for json in json_list:
    
        game_id = json['gamePk']

        #Remove players data
        json['gameData'].pop('players', 'DNE')
        game_metadata = pd.json_normalize(json['gameData'], sep = '_')
        game_metadata['gamePartition'] = game_metadata['game_pk']
        
        game_metadata_list.append(game_metadata)

    if len(game_metadata_list) == 0:
        print("Nothing to load.")            
    else:
        df = pd.concat(game_metadata_list, ignore_index=True)

        #Write df to s3
        upload_df_to_s3(df, BUCKET, task.task_id, ['gamePartition'])


def stage_game_play_players_s3(ti, task):
    '''Extracts/flattens game play players data from NHL API JSON response object'''

    game_id_list = ti.xcom_pull(task_ids='extract_game_ids_to_list', key = 'game_id_list')

    json_list = extract_game_plays(game_id_list)

    game_play_players_list = []

    for json in json_list:

        game_id = json['gamePk']

        #Each invidivdual play
        players_list = []

        for i in list(range(len(json['liveData']['plays']['allPlays']))):
            try:
                players = pd.json_normalize(json['liveData']['plays']['allPlays'][i], record_path = ['players'], meta = [['about', 'eventIdx']], sep ='_' )
                players_list.append(players) 
            except KeyError:
                #Game Start events don't contain players
                players = pd.json_normalize(json['liveData']['plays']['allPlays'][i],  meta = [['about', 'eventIdx']], sep ='_' )
                players_list.append(players) 

        try:
            play_players = pd.concat(players_list)        
            play_players['game_pk'] = game_id
            play_players['gamePartition'] = game_id

            game_play_players_list.append(play_players)

        except ValueError:
            #Raised when game is postponed
            #If game is postponed there are no plays that occur
            pass
        
    if len(game_play_players_list) == 0:
        print("Nothing to load.")            
    else:
        df = pd.concat(game_play_players_list, ignore_index=True)

        #Write df to s3
        upload_df_to_s3(df, BUCKET, task.task_id, ['gamePartition'])
            

def stage_game_play_players_metadata_s3(ti, task):
    '''Extracts/flattens game player metadata from NHL API JSON response object'''

    game_id_list = ti.xcom_pull(task_ids='extract_game_ids_to_list', key = 'game_id_list')

    json_list = extract_game_plays(game_id_list)

    player_metadata_list = []

    for json in json_list:

        #Need Game id to capture changes in captain assignments, weight changes, etc.
        game_id = json['gamePk']

        #Instantiate for of player metadata
        player_metadata_df_list = []

        #Add each individual player metadata object to list
        for key, val in json['gameData']['players'].items():
            df = pd.json_normalize(val , sep ='_')
            player_metadata_df_list.append(df)

        try:

            #Concatenate player metadata DatatFrames    
            player_metadata = pd.concat(player_metadata_df_list)

            #Add game_id and partition column to DataFrame
            player_metadata['game_pk'] = game_id
            player_metadata['gamePartition'] = game_id

            player_metadata_list.append(player_metadata)
        
        except ValueError: 
            # No objects to concatenate
            ## Raised whem game is postponed and game_id issues null records
            pass

    if len(player_metadata_df_list) == 0:
        print("Nothing to load.")            
    else:
        df = pd.concat(player_metadata_list, ignore_index=True)

        #Write df to s3
        upload_df_to_s3(df, BUCKET, task.task_id, ['gamePartition'])