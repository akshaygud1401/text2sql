from nba_api.stats.endpoints import commonallplayers, playercareerstats
import boto3
import json
from dotenv import load_dotenv
import os

load_dotenv()



# Step 1: Retrieve career player statistics for every active player
active_players = commonallplayers.CommonAllPlayers(is_only_current_season=1)
active_players_data = active_players.get_data_frames()[0]


all_players_career_stats = []


for index, player_row in active_players_data.iterrows():
   player_id = player_row['PERSON_ID']
   player_name = player_row['DISPLAY_FIRST_LAST']
  
   career = playercareerstats.PlayerCareerStats(player_id=player_id)
   career_stats = career.get_normalized_dict()['CareerTotalsRegularSeason']
  
   if len(career_stats) > 0:
       # Add player ID and name to career stats
       career_stats1 = career_stats[0]
       career_stats1['PERSON_ID'] = player_id
       career_stats1['PLAYER_NAME'] = player_name
       del career_stats1['LEAGUE_ID']
  
       all_players_career_stats.append(career_stats1)


# Step 2: Convert the data to JSON
career_stats_json = json.dumps(all_players_career_stats)


# Step 3: Upload the data to an Amazon S3 bucket
session = boto3.Session(
     region_name = 'us-east-1',
     aws_access_key_id=os.getenv('api_key'),
     aws_secret_access_key=os.getenv('api_secret'))
s3 = session.resource('s3')
bucket_name = 'text2sql2'
file_name = 'career_stats.json'


s3.Bucket(bucket_name).put_object(Key=file_name, Body=career_stats_json)

