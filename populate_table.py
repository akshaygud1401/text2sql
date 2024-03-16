import boto3
import psycopg2
import os
import json
from dotenv import load_dotenv

load_dotenv()

session = boto3.Session(
     region_name = 'us-east-1',
     aws_access_key_id=os.getenv('api_key'),
     aws_secret_access_key=os.getenv('api_secret'))
s3 = session.resource('s3')

def alter_col_types(cur, conn):
    #Code I usede to reassign data types of columns. I had to do this when creating an empty table with only varchar columns, but resolved this by creating table with appropriate data types.
    #Kept this chunk of code for future use
    conn = conn
    cur = cur
    int_cols = ['player_id', 'gp', 'gs', 'fgm', 'fga', 'fg3m', 'fg3a', 'ftm', 'fta', 'oreb', 'dreb', 'reb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts']
    dec_cols = ['min', 'fg_pct', 'fg3_pct', 'ft_pct']
    for col in int_cols:
        cur.execute(f'ALTER TABLE careerstats ALTER COLUMN {col} SET DATA TYPE INT USING {col}::INTEGER;')
    for col in dec_cols:
        cur.execute(f'ALTER TABLE careerstats ALTER COLUMN {col} SET DATA TYPE NUMERIC USING {col}::NUMERIC;')

def data_cleaning(cur, conn):
    #Drop redundant columns, adjust the data types as necessary, delete duplicate player entries that have been collected by the api
    conn = conn
    cur = cur
    cur.execute('ALTER TABLE careerstats DROP COLUMN team_id, DROP COLUMN person_id')
    #alter_col_types(cur, conn)
    #This deletes duplicate entries that occur from running the file more than once, and keeps the latest values inserted 
    cur.execute('DELETE FROM careerstats a USING careerstats b WHERE b.player_id = a.player_id and b.ctid < a.ctid;')

def main():
    #populate the postgresql table
    #every time I want to run the file wiith new data collected in s3 bucket, I recreate the table and then clean it
    bucket = 'text2sql2'
    key = 'career_stats.json'
    local_path = '/tmp/career_stats.json'

    s3.meta.client.download_file(bucket, key, local_path)
    with open(local_path, 'r') as f:
        json_data = json.load(f)

    conn = psycopg2.connect("host=text2sqldb.czoygimeglkc.us-east-1.rds.amazonaws.com dbname=textsql user=text2postgres password=blu3ski3s")
    cur = conn.cursor()
    cur.execute('DROP TABLE careerstats;')
    cur.execute('''CREATE TABLE careerstats (
  PLAYER_ID int NOT NULL,
  Team_ID varchar NOT NULL,
  GP int NOT NULL,
  GS int NOT NULL,
  MIN int NOT NULL,
  FGM int NOT NULL,
  FGA int NOT NULL,
  FG_PCT numeric NOT NULL,
  FG3M int NOT NULL,
  FG3A int NOT NULL,
  FG3_PCT numeric NOT NULL,
  FTM int NOT NULL,
  FTA int NOT NULL,
  FT_PCT numeric NOT NULL,
  OREB int NOT NULL,
  DREB int NOT NULL,
  REB int NOT NULL,
  AST int NOT NULL,
  STL int NOT NULL,
  BLK int NOT NULL,
  TOV int NOT NULL,
  PF int NOT NULL,
  PTS int NOT NULL,
  PERSON_ID int NOT NULL,
  PLAYER_NAME varchar NOT NULL
);'''
)

    for item in json_data:
        columns = ', '.join(item.keys())
        values_template = ', '.join(['%s'] * len(item))
        insert_query = f"INSERT INTO careerstats ({columns}) VALUES ({values_template})"
        values = [item[column] for column in item.keys()]
        cur.execute(insert_query, values)
    data_cleaning(cur, conn)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main() 