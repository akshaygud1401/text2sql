#Important: run this file only once to populate table
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

def data_cleaning(cur, conn):
    #Drop redundant columns, adjust the data types as necessary, delete duplicate player entries that have been collected by the api
    #Ran this once, but keeping this function in case I lose the table
    conn = conn
    cur = cur
    cur.execute('ALTER TABLE careerstats DROP COLUMN team_id, DROP COLUMN person_id')
    int_cols = ['player_id', 'gp', 'gs', 'fgm', 'fga', 'fg3m', 'fg3a', 'ftm', 'fta', 'oreb', 'dreb', 'reb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts']
    dec_cols = ['min', 'fg_pct', 'fg3_pct', 'ft_pct']
    for col in int_cols:
        cur.execute(f'ALTER TABLE careerstats ALTER COLUMN {col} SET DATA TYPE INT USING {col}::INTEGER;')
    for col in dec_cols:
        cur.execute(f'ALTER TABLE careerstats ALTER COLUMN {col} SET DATA TYPE NUMERIC USING {col}::NUMERIC;')
    #This also deletes duplicate entries that occur from running the file more than once
    cur.execute('DELETE FROM careerstats a USING careerstats b WHERE b.player_id = a.player_id and b.ctid < a.ctid;')

def main():
    #populate the empty postgresql table created in aws
    bucket = 'text2sql2'
    key = 'career_stats.json'
    local_path = '/tmp/career_stats (2).json'

    s3.meta.client.download_file(bucket, key, local_path)
    with open(local_path, 'r') as f:
        json_data = json.load(f)

    conn = psycopg2.connect("host=text2sqldb.czoygimeglkc.us-east-1.rds.amazonaws.com dbname=textsql user=text2postgres password=blu3ski3s")
    cur = conn.cursor()

    for item in json_data:
        columns = ', '.join(item.keys())
        values_template = ', '.join(['%s'] * len(item))
        insert_query = f"INSERT INTO careerstats ({columns}) VALUES ({values_template})"
        values = [item[column] for column in item.keys()]
        cur.execute(insert_query, values)
    #data_cleaning(cur, conn)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main() 