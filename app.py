from dotenv import load_dotenv
import streamlit as st
import os
import psycopg2
import google.generativeai as genai

load_dotenv()

#Configure the API key
genai.configure(api_key=os.getenv('genai_key'))

#Run and collect Google Gemini Pro's response as a sql query
def retrieve_response(usr_q, prompt):
    model = genai.GenerativeModel('gemini-pro')
    output = model.generate_content([prompt[0],usr_q])
    return output.text

#Use LLM's sql query to get result from the Postgresql database
def retrieve_sql(sql):
    conn = psycopg2.connect("host=text2sqldb.czoygimeglkc.us-east-1.rds.amazonaws.com dbname=textsql user=text2postgres password=blu3ski3s")
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    conn.commit()
    conn.close()
    for result in results:
        print(result)
    return results


#Super important- define the LLM prompt as detailed as possible so the model has the best chance of executing correctly
prompt = [
    """You excel at converting English questions to SQL queries. 
    This Postgresql database contains the career statistics of every active player in the NBA.
    The database is named textsql. The table in the database is named careerstats and has the following columns: player_id, gp (which means total games played in the player's career), 
    gs (which means total games started in the player's career), min (which means total minutes played in the player's career),
    fgm (which means total field goals made in the player's career), fga (which means total field goals attempted in the player's career),
    fg_pct (which means field goal percentage in the player's career), fg3m (which means the amount of made 3 point field goals in the player's career),
    fg3a (which means the amount of 3 point field goals attempted in the player's career), 
    fg3_pct (which means 3 point field goal percentage in the player's career), 
    ftm (which means the total amount of made free throws in the player's career),
    fta (which means the total amount of free throws attempted in the player's career), ft_pct (which means free throw percentage in the player's career),
    oreb (which means the amount of offensive rebounds collected in the player's career), dreb (which means the amount of defensive rebounds collected in the player's career),
    reb (which means total amount of rebounds, both offensive and defensive, collected in the player's career),
    ast (which means total amount of assists in the player's career), stl (which means the total amount of steals in the player's career),
    blk (which means total amount of blocks in the player's career), tov (which means the total amount of turnovers commited in the player's career),
    pf (which means total amount of personal fouls committedd in the player's career), pts (which means total points scored in the player's career),
    person_id, and player_name (which means the name of the player) \n\n Example 1: How many players started over 50 games? The SQL query would look like:
    SELECT COUNT(*) FROM careerstats WHERE gs > 50; \n Also, if you have to divide anything, make sure you only look at the rows where the row value in the denominator column is greater than 0. Do not use aany aliases either.
    For example: Who has the highest career offensive rebound percentage in the NBA today? Do not write a SQL query like this: SELECT player_name, oreb / reb FROM careerstats WHERE oreb / reb > 0.25 ORDER BY oreb / reb DESC LIMIT 1.
    Instead, The SQL query would look like:
    SELECT player_name, oreb / reb FROM careerstats WHERE reb > 0 ORDER BY oreb / reb DESC LIMIT 1;  
    Also, the sql query should not have ''' in the beginning or end, and shouldn't have the word sql in it."""
]

#Streamlit app

st.set_page_config(page_title="I can Retrieve Any SQL query")
st.header("Gemini App To Retrieve SQL Data")

question=st.text_input("Input: ",key="input")

submit=st.button("Ask the question")

# if submit is clicked
if submit:
    response=retrieve_response(question,prompt)
    print(response)
    response=retrieve_sql(response)
    st.subheader("The Response is")
    for row in response:
        print(row)
        st.header(row)

