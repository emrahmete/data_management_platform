from Connection import Connection
from League import League
from Utils import Utils
import logging
import json
import requests
import sqlalchemy as db
import warnings as w
import os
import re
from sqlalchemy import (MetaData, Table, Column, Integer,Float,Date, select, literal, and_, exists,String)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship
from datetime import datetime
import sys
from multiprocessing import Pool
w.filterwarnings("ignore",category=Warning)


# In[2]:


logFile = r"logs/extr_standings_" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).replace(" ","_").replace("-","").replace(":","") + ".log"
logging.basicConfig(filename=logFile,level=logging.INFO)
con = Connection("mycon","oracle",20,"utf-8")
sess = con.create_oracle_session()


# In[3]:


headers = {
    'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
    'x-rapidapi-key': <rapidapi-key>
    }

standings_url_base = 'https://api-football-v1.p.rapidapi.com/v2/leagueTable/'

def get_standings_info(league_ids):
    url = standings_url_base + str(league_ids)
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return json.loads(response.content.decode('utf8'))
    else:
        return None


# In[4]:


def set_standings_info(standing_res,leagueid,ids):
    league_id = leagueid
    team_id = standing_res['api']['standings'][0][ids]['team_id']
    rank = standing_res['api']['standings'][0][ids]['rank']
    group = standing_res['api']['standings'][0][ids]['group']
    form = standing_res['api']['standings'][0][ids]['forme']
    status = standing_res['api']['standings'][0][ids]['status']
    description = standing_res['api']['standings'][0][ids]['description']
    all_matches_played = standing_res['api']['standings'][0][ids]['all']['matchsPlayed']
    all_win = standing_res['api']['standings'][0][ids]['all']['win']
    all_draw = standing_res['api']['standings'][0][ids]['all']['draw']
    all_lose = standing_res['api']['standings'][0][ids]['all']['lose']
    all_goals = standing_res['api']['standings'][0][ids]['all']['goalsFor']
    all_goals_against = standing_res['api']['standings'][0][ids]['all']['goalsAgainst']
    home_matches_played = standing_res['api']['standings'][0][ids]['home']['matchsPlayed']
    home_win = standing_res['api']['standings'][0][ids]['home']['win']
    home_draw = standing_res['api']['standings'][0][ids]['home']['draw']
    home_lose = standing_res['api']['standings'][0][ids]['home']['lose']
    home_goals = standing_res['api']['standings'][0][ids]['home']['goalsFor']
    home_goals_against = standing_res['api']['standings'][0][ids]['home']['goalsAgainst']
    away_matches_played = standing_res['api']['standings'][0][ids]['away']['matchsPlayed']
    away_win = standing_res['api']['standings'][0][ids]['away']['win']
    away_draw = standing_res['api']['standings'][0][ids]['away']['draw']
    away_lose = standing_res['api']['standings'][0][ids]['away']['lose']
    away_goals = standing_res['api']['standings'][0][ids]['away']['goalsFor']
    away_goals_against = standing_res['api']['standings'][0][ids]['all']['goalsAgainst']
    goal_diff = standing_res['api']['standings'][0][ids]['goalsDiff']
    points = standing_res['api']['standings'][0][ids]['points']
    last_update   = standing_res['api']['standings'][0][ids]['lastUpdate']
    standing = Standings(league_id, team_id, rank, group, form, status, description, all_matches_played, all_win, all_draw, all_lose, all_goals, all_goals_against,home_matches_played, home_win, home_draw, home_lose, home_goals, home_goals_against, away_matches_played, away_win, away_draw, away_lose, away_goals, away_goals_against, goal_diff, points, last_update)
    return standing
    


# In[5]:


#from datetime import datetime
def process_execution():
    #d1 = datetime.now()
    sess = con.create_oracle_session()
    league_list = list()
    league_response = get_league_info()
    result_cnt = int(league_response['api']['results'])
    for league_ids in range(0,result_cnt):
        try:
            league = set_league_info(league_response,league_ids)
            league_list.append(league)
        except Exception as e:
            logging.error(str(datetime.now()) + " - " + "league index:" + str(league_ids) + " #### Exception:  "+ str(e) +"  ##### response:" + str(league_response['api']['leagues'][league_ids]))
    sess.bulk_save_objects(league_list)
    sess.commit()
    sess.close()
    #d2 = datetime.now()
    #print(abs((d2 - d1).seconds))


# In[6]:


logging.info(str(datetime.now()) + " - Extraction has been started")
process_execution()
logging.info(str(datetime.now()) + " - Extraction has been finished")

