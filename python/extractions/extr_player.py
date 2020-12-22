#!/usr/bin/env python
# coding: utf-8

# In[1]:


from Connection import Connection
from Player import Player
from PlayerStats import PlayerStats
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
arg_batchSize = int(sys.argv[1])
arg_intervalStart = int(sys.argv[2])
arg_intervalEnd = int(sys.argv[3])
arg_process = int(sys.argv[4])

logFile = r"logs/extr_player_" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).replace(" ","_").replace("-","").replace(":","") + ".log"
logging.basicConfig(filename=logFile,level=logging.INFO)
con = Connection("mycon","oracle",20,"utf-8")
sess = con.create_oracle_session()


# In[3]:


headers = {
    'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
    'x-rapidapi-key': <rapidapi-key>
    }

players_url_base = 'https://api-football-v1.p.rapidapi.com/v2/players/player/'

def get_player_info(player_ids):
    url = players_url_base + str(player_ids)
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return json.loads(response.content.decode('utf8'))
    else:
        return None


# In[4]:


def set_player_info(player_request):
    player_id = player_request['api']['players'][0]['player_id']
    player_name = str(player_request['api']['players'][0]['player_name'])
    first_name = str(player_request['api']['players'][0]['firstname'])
    last_name = str(player_request['api']['players'][0]['lastname'])
    team_id = player_request['api']['players'][0]['team_id']
    position = str(player_request['api']['players'][0]['position'])
    nationality = str(player_request['api']['players'][0]['nationality'])
    ########
    if player_request['api']['players'][0]['birth_date'] is None:
        birth_date = None
    else:
        birth_date = datetime.strptime(player_request['api']['players'][0]['birth_date'],'%d/%m/%Y').date()
    ########  
    birth_place = str(player_request['api']['players'][0]['birth_place'])
    birth_country = str(player_request['api']['players'][0]['birth_country'])
    ########
    if player_request['api']['players'][0]['height'] is None:
        height = -1
    else:
        
        if (''.join(filter(lambda i: i.isdigit(), player_request['api']['players'][0]['height']))).isdigit():
            height = int(''.join(filter(lambda i: i.isdigit(), player_request['api']['players'][0]['height'])))
        else:
            height = -1          
    ########    
    if player_request['api']['players'][0]['weight'] is None:
        weight = -1
    else:
        
        if (''.join(filter(lambda i: i.isdigit(), player_request['api']['players'][0]['weight']))).isdigit():
            weight = int(''.join(filter(lambda i: i.isdigit(), player_request['api']['players'][0]['weight'])))
        else:
            weight = -1
            
    ########
    if player_request['api']['players'][0]['rating'] is None:
        rating=-1
    else:
        rating=player_request['api']['players'][0]['rating']   
    ########
    player = Player(player_id, player_name, first_name, last_name, team_id, position, nationality, birth_date, birth_place, birth_country, height, weight, rating )
    return player

def set_player_stats_info(player_request,ids):
    player_id = player_request['api']['players'][ids]['player_id']
    team_id = player_request['api']['players'][ids]['team_id']
    league = player_request['api']['players'][ids]['league']
    season = player_request['api']['players'][ids]['season']
    injured = player_request['api']['players'][ids]['injured']
    rating = player_request['api']['players'][ids]['rating']
    captain = player_request['api']['players'][ids]['captain']
    game_appearences = player_request['api']['players'][ids]['games']['appearences']
    games_minutes_played = player_request['api']['players'][ids]['games']['minutes_played']
    games_lineups = player_request['api']['players'][ids]['games']['lineups']
    substitutes_in = player_request['api']['players'][ids]['substitutes']['in']
    substitutes_out = player_request['api']['players'][ids]['substitutes']['out']
    substitutes_bench = player_request['api']['players'][ids]['substitutes']['bench']
    goals_total = player_request['api']['players'][ids]['goals']['total']
    assists = player_request['api']['players'][ids]['goals']['assists']
    conceded = player_request['api']['players'][ids]['goals']['conceded']
    saves = player_request['api']['players'][ids]['goals']['saves']
    passes_total = player_request['api']['players'][ids]['passes']['total']
    key_pass = player_request['api']['players'][ids]['passes']['key']
    pass_accuracy = player_request['api']['players'][ids]['passes']['accuracy']
    tackles_total = player_request['api']['players'][ids]['tackles']['total']
    tackles_blocks = player_request['api']['players'][ids]['tackles']['blocks']
    tackles_interceptions = player_request['api']['players'][ids]['tackles']['interceptions']
    duels_total = player_request['api']['players'][ids]['duels']['total']
    duels_won = player_request['api']['players'][ids]['duels']['won']
    dribbles_attempts = player_request['api']['players'][ids]['dribbles']['attempts']
    dribbles_success = player_request['api']['players'][ids]['dribbles']['success']
    fouls_drawn = player_request['api']['players'][ids]['fouls']['drawn']
    fouls_commited = player_request['api']['players'][ids]['fouls']['committed']
    shots_total = player_request['api']['players'][ids]['shots']['total']
    shots_on = player_request['api']['players'][ids]['shots']['on']
    penalty_won = player_request['api']['players'][ids]['penalty']['won']
    penalty_commited = player_request['api']['players'][ids]['penalty']['commited']
    penalty_success = player_request['api']['players'][ids]['penalty']['success']
    penalty_missed = player_request['api']['players'][ids]['penalty']['missed']
    penalty_saved = player_request['api']['players'][ids]['penalty']['saved']
    yellow_card = player_request['api']['players'][ids]['cards']['yellow']
    yellowred = player_request['api']['players'][ids]['cards']['yellowred']
    red_card = player_request['api']['players'][ids]['cards']['red']
    playerStats = PlayerStats(player_id,team_id,league,season,injured,rating,captain,game_appearences,games_minutes_played,games_lineups,substitutes_in,substitutes_out,substitutes_bench,goals_total,assists,conceded,saves,passes_total,key_pass,pass_accuracy,tackles_total,tackles_blocks,tackles_interceptions,duels_total,duels_won,dribbles_attempts,dribbles_success,fouls_drawn,fouls_commited,shots_total,shots_on,penalty_won,penalty_commited,penalty_success,penalty_missed,penalty_saved,yellow_card,yellowred,red_card)
    return playerStats

# In[5]:


#from datetime import datetime
def process_execution(ids):
    #d1 = datetime.now()
    sess = con.create_oracle_session()
    ply_list = list()
    for player_ids in ids:
        try:
            ply_stat = list()
            player_request = get_player_info(player_ids)
            player = set_player_info(player_request)
            ply_list.append(player)
            numOfRecs = player_request['api']['results']
            try: 
                for psids in range(0,numOfRecs):
                    playerStats = set_player_stats_info(player_request,psids)
                    ply_stat.append(playerStats)
                sess.bulk_save_objects(ply_stat)
            except Exception as e:
                logging.error(str(datetime.now()) + " - " + "player index:" + str(player_ids) + " #### Player Stats Exception:  "+ str(e) + "  ##### response:" + str(player_request))
        except Exception as e:
            logging.error(str(datetime.now()) + " - " + "player index:" + str(player_ids) + " #### Player Exception:  "+ str(e) + "  ##### response:" + str(player_request))
    sess.bulk_save_objects(ply_list)
    sess.commit()
    sess.close()
    #d2 = datetime.now()
    #print(abs((d2 - d1).seconds))


# In[6]:


logging.info(str(datetime.now()) + " - Extraction has been started")
utl = Utils()
process_pool = Pool(processes=arg_process)
executionList = utl.pool_partitioner(batchSize=arg_batchSize,intervalStart = arg_intervalStart, intervalEnd=arg_intervalEnd)
results = process_pool.map(process_execution,executionList)
logging.info(str(datetime.now()) + " - Extraction has been finished")