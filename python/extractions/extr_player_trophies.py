#!/usr/bin/env python
# coding: utf-8

# In[1]:


from Connection import Connection
from TrophiesPlayer import TrophiesPlayer
from Utils import Utils
import logging
import json
import requests
import sqlalchemy as db
import warnings as w
import os
import re
import sys
from sqlalchemy import (MetaData, Table, Column, Integer,Float,Date, select, literal, and_, exists,String)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship
from datetime import datetime
from multiprocessing import Pool
w.filterwarnings("ignore",category=Warning)


# In[2]:
arg_batchSize = int(sys.argv[1])
arg_intervalStart = int(sys.argv[2])
arg_intervalEnd = int(sys.argv[3])
arg_process = int(sys.argv[4])

logFile = r"logs/extr_player_trophies_" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).replace(" ","_").replace("-","").replace(":","") + ".log"
logging.basicConfig(filename=logFile,level=logging.INFO)
con = Connection("mycon","oracle",20,"utf-8")
sess = con.create_oracle_session()


# In[3]:


headers = {
    'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
    'x-rapidapi-key': <rapidapi-key>
    }

players_url_base = 'https://api-football-v1.p.rapidapi.com/v2/trophies/player/'

def get_player_trop_info(player_ids):
    url = players_url_base + str(player_ids)
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return json.loads(response.content.decode('utf8'))
    else:
        return None


# In[10]:


def set_player_trop_info(player_request,ids,player_ids):
    player_id = player_ids
    league = player_request['api']['trophies'][ids]['league']
    country = player_request['api']['trophies'][ids]['country']
    season = player_request['api']['trophies'][ids]['season']
    place = player_request['api']['trophies'][ids]['place']
    player_trop = TrophiesPlayer(player_id, league, country, season, place)
    return player_trop


# In[11]:


#from datetime import datetime
def process_execution(ids):
    #d1 = datetime.now()
    sess = con.create_oracle_session()
    for player_ids in ids:
        try:
            ply_list = list()
            player_request = get_player_trop_info(player_ids)
            numOfRecs = player_request['api']['results']
            try: 
                for psids in range(0,numOfRecs):
                    player_trop = set_player_trop_info(player_request,psids,player_ids)
                    ply_list.append(player_trop)
                sess.bulk_save_objects(ply_list)
            except Exception as e:
                logging.error(str(datetime.now()) + " - " + "player index:" + str(player_ids) + " #### Player Trophies Exception:  "+ str(e) + "  ##### response:" + str(player_request))
        except:
            logging.error(str(datetime.now()) + " - " + "player index:" + str(player_ids) + "##### response:" + str(player_request))
    sess.commit()
    sess.close()
    #d2 = datetime.now()
    #print(abs((d2 - d1).seconds))


# In[12]:


logging.info(str(datetime.now()) + " - Extraction has been started")
utl = Utils()
process_pool = Pool(processes=arg_process)
executionList = utl.pool_partitioner(batchSize=arg_batchSize,intervalStart = arg_intervalStart, intervalEnd=arg_intervalEnd)
results = process_pool.map(process_execution,executionList)
logging.info(str(datetime.now()) + " - Extraction has been finished")

