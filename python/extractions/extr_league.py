#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


logFile = r"logs/extr_league_" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).replace(" ","_").replace("-","").replace(":","") + ".log"
logging.basicConfig(filename=logFile,level=logging.INFO)
con = Connection("mycon","oracle",20,"utf-8")
sess = con.create_oracle_session()


# In[3]:


headers = {
    'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
    'x-rapidapi-key': <rapidapi-key>
    }

players_url_base = 'https://api-football-v1.p.rapidapi.com/v2/leagues'

def get_league_info():
    url = players_url_base
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return json.loads(response.content.decode('utf8'))
    else:
        return None


# In[4]:


def set_league_info(legaue_respone,ids):
    league_id = legaue_respone['api']['leagues'][ids]['league_id']
    name = legaue_respone['api']['leagues'][ids]['name']
    type = legaue_respone['api']['leagues'][ids]['type']
    country = legaue_respone['api']['leagues'][ids]['country']
    country_code = legaue_respone['api']['leagues'][ids]['country_code']
    season = legaue_respone['api']['leagues'][ids]['season']
    
    ########
    if legaue_respone['api']['leagues'][ids]['season_start'] is None or legaue_respone['api']['leagues'][ids]['season_start'] == '':
        season_start = None
    else:
        season_start = datetime.strptime(legaue_respone['api']['leagues'][ids]['season_start'],'%Y-%m-%d').date()
    ########
    
    ########
    if legaue_respone['api']['leagues'][ids]['season_end'] is None or legaue_respone['api']['leagues'][ids]['season_end'] == '':
        season_end = None
    else:
        season_end = datetime.strptime(legaue_respone['api']['leagues'][ids]['season_end'],'%Y-%m-%d').date()
    ########
    standings = legaue_respone['api']['leagues'][ids]['standings']
    is_current = legaue_respone['api']['leagues'][ids]['is_current']
    
    league = League(league_id,name,type,country,country_code,season,season_start,season_end,standings,is_current)
    return league
    


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

