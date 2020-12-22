#!/usr/bin/env python
# coding: utf-8

# In[1]:


from Connection import Connection
from Seasons import Seasons
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


logFile = r"logs/extr_seasons_" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).replace(" ","_").replace("-","").replace(":","") + ".log"
logging.basicConfig(filename=logFile,level=logging.INFO)
con = Connection("mycon","oracle",20,"utf-8")
sess = con.create_oracle_session()


# In[3]:


players_url_base = "https://api-football-v1.p.rapidapi.com/v2/seasons"

headers = {
    'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
    'x-rapidapi-key': <rapidapi-key>
    }

def get_seasons_info():
    url = players_url_base
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return json.loads(response.content.decode('utf8'))
    else:
        return None


# In[4]:


def set_seasons_info(seasons_respone,ids):
    season = seasons_respone['api']['seasons'][ids]
    seasons = Seasons(season)
    return seasons
    


# In[5]:


#from datetime import datetime
def process_execution():
    #d1 = datetime.now()
    sess = con.create_oracle_session()
    seasons_list = list()
    seasons_response = get_seasons_info()
    result_cnt = int(seasons_response['api']['results'])
    for seasons_ids in range(0,result_cnt):
        try:
            season = set_seasons_info(seasons_response,seasons_ids)
            seasons_list.append(season)
        except Exception as e:
            logging.error(str(datetime.now()) + " - " + "season index:" + str(seasons_ids) + " #### Exception:  "+ str(e) +"  ##### response:" + str(seasons_response['api']['seasons'][seasons_ids]))
    sess.bulk_save_objects(seasons_list)
    sess.commit()
    sess.close()
    #d2 = datetime.now()
    #print(abs((d2 - d1).seconds))


# In[6]:


logging.info(str(datetime.now()) + " - Extraction has been started")
process_execution()
logging.info(str(datetime.now()) + " - Extraction has been finished")

