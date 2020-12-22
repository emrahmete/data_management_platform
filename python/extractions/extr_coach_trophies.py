#!/usr/bin/env python
# coding: utf-8

# In[1]:


from Connection import Connection
from TrophiesCoach import TrophiesCoach
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

logFile = r"logs/extr_coach_trophies_" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).replace(" ","_").replace("-","").replace(":","") + ".log"
logging.basicConfig(filename=logFile,level=logging.INFO)
con = Connection("mycon","oracle",20,"utf-8")
sess = con.create_oracle_session()


# In[3]:


headers = {
    'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
    'x-rapidapi-key': <rapidapi-key>
    }

coach_url_base = 'https://api-football-v1.p.rapidapi.com/v2/trophies/coach/'

def get_coach_trop_info(coach_ids):
    url = coach_url_base + str(coach_ids)
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return json.loads(response.content.decode('utf8'))
    else:
        return None


# In[10]:


def set_coach_trop_info(coach_request,ids,coach_ids):
    coach_id = coach_ids
    league = coach_request['api']['trophies'][ids]['league']
    country = coach_request['api']['trophies'][ids]['country']
    season = coach_request['api']['trophies'][ids]['season']
    place = coach_request['api']['trophies'][ids]['place']
    coach_trop = TrophiesCoach(coach_id, league, country, season, place)
    return coach_trop


# In[11]:


#from datetime import datetime
def process_execution(ids):
    #d1 = datetime.now()
    sess = con.create_oracle_session()
    for coach_ids in ids:
        try:
            chc_list = list()
            coach_request = get_coach_trop_info(coach_ids)
            numOfRecs = coach_request['api']['results']
            try: 
                for chids in range(0,numOfRecs):
                    coach_trop = set_coach_trop_info(coach_request,chids,coach_ids)
                    chc_list.append(coach_trop)
                sess.bulk_save_objects(chc_list)
            except Exception as e:
                logging.error(str(datetime.now()) + " - " + "coach index:" + str(coach_ids) + " #### Coach Trophies Exception:  "+ str(e) + "  ##### response:" + str(coach_request))
        except:
            logging.error(str(datetime.now()) + " - " + "Coach index:" + str(coach_ids) + "##### response:" + str(coach_request))
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

