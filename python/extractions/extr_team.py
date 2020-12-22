#!/usr/bin/env python
# coding: utf-8

# In[4]:


from Connection import Connection
from Team import Team
from Utils import Utils
import logging
import json
import requests
import sqlalchemy as db
import warnings as w
import os
from sqlalchemy import (MetaData, Table, Column, Integer,Date, select, literal, and_, exists,String, TEXT)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship
from datetime import datetime
from multiprocessing import Pool
import sys
w.filterwarnings("ignore",category=Warning)


# In[5]:

arg_batchSize = int(sys.argv[1])
arg_intervalStart = int(sys.argv[2])
arg_intervalEnd = int(sys.argv[3])
arg_process = int(sys.argv[4])

logFile = r"logs/extr_team_" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).replace(" ","_").replace("-","").replace(":","") + ".log"
logging.basicConfig(filename=logFile,level=logging.INFO)
con = Connection("mycon","oracle",20,"utf-8")
sess = con.create_oracle_session()


# In[6]:


headers = {
    'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
    'x-rapidapi-key': <rapidapi-key>
    }

def get_team_info(team_ids):
    teams_url_base = 'https://api-football-v1.p.rapidapi.com/v2/teams/team/' + str(team_ids)
    response = requests.get(teams_url_base, headers=headers)
    if response.status_code == 200:
        return json.loads(response.content.decode('utf8'))
    else:
        return None


# In[7]:


def set_team_info(team_request):
    team_id = team_request['api']['teams'][0]['team_id']
    name = team_request['api']['teams'][0]['name']
    code = team_request['api']['teams'][0]['code']
    country = team_request['api']['teams'][0]['country']
    is_national = team_request['api']['teams'][0]['is_national']
    founded = team_request['api']['teams'][0]['founded']
    venue_name = team_request['api']['teams'][0]['venue_name']
    venue_surface = team_request['api']['teams'][0]['venue_surface']
    venue_address = team_request['api']['teams'][0]['venue_address']
    venue_city = team_request['api']['teams'][0]['venue_city']
    venue_capacity = team_request['api']['teams'][0]['venue_capacity']
    team = Team(team_id, name, code, country, is_national, founded, venue_name, venue_surface, venue_address, venue_city, venue_capacity)
    return team


# In[8]:


#from datetime import datetime
def process_execution(ids):
    #d1 = datetime.now()
    sess = con.create_oracle_session()
    team_list = list()
    for team_ids in ids:
        try:
            team_request = get_team_info(team_ids)
            team = set_team_info(team_request)
            team_list.append(team)
        except Exception as e:
            logging.error(str(datetime.now()) + " - " + "team index:" + str(team_ids) + " #### Exception:  "+ str(e) + "  ##### response:" + str(team_request))
    sess.bulk_save_objects(team_list)
    sess.commit()
    sess.close()
    #d2 = datetime.now()
    #print(abs((d2 - d1).seconds))


# In[9]:


logging.info(str(datetime.now()) + " - Extraction has been started")
utl = Utils()
process_pool = Pool(processes=arg_process)
executionList = utl.pool_partitioner(batchSize=arg_batchSize,intervalStart = arg_intervalStart, intervalEnd=arg_intervalEnd)
results = process_pool.map(process_execution,executionList)
logging.info(str(datetime.now()) + " - Extraction has been finished")


# In[ ]:




