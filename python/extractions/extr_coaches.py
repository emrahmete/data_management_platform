from Connection import Connection
from Coaches import Coaches
from CoachesCareer import CoachesCareer
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
from multiprocessing import Pool
import sys
w.filterwarnings("ignore",category=Warning)


# In[2]:
arg_batchSize = int(sys.argv[1])
arg_intervalStart = int(sys.argv[2])
arg_intervalEnd = int(sys.argv[3])
arg_process = int(sys.argv[4])

logFile = r"logs/extr_coaches_" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).replace(" ","_").replace("-","").replace(":","") + ".log"
logging.basicConfig(filename=logFile,level=logging.INFO)
con = Connection("mycon","oracle",20,"utf-8")
sess = con.create_oracle_session()


# In[3]:


headers = {
    'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
    'x-rapidapi-key': <rapidapi-key>
    }

coaches_url_base = 'https://api-football-v1.p.rapidapi.com/v2/coachs/coach/'

def get_coach_info(coach_ids):
    url = coaches_url_base + str(coach_ids)
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return json.loads(response.content.decode('utf8'))
    else:
        return None


# In[4]:


def set_coach_info(coach_request):
    
    coach_id = int(coach_request['api']['coachs'][0]['id'])
    name = coach_request['api']['coachs'][0]['name']
    first_name = coach_request['api']['coachs'][0]['firstname']
    last_name = coach_request['api']['coachs'][0]['lastname']
    nationality = coach_request['api']['coachs'][0]['nationality']
    
    ########
    if coach_request['api']['coachs'][0]['birth_date'] is None:
        birth_date = None
    else:
        birth_date = datetime.strptime(coach_request['api']['coachs'][0]['birth_date'],'%d/%m/%Y').date()
    ########  
    
    birth_place = coach_request['api']['coachs'][0]['birth_place']
    birth_country = coach_request['api']['coachs'][0]['birth_country']
    
    ########
    if coach_request['api']['coachs'][0]['height'] is None:
        height = -1
    else:
        height = int(''.join(filter(lambda i: i.isdigit(), coach_request['api']['coachs'][0]['height'])))
    ########    
    if coach_request['api']['coachs'][0]['weight'] is None:
        weight = -1
    else:
        weight = int(''.join(filter(lambda i: i.isdigit(), coach_request['api']['coachs'][0]['weight'])))
    ########    
 
    coach = Coaches(coach_id, name, first_name, last_name, nationality, birth_date, birth_place, birth_country, height, weight)
    return coach


# In[5]:


def set_coach_carrer_info(coach_request,ids):
        
    coach_id = int(coach_request['api']['coachs'][0]['id'])
    team_id = coach_request['api']['coachs'][0]['career'][ids]['team']['id']
    start_date = coach_request['api']['coachs'][0]['career'][ids]['start']
    end_date = coach_request['api']['coachs'][0]['career'][ids]['end']
    
    coaches_career = CoachesCareer(coach_id, team_id, start_date, end_date)
    return coaches_career


# In[6]:


#from datetime import datetime
def process_execution(ids):
    #d1 = datetime.now()
    sess = con.create_oracle_session()
    chc_list = list()
    for coach_ids in ids:
        try:
            coach_career = list()
            coach_request = get_coach_info(coach_ids)
            coach = set_coach_info(coach_request)
            chc_list.append(coach)
            numOfRecs = len(coach_request['api']['coachs'][0]['career'])
            try: 
                for chids in range(0,numOfRecs):
                    ch_careeer = set_coach_carrer_info(coach_request,chids)
                    coach_career.append(ch_careeer)
                sess.bulk_save_objects(coach_career)
            except Exception as e:
                logging.error(str(datetime.now()) + " - " + "Coach index:" + str(coach_ids) + " #### Coach Career Exception:  "+ str(e) + "  ##### response:" + str(coach_request))
        except Exception as e:
            logging.error(str(datetime.now()) + " - " + "Coach index:" + str(coach_ids) + " #### Coach Exception:  "+ str(e) + "  ##### response:" + str(coach_request))
    sess.bulk_save_objects(chc_list)
    sess.commit()
    sess.close()
    #d2 = datetime.now()
    #print(abs((d2 - d1).seconds))


# In[32]:


logging.info(str(datetime.now()) + " - Extraction has been started")
utl = Utils()
process_pool = Pool(processes=25)
executionList = utl.pool_partitioner(batchSize=250,intervalStart = 7501, intervalEnd=12182)
results = process_pool.map(process_execution,executionList)
logging.info(str(datetime.now()) + " - Extraction has been finished")


# In[5]:


#player_request['api']['players'][0]['player_id']

#def pool_partitioner(self,batchSize,intervalStart,intervalEnd):
logging.info(str(datetime.now()) + " - Extraction has been started")
utl = Utils()
process_pool = Pool(processes=arg_process)
executionList = utl.pool_partitioner(batchSize=arg_batchSize,intervalStart = arg_intervalStart, intervalEnd=arg_intervalEnd)
results = process_pool.map(process_execution,executionList)
logging.info(str(datetime.now()) + " - Extraction has been finished")
