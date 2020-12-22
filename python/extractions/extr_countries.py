#!/usr/bin/env python
# coding: utf-8

# In[1]:


from Connection import Connection
from Countries import Countries
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


logFile = r"logs/extr_countries_" + str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).replace(" ","_").replace("-","").replace(":","") + ".log"
logging.basicConfig(filename=logFile,level=logging.INFO)
con = Connection("mycon","oracle",20,"utf-8")
sess = con.create_oracle_session()


# In[3]:


headers = {
    'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
    'x-rapidapi-key': <rapidapi-key>
    }

players_url_base = 'https://api-football-v1.p.rapidapi.com/v2/countries'

def get_countries_info():
    url = players_url_base
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return json.loads(response.content.decode('utf8'))
    else:
        return None


# In[4]:


def set_countries_info(countries_respone,ids):
    country = countries_respone['api']['countries'][ids]['country']
    code = countries_respone['api']['countries'][ids]['code']
    countries = Countries(country,code)
    return countries
    


# In[5]:


#from datetime import datetime
def process_execution():
    #d1 = datetime.now()
    sess = con.create_oracle_session()
    countries_list = list()
    countries_response = get_countries_info()
    result_cnt = int(countries_response['api']['results'])
    for countries_ids in range(0,result_cnt):
        try:
            country = set_countries_info(countries_response,countries_ids)
            countries_list.append(country)
        except Exception as e:
            logging.error(str(datetime.now()) + " - " + "country index:" + str(countries_ids) + " #### Exception:  "+ str(e) +"  ##### response:" + str(countries_response['api']['countries'][countries_ids]))
    sess.bulk_save_objects(countries_list)
    sess.commit()
    sess.close()
    #d2 = datetime.now()
    #print(abs((d2 - d1).seconds))


# In[6]:


logging.info(str(datetime.now()) + " - Extraction has been started")
process_execution()
logging.info(str(datetime.now()) + " - Extraction has been finished")

