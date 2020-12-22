#!/usr/bin/env python
# coding: utf-8

# ### OCI Data Science - Useful Tips
# Everything stored in the <span style="background-color: #d5d8dc ">/home/datascience</span> folder is now stored on your block volume drive. The <span style="background-color: #d5d8dc ">ads-examples</span> folder has moved outside of your working space. Notebook examples are now accessible through a Launcher tab "Notebook Examples" button.
# <details>
# <summary><font size="2">1. Check for Public Internet Access</font></summary>
# 
# ```python
# import requests
# response = requests.get("https://oracle.com")
# assert response.status_code==200, "Internet connection failed"
# ```
# </details>
# <details>
# <summary><font size="2">2. OCI Configuration and Key Files Set Up</font></summary><p>Follow the instructions in the getting-started notebook. That notebook is accessible via the "Getting Started" Launcher tab button.</p>
# </details>
# <details>
# <summary><font size="2">3. Helpful Documentation </font></summary>
# <ul><li><a href="https://docs.cloud.oracle.com/en-us/iaas/data-science/using/data-science.htm">Data Science Service Documentation</a></li>
# <li><a href="https://docs.cloud.oracle.com/iaas/tools/ads-sdk/latest/index.html">ADS documentation</a></li>
# </ul>
# </details>
# <details>
# <summary><font size="2">4. Typical Cell Imports and Settings</font></summary>
# 
# ```python
# %load_ext autoreload
# %autoreload 2
# %matplotlib inline
# 
# import warnings
# warnings.filterwarnings('ignore')
# 
# import logging
# logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.ERROR)
# 
# import ads
# from ads.dataset.factory import DatasetFactory
# from ads.automl.provider import OracleAutoMLProvider
# from ads.automl.driver import AutoML
# from ads.evaluations.evaluator import ADSEvaluator
# from ads.common.data import MLData
# from ads.explanations.explainer import ADSExplainer
# from ads.explanations.mlx_global_explainer import MLXGlobalExplainer
# from ads.explanations.mlx_local_explainer import MLXLocalExplainer
# from ads.catalog.model import ModelCatalog
# from ads.common.model_artifact import ModelArtifact
# ```
# </details>
# <details>
# <summary><font size="2">5. Useful Environment Variables</font></summary>
# 
# ```python
# import os
# print(os.environ["NB_SESSION_COMPARTMENT_OCID"])
# print(os.environ["PROJECT_OCID"])
# print(os.environ["USER_OCID"])
# print(os.environ["TENANCY_OCID"])
# print(os.environ["NB_REGION"])
# ```
# </details>

# In[1]:


import urllib
from bs4 import BeautifulSoup
import re


# In[2]:


class Player():

	def __init__(self, url):
		playerAttributes = {} #will store all the information in dictionnary

		opener = urllib.request.build_opener()
		opener.addheaders = [('User-agent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36')]
		inData = opener.open(url)
		content = inData.read()
		soup = BeautifulSoup( content, "html.parser")

		#retrieving picture url and basic name
		link = soup.find("div", {"class":"dataBild"})
		playerAttributes["Picture"] = link.img["src"]
		playerAttributes["Name"] = link.img["title"]

		#reading tabular info and storing
		for link in soup.find_all("table", {"class":"auflistung"}):
			for line in link.find_all("tr"):#, {"class" : "dataValue"}):
				text = re.sub("\r|\n|\t|\xa0|  ", "", line.text)
				lhs, rhs = text.split(":")
				if rhs:
					playerAttributes[lhs] = rhs

		#retrieving player value over career time graph and storing
		theXs = [ int(_)//1000 for _ in re.findall( b"'x':(\d+)", content)]
		theYs = [ int(_) for _ in re.findall( b"'y':(\d+)", content)]
		if theYs:
			value = theYs[-1]
			playerAttributes["Value (int)"] = value
			playerAttributes["Value Graph"] = zip(theXs, theYs)
			#putting actual player market value in printable form
			valueString = ""
			while value:
				nextVal = value // 1000
				if nextVal:
					valueString = "," + "%03d" %(value % 1000) + valueString
				else:
					valueString = "£%d" %(value % 1000) + valueString
				value = nextVal
			playerAttributes["Market Value"] = valueString
		self.playerAttributes = playerAttributes

	def __getitem__(self, arg):
		return self.playerAttributes[arg] if arg in self.playerAttributes else "-" #or "n/a"


# In[3]:


def research(playerName):
	try:
		baseUrl = "https://www.transfermarkt.co.uk/schnellsuche/ergebnis/schnellsuche?query="
		baseProfileUrl = "http://www.transfermarkt.co.uk"
		url = baseUrl + playerName
		#print(url)
		opener = urllib.request.build_opener()
		opener.addheaders = [('User-agent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36')]
		inData = opener.open(url)
		content = inData.read()
		#print(content)
		soup = BeautifulSoup( content, "html.parser") #use html.parser if lxml not installed
		dicPlayers = {}
	#	for link in soup.find_all("a", {"class" : "spielprofil_tooltip"}):
	#		dicPlayers[link.text] = Player(baseProfileUrl + link["href"])
	#
	#	for name, player in dicPlayers.items():
	#		#print(player["Name"], "\t", player["Age"],"\t", player["Current club"], "\t", player["Printable Value"],"\t", player["Position"])
	#		print("\t%-35s %2s %35s %13s\t%-30s" %(player["Name"], player["Age"], player["Current club"], player["Printable Value"], player["Position"]))
	#

		#quickerSearch
		dicUrls = {}
		dicAttributes = {}
		ind=0        
		for name, age, club, value in zip( soup.find_all("a", {"class":"spielprofil_tooltip"}), soup.find_all("td", class_ = "zentriert", text=re.compile("\d+")), soup.find_all("img", {"class":"tiny_wappen"}),soup.find_all("td", class_ = "rechts hauptlink")):
			#print( "\t%25s %2s %-35s %8s" %(name.text, age.text, club["alt"],value.text))
			dicUrls[ind] = baseProfileUrl + name["href"]
			dicAttributes[ind] = (name.text,age.text, club["alt"], value.text)
			ind=ind+1
		return dicUrls, dicAttributes
	except Exception as e:
		print(str(e))
		return None


# In[16]:


if __name__ == "__main__":
	while True:
		name = str( input("Enter name for search:\t"))
		#name ='Cristiano Ronaldo'
		output = research(name)
		if output:
			dicUrls, dicProperties = output
			for ind, (name, age, club, value)  in dicProperties.items():
				print( "\t%25s %2s %-35s %8s" %(name, age, club, value))


# In[19]:


if __name__ == "__main__":
	while True:
		name = str( input("Enter name for search:\t"))
		#name ='Cristiano Ronaldo'
		output = research(name)
		 print(output)
		if output:
			dicUrls, dicProperties = output
			for name, (age, club, value)  in dicProperties.items():
				print( "\t%25s %2s %-35s %8s" %(name, age, club, value))


# In[4]:


import sqlalchemy as db
import pandas as pd
import numpy as np
import os
import warnings as w
from Connection import Connection
from PlayerTMarkt import PlayerTMarkt
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
import unidecode
w.filterwarnings("ignore",category=Warning)

os.environ['TNS_ADMIN']='/home/datascience/blockstorage/instantclient_19_5/network/admin/'
engine = db.create_engine('oracle://ADMIN:102102102102Ce@dbml19c_high')


# In[10]:


con = Connection("mycon","oracle",20,"utf-8")
#sql= "SELECT player_id,player_name,first_name,last_name,replace(replace(CONVERT(first_name, 'US7ASCII', 'AL32UTF8'),' ','%20')|| '%20' || CONVERT(last_name, 'US7ASCII', 'AL32UTF8'),' ','%20') search, market_value from playertmarkt_2 where market_value = 'value' order by 1"
sql="""select player_id,
        player_name,first_name,last_name,position, t.name team,trunc((sysdate-birth_date)/365.25) age,nationality,
        convert(case when Rtrim(Substr(first_name,1,Instr(first_name,' '))) is null then first_name
        else Rtrim(Substr(first_name,1,Instr(first_name,' '))) end  || '%20'||
        case when Rtrim(Substr(last_name,1,Instr(last_name,' '))) is null then last_name
        else Rtrim(Substr(last_name,1,Instr(last_name,' '))) end , 'US7ASCII', 'AL32UTF8') search1, 
        convert(replace(player_name,' ','%20'),'US7ASCII', 'AL32UTF8') search2,
        convert(replace(last_name,' ','%20'),'US7ASCII', 'AL32UTF8') search3,
        convert(replace(first_name,' ','%20'),'US7ASCII', 'AL32UTF8') search4
        from player p, team t where p.team_id = t.team_id and player_id < 1001 order by 1"""
sess = con.create_oracle_session()


# In[12]:


x = pd.read_sql(con=con.engine,sql=sql)


# In[13]:


y = x.head(1000)
y.head()


# In[30]:


for index, row in y.iterrows():
   try:
       search = str(row['search1'])
       output = research(search)
       if all(output):
           dicUrls, dicProperties = output
           for ind, (name, age, club, value)  in dicProperties.items():
               if (int(age) == int(row['age'])) or (str(row['team']) in str(unidecode.unidecode(club))):
                   y.at[index,'market_value']=value
                   break
               else:
                   search = str(row['search2'])
                   output = research(search)
                   if all(output):
                       dicUrls, dicProperties = output
                       for ind, (name, age, club, value)  in dicProperties.items():
                           if (int(age) == int(row['age'])) or (str(row['team']) in str(unidecode.unidecode(club))):
                               y.at[index,'market_value']=value
                               break
                   
                       
       if not all(output):#search2
           search = str(row['search2'])
           output = research(search)
           if all(output):
               dicUrls, dicProperties = output
               for ind, (name, age, club, value)  in dicProperties.items():
                   if (int(age) == int(row['age'])) or (str(row['team']) in str(unidecode.unidecode(club))):
                       y.at[index,'market_value']=value
                       break
           else:#search3
               search = str(row['search3'])
               output = research(search)
               if all(output):
                   dicUrls, dicProperties = output
                   for ind, (name, age, club, value)  in dicProperties.items():
                       if ((int(age) == int(row['age'])) or (str(row['team']) in str(unidecode.unidecode(club)))):
                           y.at[index,'market_value']=value
                           break
               else:
                   search = str(row['search4'])
                   output = research(search)
                   if all(output):
                       dicUrls, dicProperties = output
                       for ind, (name, age, club, value)  in dicProperties.items():
                           if ((int(age) == int(row['age'])) or (str(row['team']) in str(unidecode.unidecode(club)))):
                               y.at[index,'market_value']=value
                               break
               
   except Exception as e:
       print(str(e))


# In[14]:


for index, row in y.iterrows():
    find = 0
    try:
        search = str(row['search1'])
        output = research(search)
        
        if all(output):
            dicUrls, dicProperties = output
            for ind, (name, age, club, value)  in dicProperties.items():
                if (int(age) == int(row['age'])) or (str(row['team']) in str(unidecode.unidecode(club))):
                    y.at[index,'market_value']=value
                    find = 1
                    break
        
        if find == 0:
            search = str(row['search2'])
            output = research(search)
            if all(output):
                dicUrls, dicProperties = output
                for ind, (name, age, club, value)  in dicProperties.items():
                    if (int(age) == int(row['age'])) or (str(row['team']) in str(unidecode.unidecode(club))):
                        y.at[index,'market_value']=value
                        find = 1
                        break
        
        if find == 0:
            search = str(row['search3'])
            output = research(search)
            if all(output):
                dicUrls, dicProperties = output
                for ind, (name, age, club, value)  in dicProperties.items():
                    if (int(age) == int(row['age'])) or (str(row['team']) in str(unidecode.unidecode(club))):
                        y.at[index,'market_value']=value
                        find = 1
                        break
                        
        if find == 0:
            search = str(row['search4'])
            output = research(search)
            if all(output):
                dicUrls, dicProperties = output
                for ind, (name, age, club, value)  in dicProperties.items():
                    if (int(age) == int(row['age'])) or (str(row['team']) in str(unidecode.unidecode(club))):
                        y.at[index,'market_value']=value
                        find = 1
                        break
    
    except Exception as e:
        print(str(e))


# In[9]:


y


# #######################

# In[9]:


y.drop('search',axis='columns', inplace=True)
y.drop('position',axis='columns', inplace=True)
y.drop('team',axis='columns', inplace=True)
y.drop('age',axis='columns', inplace=True)
y.drop('nationality',axis='columns', inplace=True)
y.reset_index(drop=True, inplace=True)


# In[81]:


y.head(15)


# In[96]:


unidecode.unidecode('Atlético Madrid')


# In[15]:


y.to_sql('playertmarkt3', con=con.engine,if_exists='append',index=False)


# In[95]:


if "Atletico Madrid" in unaccented_string: 
    print('go')


# In[ ]:





# In[ ]:


#https://ggrahambaker.github.io/blog/2019/06/11/webscraping-transfermarkt
import pandas as pd
from bs4 import BeautifulSoup
import requests
headers = {'User-Agent': 
           'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}

page = 'https://www.transfermarkt.co.uk/premier-league/startseite/wettbewerb/GB1/plus/?saison_id=2018'

tree = requests.get(page, headers = headers)
#print(tree)
soup = BeautifulSoup(tree.content, 'html.parser')


# In[4]:


def build_by_year(year):
    page = 'https://www.transfermarkt.co.uk/premier-league/startseite/wettbewerb/GB1/plus/?saison_id=' + year
    tree = requests.get(page, headers = headers)
    soup = BeautifulSoup(tree.content, 'html.parser')
    spending_table = soup.select("tbody")[1]
    names = soup.select("td.hide-for-pad > a.vereinprofil_tooltip")
    season = []
    for name in names:
        temp = []
        temp_name = name.text.rstrip('FC').lower().rstrip()
        temp.append(temp_name)
        temp.append(name.get('href'))
        season.append(temp)
    return season


# In[10]:


def get_all_transfer_info_year(url, year):
    date_dict = {
    '19/20': '2019',    
    '18/19': '2018',
    '17/18': '2017',
    '16/17': '2016',
    '15/16': '2015',
    '14/15': '2014',
    '13/14': '2013',
    '12/13': '2012',
    '11/12': '2011',
    '10/11': '2010',
    '09/10': '2009',
    '08/09': '2008',
    '07/08': '2007',
    '06/07': '2006',
    '05/06': '2005',
    '04/05': '2004'}
    year_to_ret = ''
    for dash, whole in date_dict.items():
        if year == whole:
            year_to_ret = dash
    fixed_url = url.replace("startseite", "alletransfers")[1:]
    full_url = 'https://www.transfermarkt.co.uk/' + fixed_url 
    tree = requests.get(full_url, headers = headers)
    s = BeautifulSoup(tree.content, 'html.parser')
    incoming = s.select("td.redtext")
    in_and_out = []
    for idx, spent in enumerate(incoming):
        for parent in incoming[idx].parents:
            if parent.get('class') == ['box']:
                split = parent.text.split()
                if split[0] == 'Arrivals' and split[1] == year_to_ret:
                    in_and_out = [spent.text, '']
                    break
            
        # if we got here, we didnt find it!
    if not bool(in_and_out):
        in_and_out = ['0', '']
    outgoing = s.select("td.greentext")    
    for idx, sale in enumerate(outgoing):
        for parent in outgoing[idx].parents:
            if parent.get('class') == ['box']:
                split = parent.text.split()
                if split[0] == 'Departures' and split[1] == year_to_ret:
                    in_and_out[1] = sale.text
    
    if in_and_out[1] == '':
        in_and_out[1] =  '0'
    return in_and_out


# In[11]:


def team_lookup(name):
    team_list = [
        'bournemouth',
        'arsenal', 
        'aston villa', 
        'birmingham city', 
        'blackburn rovers',
        'blackpool',
        'bolton wanderers', 
        'brighton hove albion', 
        'burnley', 
        'cardiff city', 
        'charlton athletic', 
        'chelsea', 
        'crystal palace', 
        'derby county', 
        'everton', 
        'fulham', 
        'huddersfield town',
        'hull city',
        'leicester city', 
        'liverpool', 
        'manchester city', 
        'manchester united', 
        'middlesbrough', 
        'newcastle united', 
        'norwich city', 
        'portsmouth', 
        'queens park rangers', 
        'reading', 
        'sheffield united', 
        'southampton', 
        'stoke city', 
        'sunderland',
        'swansea city',
        'tottenham hotspur', 
        'watford', 
        'west bromwich albion', 
        'west ham united', 
        'wigan athletic', 
        'wolverhampton wanderers'
    ]
    max_score = 0
    name_to_ret = ''
    for team in team_list:
        temp_score = fuzz.ratio(name, team)
        if temp_score > max_score:
            max_score = temp_score
            name_to_ret = team
        
    return name_to_ret


# In[12]:


build_by_year('2018')


# In[13]:


import http.client

conn = http.client.HTTPSConnection("v3.football.api-sports.io")

headers = {
    'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': "XxXxXxXxXxXxXxXxXxXxXxXx"
    }

conn.request("GET", "/players?id=276&season=2019", headers=headers)

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))


# In[14]:


import http.client

conn = http.client.HTTPSConnection("v3.football.api-sports.io")

headers = {
    'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': "746c5f597571966a72de1dc0e66282e0"
    }

conn.request("GET", "/players?id=276&season=2019", headers=headers)

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))


# In[ ]:




