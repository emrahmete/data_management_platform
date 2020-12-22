import urllib
from bs4 import BeautifulSoup
import re

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

arg_intervalStart = int(sys.argv[1])
arg_intervalEnd = int(sys.argv[2])

os.environ['TNS_ADMIN']='/home/datascience/instantclient_19_5/network/admin/'
engine = db.create_engine('oracle://ADMIN:102102102102Ce@dbml19c_high')

con = Connection("mycon","oracle",20,"utf-8")
#sql= "SELECT player_id,player_name,first_name,last_name,replace(replace(CONVERT(first_name, 'US7ASCII', 'AL32UTF8'),' ','%20')|| '%20' || CONVERT(last_name, 'US7ASCII', 'AL32UTF8'),' ','%20') search, market_value from playertmarkt_2 where market_value = 'value' order by 1"
sql="""select player_id,
        player_name,first_name,last_name,position, case when t.name = 'Paris Saint Germain' then 'Paris Saint-Germain' else t.name end team,
        trunc((trunc(sysdate)-trunc(birth_date))/365.25) age,nationality,
        convert(case when Rtrim(Substr(first_name,1,Instr(first_name,' '))) is null then first_name
        else Rtrim(Substr(first_name,1,Instr(first_name,' '))) end  || '%20'||
        case when Rtrim(Substr(last_name,1,Instr(last_name,' '))) is null then last_name
        else Rtrim(Substr(last_name,1,Instr(last_name,' '))) end , 'US7ASCII', 'AL32UTF8') search1, 
        convert(replace(player_name,' ','%20'),'US7ASCII', 'AL32UTF8') search2,
        convert(replace(last_name,' ','%20'),'US7ASCII', 'AL32UTF8') search3,
        convert(replace(first_name,' ','%20'),'US7ASCII', 'AL32UTF8') search4
        from player p, team t where p.team_id = t.team_id and player_name <> 'Data not available' and 
        player_id >"""+ str(arg_intervalStart) + """ 
        and player_id <="""+ str(arg_intervalEnd) +  """ 
        order by 1"""

print(sql)

sess = con.create_oracle_session()

x = pd.read_sql(con=con.engine,sql=sql)

y = x

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
		print(url)
		return None
        

for index, row in y.iterrows():
    find = 0
    try:
        search = str(row['search1'])
        output = research(search)
        
        if all(output):
            dicUrls, dicProperties = output
            for ind, (name, age, club, value)  in dicProperties.items():
                if (int(age) == int(row['age'])) or (str(row['team']).lower() in str(unidecode.unidecode(club))):
                    y.at[index,'market_value']=value
                    find = 1
                    break
        
        if find == 0:
            search = str(row['search2'])
            output = research(search)
            if all(output):
                dicUrls, dicProperties = output
                for ind, (name, age, club, value)  in dicProperties.items():
                    if (int(age) == int(row['age'])) or (str(row['team']).lower() in str(unidecode.unidecode(club))):
                        y.at[index,'market_value']=value
                        find = 1
                        break
        
        if find == 0:
            search = str(row['search3'])
            output = research(search)
            if all(output):
                dicUrls, dicProperties = output
                for ind, (name, age, club, value)  in dicProperties.items():
                    if (int(age) == int(row['age'])) or (str(row['team']).lower() in str(unidecode.unidecode(club))):
                        y.at[index,'market_value']=value
                        find = 1
                        break
                        
        if find == 0:
            search = str(row['search4'])
            output = research(search)
            if all(output):
                dicUrls, dicProperties = output
                for ind, (name, age, club, value)  in dicProperties.items():
                    if (int(age) == int(row['age'])) or (str(row['team']).lower() in str(unidecode.unidecode(club))):
                        y.at[index,'market_value']=value
                        find = 1
                        break
    
    except Exception as e:
        None
        
con2 = Connection("mycon","oracle",20,"utf-8")
y.to_sql('playertmarkt3', con=con2.engine,if_exists='append',index=False)