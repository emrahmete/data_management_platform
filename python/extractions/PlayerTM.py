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

Base = declarative_base()

class Player(Base):
    
    __table_args__ = {'extend_existing': True} 
    __tablename__ = 'player_tm'
    
    player_id= Column(Integer, primary_key=True)
    player_name = Column(String)
    team = Column(String)
    position = Column(String)
    nationality = Column(String)
    birth_date = Column(Date)
    birth_place = Column(String)
    height = Column(Integer)
    currentMarketValue = Column(String)
    marketValueLastUpdate = Column(Date)
    maxMarketValue = Column(String)
    maxMarketValueDate = Column(Date)
    
    def __init__(self,player_id, player_name, team, position, nationality, birth_date, birth_place, nationality, height, currentMarketValue, maxMarketValue, maxMarketValueDate):
        self.player_id = player_id
        self.player_name = player_name
        self.team = team
        self.position = position
        self.nationality = nationality
        self.birth_date = birth_date
        self.birth_place = birth_place
        self.height = height
        self.currentMarketValue = currentMarketValue
        self.marketValueLastUpdate = marketValueLastUpdate
        self.maxMarketValue = maxMarketValue
        self.maxMarketValueDate = maxMarketValueDate