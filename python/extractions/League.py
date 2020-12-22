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

class League(Base):
    
    __table_args__ = {'extend_existing': True} 
    __tablename__ = 'league'
    
    league_id= Column(Integer, primary_key=True)
    name = Column(String)
    type = Column(String)
    country = Column(String)
    country_code = Column(String)
    season = Column(String)
    season_start = Column(Date)
    season_end = Column(Date)
    standings = Column(Integer)
    is_current = Column(Integer)
    
    
    def __init__(self,league_id,name,type,country,country_code,season,season_start,season_end,standings,is_current):
        self.league_id = league_id
        self.name = name
        self.type = type
        self.country = country
        self.country_code = country_code
        self.season = season
        self.season_start = season_start
        self.season_end = season_end
        self.standings = standings
        self.is_current = is_current