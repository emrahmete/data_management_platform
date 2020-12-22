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

class TrophiesCoach(Base):
    
    __table_args__ = {'extend_existing': True} 
    __tablename__ = 'coach_trophies'
    
    coach_id= Column(Integer, primary_key=True)
    league = Column(String)
    country = Column(String)
    season = Column(String)
    place = Column(String)
        
    def __init__(self, coach_id, league, country, season, place ):
        self.coach_id = coach_id
        self.league = league
        self.country = country
        self.season = season
        self.place = place