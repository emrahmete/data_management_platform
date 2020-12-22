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

class PlayerTMarkt(Base):
    
    __table_args__ = {'extend_existing': True} 
    __tablename__ = 'playertmarkt'
    
    player_id= Column(Integer, primary_key=True)
    player_name = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    marketValue = Column(String)
    
    def __init__(self,player_id, player_name, first_name, last_name, marketValue):
        self.player_id = player_id
        self.player_name = player_name
        self.first_name = first_name
        self.last_name = last_name
        self.marketValue = marketValue