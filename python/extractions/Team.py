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

class Team(Base):
    
    __table_args__ = {'extend_existing': True} 
    __tablename__ = 'team'
    
    team_id= Column(Integer, primary_key=True)
    name = Column(String)
    code = Column(String)
    country = Column(String)
    is_national = Column(String)
    founded = Column(String)
    venue_name = Column(String)
    venue_surface = Column(String)
    venue_address = Column(String)
    venue_city = Column(String)
    venue_capacity = Column(Integer)
    
    def __init__(self, team_id, name, code, country, is_national, founded, venue_name, venue_surface, venue_address, venue_city, venue_capacity):
        self.team_id = team_id
        self.name = name
        self.code = code
        self.country = country
        self.is_national = is_national
        self.founded = founded
        self.venue_name = venue_name
        self.venue_surface = venue_surface
        self.venue_address = venue_address
        self.venue_city = venue_city
        self.venue_capacity = venue_capacity