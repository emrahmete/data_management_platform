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
    __tablename__ = 'player'
    
    player_id= Column(Integer, primary_key=True)
    player_name = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    team_id = Column(Integer)
    position = Column(String)
    nationality = Column(String)
    birth_date = Column(Date)
    birth_place = Column(String)
    birth_country = Column(String)
    height = Column(Integer)
    weight = Column(Integer)
    rating = Column(Float)
    
    def __init__(self,player_id, player_name, first_name, last_name, team_id, position, nationality, birth_date, birth_place, birth_country, height, weight, rating ):
        self.player_id = player_id
        self.player_name = player_name
        self.first_name = first_name
        self.last_name = last_name
        self.team_id = team_id
        self.position = position
        self.nationality = nationality
        self.birth_date = birth_date
        self.birth_place = birth_place
        self.birth_country = birth_country
        self.height = height
        self.weight = weight
        self.rating = rating