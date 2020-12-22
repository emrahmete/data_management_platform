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

class Coaches(Base):
    
    __table_args__ = {'extend_existing': True} 
    __tablename__ = 'coaches'
    
    coach_id= Column(Integer, primary_key=True)
    name = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    nationality = Column(String)
    birth_date = Column(Date)
    birth_place = Column(String)
    birth_country = Column(String)
    height = Column(Integer)
    weight = Column(Integer)
    
    def __init__(self, coach_id, name, first_name, last_name, nationality, birth_date, birth_place, birth_country, height, weight):
        self.coach_id = coach_id
        self.name = name
        self.first_name = first_name
        self.last_name = last_name
        self.nationality = nationality
        self.birth_date = birth_date
        self.birth_place = birth_place
        self.birth_country = birth_country
        self.height = height
        self.weight = weight