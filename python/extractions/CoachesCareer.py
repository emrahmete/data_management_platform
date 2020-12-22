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

class CoachesCareer(Base):
    
    __table_args__ = {'extend_existing': True} 
    __tablename__ = 'coaches_career'
    
    coach_id = Column(Integer, primary_key=True)
    team_id = Column(Integer)
    start_date = Column(String)
    end_date = Column(String)
    
    def __init__(self,coach_id, team_id, start_date, end_date):
        self.coach_id = coach_id
        self.team_id = team_id
        self.start_date = start_date
        self.end_date = end_date




