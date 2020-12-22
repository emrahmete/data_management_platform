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

class Seasons(Base):
    
    __table_args__ = {'extend_existing': True} 
    __tablename__ = 'seasons'
    
    season = Column(String, primary_key=True)
    
    def __init__(self,season):
        self.season = season