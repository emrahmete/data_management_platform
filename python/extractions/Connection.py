import json
import requests
import sqlalchemy as db
import warnings as w
import os
import re
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship


class Connection():
  
    def __init__(self,con_name,dbname,pool_size,encoding):
        os.environ['TNS_ADMIN']='/home/datascience/instantclient_19_5/network/admin/'
        os.environ['NLS_LANG']= 'TURKISH_TURKEY.AL32UTF8'
        self.con_name = con_name
        self.dbname = dbname
        self.pool_size= pool_size
        self.encoding = encoding
        self.engine = db.create_engine('oracle://<db_user_name>:<password>',convert_unicode=True,encoding=self.encoding,pool_size=self.pool_size)

    def create_oracle_session(self):
        Base = declarative_base()
        Session = sessionmaker(bind=self.engine)
        session = Session()
        return session

    def con_close(session):
        session.close()
        
    def con_commit(session):
        session.commit()
    
    def con_rollback(session):
        session.rollback()