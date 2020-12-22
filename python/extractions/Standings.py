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

class Standings(Base):
    
    __table_args__ = {'extend_existing': True} 
    __tablename__ = 'standings'
    
    league_id     = Column(Integer, primary_key=True)
    team_id       = Column(Integer, primary_key=True)
    rank          = Column(Integer)
    group         = Column(String)
    form          = Column(String)
    status        = Column(String)
    description   = Column(String)
    all_matches_played = Column(Integer)
    all_win      = Column(Integer)
    all_draw     = Column(Integer)
    all_lose     = Column(Integer)
    all_goals    = Column(Integer)
    all_goals_against = Column(Integer)
    home_matches_played = Column(Integer)
    home_win      = Column(Integer)
    home_draw     = Column(Integer)
    home_lose     = Column(Integer)
    home_goals    = Column(Integer)
    home_goals_against = Column(Integer)
    away_matches_played = Column(Integer)
    away_win      = Column(Integer)
    away_draw     = Column(Integer)
    away_lose     = Column(Integer)
    away_goals    = Column(Integer)
    away_goals_against = Column(Integer)
    goal_diff     = Column(Integer)
    points        = Column(Integer)
    last_update   = Column(String)
    
    def __init__(self,league_id, team_id, rank, group, form, status, description, all_matches_played, all_win, all_draw, all_lose, all_goals, all_goals_against,home_matches_played, home_win, home_draw, home_lose, home_goals, home_goals_against, away_matches_played, away_win, away_draw, away_lose, away_goals, away_goals_against, goal_diff, points, last_update):
        self.league_id = league_id
        self.team_id = team_id
        self.rank = rank
        self.group = group
        self.form = form
        self.status = status
        self.description = description        
        self.all_matches_played = all_matches_played
        self.all_win = all_win
        self.all_draw = all_draw
        self.all_lose = all_lose
        self.all_goals = all_goals
        self.all_goals_against = all_goals_against      
        self.home_matches_played = home_matches_played
        self.home_win = home_win
        self.home_draw = home_draw
        self.home_lose = home_lose
        self.home_goals = home_goals
        self.home_goals_against = home_goals_against
        self.away_matches_played = away_matches_played
        self.away_win = away_win
        self.away_draw = away_draw
        self.away_lose = away_lose
        self.away_goals = away_goals
        self.away_goals_against = away_goals_against
        self.goal_diff = goal_diff
        self.points = points
        self.last_update = last_update
        
        