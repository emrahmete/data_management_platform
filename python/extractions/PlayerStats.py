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

class PlayerStats(Base):
    
    __table_args__ = {'extend_existing': True} 
    __tablename__ = 'player_stats'
    
    player_id               = Column(Integer, primary_key=True)
    team_id                 = Column(Integer, primary_key=True)
    league                  = Column(String,  primary_key=True)
    season                  = Column(String,  primary_key=True)
    injured                 = Column(String)
    rating                  = Column(Integer)
    captain                 = Column(Integer)
    game_appearences        = Column(Integer)
    games_minutes_played    = Column(Integer)
    games_lineups           = Column(Integer)
    substitutes_in          = Column(Integer)
    substitutes_out         = Column(Integer)
    substitutes_bench       = Column(Integer)
    goals_total             = Column(Integer)
    assists                 = Column(Integer)
    conceded                = Column(Integer)
    saves                   = Column(Integer)
    passes_total            = Column(Integer)
    key_pass                = Column(Integer)
    pass_accuracy           = Column(Integer)
    tackles_total           = Column(Integer)
    tackles_blocks          = Column(Integer)
    tackles_interceptions   = Column(Integer)
    duels_total             = Column(Integer)
    duels_won               = Column(Integer)
    dribbles_attempts       = Column(Integer)
    dribbles_success        = Column(Integer)
    fouls_drawn             = Column(Integer)
    fouls_commited          = Column(Integer)
    shots_total             = Column(Integer)
    shots_on                = Column(Integer)
    penalty_won             = Column(Integer)
    penalty_commited        = Column(Integer)
    penalty_success         = Column(Integer)
    penalty_missed          = Column(Integer)
    penalty_saved           = Column(Integer)
    yellow_card             = Column(Integer)
    yellowred               = Column(Integer)
    red_card                = Column(Integer)
    
    def __init__(self,player_id,team_id,league,season,injured,rating,captain,game_appearences,games_minutes_played,games_lineups,substitutes_in,substitutes_out,substitutes_bench,goals_total,assists,conceded,saves,passes_total,key_pass,pass_accuracy,tackles_total,tackles_blocks,tackles_interceptions,duels_total,duels_won,dribbles_attempts,dribbles_success,fouls_drawn,fouls_commited,shots_total,shots_on,penalty_won,penalty_commited,penalty_success,penalty_missed,penalty_saved,yellow_card,yellowred,red_card):
        self.player_id               = player_id
        self.team_id                 = team_id
        self.league                  = league
        self.season                  = season
        self.injured                 = injured
        self.rating                  = rating
        self.captain                 = captain
        self.game_appearences        = game_appearences
        self.games_minutes_played    = games_minutes_played
        self.games_lineups           = games_lineups
        self.substitutes_in          = substitutes_in
        self.substitutes_out         = substitutes_out
        self.substitutes_bench       = substitutes_bench
        self.goals_total             = goals_total
        self.assists                 = assists
        self.conceded                = conceded
        self.saves                   = saves
        self.passes_total            = passes_total
        self.key_pass                = key_pass
        self.pass_accuracy           = pass_accuracy
        self.tackles_total           = tackles_total
        self.tackles_blocks          = tackles_blocks
        self.tackles_interceptions   = tackles_interceptions
        self.duels_total             = duels_total
        self.duels_won               = duels_won
        self.dribbles_attempts       = dribbles_attempts
        self.dribbles_success        = dribbles_success
        self.fouls_drawn             = fouls_drawn
        self.fouls_commited          = fouls_commited
        self.shots_total             = shots_total
        self.shots_on                = shots_on
        self.penalty_won             = penalty_won
        self.penalty_commited        = penalty_commited
        self.penalty_success         = penalty_success
        self.penalty_missed          = penalty_missed
        self.penalty_saved           = penalty_saved
        self.yellow_card             = yellow_card
        self.yellowred               = yellowred
        self.red_card                = red_card