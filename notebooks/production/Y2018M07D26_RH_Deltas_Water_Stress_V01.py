
# coding: utf-8

# In[1]:

""" Calculate water stress with raw, ma10 and ols10 at subbasin level.
-------------------------------------------------------------------------------

Update Y2020M02D06 limit to [0-1], output version increase 2-3 


The tresholds per month will be used to set waterstress to 1 before doing a
regression. In order to determine if a subbasin is arid and lowwater use, 
a full range regression ols1960-2014 for riverdischarge and ptotww and ptotwn
will be used. 

Author: Rutger Hofste
Date: 20180604
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.
    DATABASE_ENDPOINT (string) : RDS or postGreSQL endpoint.
    DATABASE_NAME (string) : Database name.
    TABLE_NAME_AREA_30SPFAF06 (string) : Table name used for areas. Must exist
        on same database as used in rest of script.
    S3_INPUT_PATH_RIVERDISCHARGE (string) : AWS S3 input path for 
        riverdischarge.    
    S3_INPUT_PATH_DEMAND (string) : AWS S3 input path for 
        demand.     

"""

TESTING = 0
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M07D26_RH_Deltas_Water_Stress_V01'
OUTPUT_VERSION = 3

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
INPUT_TABLE_NAME = 'y2018m07d26_rh_deltas_arid_lowwateruse_v02_v01'
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)

print("Input Table: " , INPUT_TABLE_NAME, 
      "\nOutput Table: " , OUTPUT_TABLE_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

# imports
import re
import os
import numpy as np
import pandas as pd
import aqueduct3
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)


# In[4]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
#connection = engine.connect()

if OVERWRITE_OUTPUT:
    sql = text("DROP TABLE IF EXISTS {};".format(OUTPUT_TABLE_NAME))
    result = engine.execute(sql)


# In[5]:

temporal_reducers = ["","ma10_","ols10_","capped_ols10_"]
if TESTING:
    temporal_reducers = [""]


# In[6]:

"""
Calculates Water Stress 

totww / (riverdischarge+totwn)

Exceptions:
    when aridandlowwateruse 
        water stress = 1
    else 
        ws = totww / (riverdischarge+totwn)
"""


sql = "CREATE TABLE {} AS".format(OUTPUT_TABLE_NAME)
sql +=  " SELECT *,"
for temporal_reducer in temporal_reducers:   
    sql += " CASE when {}ptotww_m_30spfaf06 IS NULL OR {}riverdischarge_m_30spfaf06 <= 0".format(temporal_reducer,temporal_reducer)
    sql += " THEN NULL else"
    sql += " GREATEST(0,LEAST(1,{}ptotww_m_30spfaf06 / {}riverdischarge_m_30spfaf06))".format(temporal_reducer,temporal_reducer,temporal_reducer)
    sql += " END"
    sql += " AS {}waterstress_dimensionless_30spfaf06 ,".format(temporal_reducer)
    
    sql += " CASE when {}ptotww_m_30spfaf06 IS NULL OR {}riverdischarge_m_30spfaf06 <=0".format(temporal_reducer,temporal_reducer,temporal_reducer)
    sql += " THEN NULL else"
    sql += " GREATEST(0,LEAST(1,{}ptotwn_m_30spfaf06 / {}riverdischarge_m_30spfaf06))".format(temporal_reducer,temporal_reducer,temporal_reducer)
    sql += " END"
    sql += " AS {}waterdepletion_dimensionless_30spfaf06,".format(temporal_reducer)

sql = sql[:-1]
sql += " FROM {}".format(INPUT_TABLE_NAME)

if TESTING:
    sql += " LIMIT 100"
    


# In[7]:

print(sql)


# In[8]:

result = engine.execute(sql)


# In[9]:

sql_index = "CREATE INDEX {}delta_id ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"delta_id")


# In[10]:

print(sql_index)


# In[11]:

result = engine.execute(sql_index)


# In[12]:

engine.dispose()


# In[13]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:02.265367  
# 0:00:02.258329

# In[ ]:



