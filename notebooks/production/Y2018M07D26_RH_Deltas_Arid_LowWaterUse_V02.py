
# coding: utf-8

# In[1]:

""" Add column for arid, lowwateruse and aridandlowwateruse for each subbasin for deltas. 
-------------------------------------------------------------------------------

This script has been edited on 20180625 to take into account the newly
columns based on stats such as moving averga and ols.

The script will create arid and low water use columns for the 'raw' values,
moving average values and linear regression.


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
SCRIPT_NAME = 'Y2018M07D26_RH_Deltas_Arid_LowWaterUse_V02'
OUTPUT_VERSION = 1

THRESHOLD_ARID_YEAR = 0.03 #units are m/year, threshold defined by Aqueduct 2.1
THRESHOLD_LOW_WATER_USE_YEAR = 0.012 #units are m/year, threshold defined by Aqueduct 2.1 Withdrawal

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = "y2018m07d26_rh_deltas_cap_linear_trends_v01_v01"
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

threshold_arid_month = THRESHOLD_ARID_YEAR/12
threshold_low_water_use_month = THRESHOLD_LOW_WATER_USE_YEAR/12


# In[6]:

threshold_arid_month


# In[7]:

threshold_low_water_use_month


# In[8]:

temporal_reducers = ["","ma10_","ols10_"]


# In[9]:

sql = "CREATE TABLE {} AS".format(OUTPUT_TABLE_NAME)
sql = sql + " SELECT *,"
for temporal_reducer in temporal_reducers:
    sql = sql + " CASE"
    sql = sql + " WHEN {}riverdischarge_m_30spfaf06 < {} AND temporal_resolution = 'month' THEN 1".format(temporal_reducer,threshold_arid_month)
    sql = sql + " WHEN {}riverdischarge_m_30spfaf06 < {} AND temporal_resolution = 'year' THEN 1".format(temporal_reducer,THRESHOLD_ARID_YEAR)
    sql = sql + " ELSE 0 "
    sql = sql + " END"
    sql = sql + " AS {}arid_boolean_30spfaf06,".format(temporal_reducer)

for temporal_reducer in temporal_reducers:
    sql = sql + " CASE"
    sql = sql + " WHEN {}ptotww_m_30spfaf06 < {} AND temporal_resolution = 'month' THEN 1".format(temporal_reducer,threshold_low_water_use_month)
    sql = sql + " WHEN {}ptotww_m_30spfaf06 < {} AND temporal_resolution = 'year' THEN 1".format(temporal_reducer,THRESHOLD_LOW_WATER_USE_YEAR)
    sql = sql + " ELSE 0 "
    sql = sql + " END"
    sql = sql + " AS {}lowwateruse_boolean_30spfaf06 ,".format(temporal_reducer)


for temporal_reducer in temporal_reducers:    
    sql = sql + " CASE"
    sql = sql + " WHEN {}ptotww_m_30spfaf06 < {} AND temporal_resolution = 'month' AND {}riverdischarge_m_30spfaf06 < {} THEN 1".format(temporal_reducer, threshold_low_water_use_month, temporal_reducer,threshold_arid_month)
    sql = sql + " WHEN {}ptotww_m_30spfaf06 < {} AND temporal_resolution = 'year' AND {}riverdischarge_m_30spfaf06 < {} THEN 1".format(temporal_reducer, THRESHOLD_LOW_WATER_USE_YEAR, temporal_reducer,THRESHOLD_ARID_YEAR)
    sql = sql + " ELSE 0 "
    sql = sql + " END"
    sql = sql + " AS {}aridandlowwateruse_boolean_30spfaf06 ,".format(temporal_reducer)

    
sql = sql[:-1]
sql = sql + " FROM {}".format(INPUT_TABLE_NAME)

if TESTING:
    sql = sql + " LIMIT 100"


# In[10]:

print(sql)


# In[11]:

result = engine.execute(sql)


# In[12]:

sql_index = "CREATE INDEX {}delta_id ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"delta_id")


# In[13]:

result = engine.execute(sql_index)


# In[14]:

engine.dispose()


# In[15]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:01.792598

# In[ ]:



