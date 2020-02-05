
# coding: utf-8

# In[1]:

""" Using the full range ols_ols10, apply the arid and lowwateruse thresholds.
-------------------------------------------------------------------------------

2020/02/03 starting from version 5


Author: Rutger Hofste
Date: 20180709
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
SCRIPT_NAME = 'Y2018M07D09_RH_Arid_LowWaterUse_Full_Ols_PostGIS_V01'
OUTPUT_VERSION = 5

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
INPUT_TABLE_NAME = 'y2018m06d28_rh_ws_full_range_ols_postgis_30spfaf06_v02_v06'
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)

THRESHOLD_ARID_YEAR = 0.03 #units are m/year, threshold defined by Aqueduct 2.1
THRESHOLD_LOW_WATER_USE_YEAR = 0.012 #units are m/year, threshold defined by Aqueduct 2.1 Withdrawal

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
    sql = "DROP TABLE IF EXISTS {};".format(OUTPUT_TABLE_NAME)
    print(sql)
    result = engine.execute(sql)


# In[5]:

temporal_reducers = ["ols_ols10_"]


# In[6]:

sql = "CREATE TABLE {} AS ".format(OUTPUT_TABLE_NAME)
sql = sql + "SELECT pfafid_30spfaf06, ols_ols10_riverdischarge_m_30spfaf06, ols_ols10_ptotww_m_30spfaf06, ols_ols10_ptotwn_m_30spfaf06, year, "

# arid
sql = sql + " CASE"
sql = sql + " WHEN (ols_ols10_riverdischarge_m_30spfaf06) < {} THEN 1".format(THRESHOLD_ARID_YEAR)
sql = sql + " ELSE 0 "
sql = sql + " END"
sql = sql + " AS ols_ols10_arid_boolean_30spfaf06,"

#lowwateruse
sql = sql + " CASE"
sql = sql + " WHEN ols_ols10_ptotww_m_30spfaf06 < {} THEN 1".format(THRESHOLD_LOW_WATER_USE_YEAR)
sql = sql + " ELSE 0 "
sql = sql + " END"
sql = sql + " AS ols_ols10_lowwateruse_boolean_30spfaf06,"

# Arid AND Lowwateruse  
sql = sql + " CASE"
sql = sql + " WHEN ols_ols10_ptotww_m_30spfaf06 < {} AND (ols_ols10_riverdischarge_m_30spfaf06)  < {} THEN 1".format(THRESHOLD_LOW_WATER_USE_YEAR, THRESHOLD_ARID_YEAR)
sql = sql + " ELSE 0 "
sql = sql + " END"
sql = sql + " AS ols_ols10_aridandlowwateruse_boolean_30spfaf06 ,"
sql = sql[:-1]
sql = sql + " FROM {}".format(INPUT_TABLE_NAME)
sql = sql + " WHERE temporal_resolution = 'year' "


# In[7]:

sql


# In[8]:

result = engine.execute(sql)


# In[9]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:02:11.888964  
# 0:02:12.255110  
# 0:01:56.781839  
# 0:03:23.336755  
# 0:02:31.679844
# 

# In[ ]:



