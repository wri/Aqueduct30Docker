
# coding: utf-8

# In[1]:

""" Calculate water stress with raw, ma10 and ols10 at subbasin level.
-------------------------------------------------------------------------------

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
SCRIPT_NAME = 'Y2018M06D04_RH_Water_Stress_PostGIS_30sPfaf06_V02'
OUTPUT_VERSION = 5

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
INPUT_TABLE_NAME = 'y2018m06d04_rh_arid_lowwateruse_postgis_30spfaf06_v02_v06'
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
    sql = "DROP TABLE IF EXISTS {};".format(OUTPUT_TABLE_NAME)
    print(sql)
    result = engine.execute(sql)
    


# Water Stress = ptotww / (riverdischarge + ptotwn)

# In[5]:

temporal_reducers = ["","ma10_","ols10_"]
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
    sql += " CASE "
    sql += " WHEN {}aridandlowwateruse_boolean_30spfaf06 = 1 THEN 1 ".format(temporal_reducer)
    sql += " ELSE {}ptotww_m_30spfaf06 / ({}riverdischarge_m_30spfaf06 + {}ptotwn_m_30spfaf06) ".format(temporal_reducer,temporal_reducer,temporal_reducer)
    sql += " END"
    sql += " AS {}waterstress_dimensionless_30spfaf06 ,".format(temporal_reducer)

sql = sql[:-1]
sql += " FROM {}".format(INPUT_TABLE_NAME)

if TESTING:
    sql += " LIMIT 100"
    


# In[7]:

print(sql)


# In[8]:

result = engine.execute(sql)


# In[9]:

sql_index = "CREATE INDEX {}pfafid_30spfaf06 ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"pfafid_30spfaf06")


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
# 0:02:51.356640  
# 0:03:09.128359  
# 0:08:57.643207  
# 0:08:51.883693
# 
# 

# In[ ]:



