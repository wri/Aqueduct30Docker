
# coding: utf-8

# In[1]:

""" Create total WW and total WN columns in simplified table for deltas.
-------------------------------------------------------------------------------


Author: Rutger Hofste
Date: 20180726
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
SCRIPT_NAME = "Y2018M07D26_RH_Deltas_Total_Demand_V01"
OUTPUT_VERSION = 2

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
INPUT_TABLE_NAME = "y2018m07d25_rh_group_delta_basins_v01_v01"
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
connection = engine.connect()

if OVERWRITE_OUTPUT:
    sql = text("DROP TABLE IF EXISTS {};".format(OUTPUT_TABLE_NAME))
    result = engine.execute(sql)


# In[5]:

if TESTING:
    # Appr 6 s
    sql = text("CREATE TABLE {} AS SELECT * FROM {} LIMIT 100000".format(OUTPUT_TABLE_NAME,INPUT_TABLE_NAME))
else:
    # Appr 10 min
    sql = text("CREATE TABLE {} AS SELECT * FROM {}".format(OUTPUT_TABLE_NAME,INPUT_TABLE_NAME))
result = engine.execute(sql)


# In[6]:

sql = "ALTER TABLE {} ADD COLUMN ptotwn_m_30spfaf06 double precision".format(OUTPUT_TABLE_NAME)
result = engine.execute(sql)


# In[7]:

sql = "ALTER TABLE {} ADD COLUMN ptotww_m_30spfaf06 double precision".format(OUTPUT_TABLE_NAME)
result = engine.execute(sql)


# In[8]:

sql = "UPDATE {}     SET ptotwn_m_30spfaf06 = pdomwn_m_30spfaf06 + pindwn_m_30spfaf06 + pirrwn_m_30spfaf06 + plivwn_m_30spfaf06;".format(OUTPUT_TABLE_NAME)
result = engine.execute(sql)


# In[9]:

sql = "UPDATE {}     SET ptotww_m_30spfaf06 = pdomww_m_30spfaf06 + pindww_m_30spfaf06 + pirrww_m_30spfaf06 + plivww_m_30spfaf06;".format(OUTPUT_TABLE_NAME)
result = engine.execute(sql)


# In[10]:

engine.dispose()


# In[11]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:02.206605
#         

# In[ ]:



