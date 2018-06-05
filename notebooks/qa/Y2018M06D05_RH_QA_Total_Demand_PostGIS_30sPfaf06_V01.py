
# coding: utf-8

# In[1]:

""" For a specific sub basin, query a 10y series for demand and supply to check
input for ma10.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20180605
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M06D05_RH_QA_Total_Demand_PostGIS_30sPfaf06_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = 'y2018m05d29_rh_total_demand_postgis_30spfaf06_v01_v02'
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)
OUTPUT_SCHEMA = "test"


# Filter 
PFAF_ID = 172111
TEMPORAL_RESOLUTION = "month"
YEAR = 1970 #final year i.e. ma starting for year-9
MONTH = 1


print("Input Table: " , INPUT_TABLE_NAME, 
      "\nOutput Table: " , OUTPUT_SCHEMA +"."+OUTPUT_TABLE_NAME)



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
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)


# In[4]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
connection = engine.connect()

sqls = []

if OVERWRITE_OUTPUT:
    sqls.append("DROP TABLE IF EXISTS {}.{};".format(OUTPUT_SCHEMA,OUTPUT_TABLE_NAME))


# In[5]:

sqls.append(
"CREATE TABLE {}.{} AS "
"SELECT * FROM {} "
"WHERE pfafid_30spfaf06 = {} AND "
"temporal_resolution = '{}' AND "
"year <= {} AND "
"year >= {} AND "
"month = {};".format(OUTPUT_SCHEMA,OUTPUT_TABLE_NAME, INPUT_TABLE_NAME,PFAF_ID,TEMPORAL_RESOLUTION,YEAR,YEAR-9,MONTH)
)


# In[6]:

sqls


# In[7]:

for sql in sqls:
    print(sql)
    result = engine.execute(sql)   


# In[8]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:



