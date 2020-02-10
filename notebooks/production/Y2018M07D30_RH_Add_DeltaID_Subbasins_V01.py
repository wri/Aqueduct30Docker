
# coding: utf-8

# In[1]:

""" Add delta id column to subbasin results. 
-------------------------------------------------------------------------------
Y2020M02D06 Update output version 2-3, input left 6-7 input right 1-1

Author: Rutger Hofste
Date: 20180730
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
SCRIPT_NAME = "Y2018M07D30_RH_Add_DeltaID_Subbasins_V01"
OUTPUT_VERSION = 3

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME_LEFT = "y2018m07d12_rh_ws_categorization_label_postgis_v01_v07"
INPUT_TABLE_NAME_RIGHT = "y2018m07d25_rh_delta_lookup_table_postgis_v01_v01"
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)


print("INPUT_TABLE_NAME_LEFT: " , INPUT_TABLE_NAME_LEFT, 
      "\nINPUT_TABLE_NAME_RIGHT: ",INPUT_TABLE_NAME_RIGHT,
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

#columns_to_keep_right = ["pfaf_id","delta_id"]
columns_to_keep_right = ["delta_id"]


# In[6]:

sql =  "CREATE TABLE {} AS".format(OUTPUT_TABLE_NAME)
sql += " SELECT "
sql += " l.*,"
sql += " r.delta_id"
sql += " FROM {} l".format(INPUT_TABLE_NAME_LEFT)
sql += " LEFT JOIN {} r ON".format(INPUT_TABLE_NAME_RIGHT)
sql += " l.pfafid_30spfaf06 = r.pfaf_id"
if TESTING:
    sql += " LIMIT 100"


# In[7]:

print(sql)


# In[8]:

result = engine.execute(sql)


# In[9]:

sql_index = "CREATE INDEX {}pfafid_30spfaf06 ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"pfafid_30spfaf06")


# In[10]:

result = engine.execute(sql_index)


# In[11]:

engine.dispose()


# In[12]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:13:15.582667  
# 0:17:09.497583  
# 0:15:13.606974  
# 0:16:41.309713
# 

# In[ ]:



