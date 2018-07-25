
# coding: utf-8

# In[1]:

""" Join delta_ids, supply and demand tables.
-------------------------------------------------------------------------------

the result is a table with the normal supply and demand and the delta id
appended to the table. 


Author: Rutger Hofste
Date: 20180725
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
SCRIPT_NAME = "Y2018M07D25_RH_Join_Deltas_Values_V01"
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
INPUT_TABLE_NAME_LEFT = "global_historical_all_multiple_m_30spfaf06_v02"
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

columns_to_keep_left = ["pfafid_30spfaf06",
                        "temporal_resolution",
                        "year",
                        "month",
                        "area_m2_30spfaf06",
                        "area_count_30spfaf06"]


# In[6]:

sectors = ["pdom",
           "pind",
           "pirr",
           "pliv"]
use_types = ["ww","wn"]


# In[7]:

for sector in sectors:
    for use_type in use_types:
        columns_to_keep_left.append("{}{}_count_30spfaf06".format(sector,use_type))
        columns_to_keep_left.append("{}{}_m_30spfaf06".format(sector,use_type))


# In[8]:

columns_to_keep_left.append("riverdischarge_m_30spfaf06")
columns_to_keep_left.append("riverdischarge_count_30spfaf06")


# In[9]:

columns_to_keep_left


# In[10]:

#columns_to_keep_right = ["pfaf_id","delta_id"]
columns_to_keep_right = ["delta_id"]


# In[11]:

sql =  "CREATE TABLE {} AS".format(OUTPUT_TABLE_NAME)
sql += " SELECT "
for column_to_keep_left in columns_to_keep_left:
    sql += " l.{},".format(column_to_keep_left)
for column_to_keep_right in columns_to_keep_right:
    sql += " r.{},".format(column_to_keep_right)
sql = sql[:-1]    
sql += " FROM {} l".format(INPUT_TABLE_NAME_LEFT)
sql += " INNER JOIN {} r ON".format(INPUT_TABLE_NAME_RIGHT)
sql += " l.pfafid_30spfaf06 = r.pfaf_id"
sql += " WHERE r.delta_id >= 0"
if TESTING:
    sql += " LIMIT 100"
    


# In[12]:

print(sql)


# In[13]:

result = engine.execute(sql)


# In[14]:

sql_index = "CREATE INDEX {}pfafid_30spfaf06 ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"pfafid_30spfaf06")


# In[15]:

result = engine.execute(sql_index)


# In[16]:

engine.dispose()


# In[17]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

