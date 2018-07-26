
# coding: utf-8

# In[1]:

""" Group Delta basins and calculate supple and demand. 
-------------------------------------------------------------------------------

This script will calculate new fluxes for demand and supply per delta region.

1) convert fluxes to volumes
2) take the sum of all volumes
3) divide by sum of all areas.

probably using cte's.


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
SCRIPT_NAME = "Y2018M07D25_RH_Group_Delta_Basins_V01"
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
INPUT_TABLE_NAME = "y2018m07d25_rh_join_deltas_values_v01_v01"
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

# Convert fluxes to volumes


# In[6]:

columns_to_keep = ["pfafid_30spfaf06",
                   "temporal_resolution",
                   "year",
                   "month",
                   "area_m2_30spfaf06",
                   "area_count_30spfaf06",
                   "delta_id"]


# In[ ]:




# In[7]:

sectors = ["pdom",
           "pind",
           "pirr",
           "pliv"]
use_types = ["ww","wn"]


# In[8]:

sql = "CREATE TABLE {} AS".format(OUTPUT_TABLE_NAME)
sql += " WITH cte AS ("
sql += " SELECT"
for column_to_keep in columns_to_keep:
    sql += " {},".format(column_to_keep)
for sector in sectors:
    for use_type in use_types:
        sql += " area_m2_30spfaf06 * {}{}_m_30spfaf06 AS {}{}_m3_30spfaf06 ,".format(sector,use_type,sector,use_type)
        
sql += " area_m2_30spfaf06 * {}_m_30spfaf06 AS {}_m3_30spfaf06 ,".format("riverdischarge","riverdischarge")        
sql = sql[:-1]

sql += " FROM {}".format(INPUT_TABLE_NAME)
sql += " )"
sql += " SELECT "
sql += " delta_id,"
sql += " temporal_resolution,"
sql += " year,"
sql += " month,"

sql += " SUM(area_m2_30spfaf06) AS area_m2_30spfaf06,"
sql += " SUM(area_count_30spfaf06) AS area_count_30spfaf06,"


for sector in sectors:
    for use_type in use_types:
        sql += " SUM({}{}_m3_30spfaf06) / SUM(area_m2_30spfaf06) AS {}{}_m_30spfaf06,".format(sector,use_type,sector,use_type)

sql += " SUM({}_m3_30spfaf06) / SUM(area_m2_30spfaf06) AS {}_m_30spfaf06,".format("riverdischarge","riverdischarge")
sql = sql[:-1]



sql += " FROM cte"
sql += " GROUP BY delta_id, temporal_resolution, year, month"


# In[9]:

sql


# In[10]:

result = engine.execute(sql)


# In[11]:

sql_index = "CREATE INDEX {}delta_id ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"delta_id")


# In[12]:

result = engine.execute(sql_index)


# In[13]:

engine.dispose()


# In[14]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

