
# coding: utf-8

# In[1]:

""" Use first valid of delta or subbasin column. 
-------------------------------------------------------------------------------

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
SCRIPT_NAME = 'Y2018M07D30_RH_Coalesce_Columns_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = "y2018m07d30_rh_merge_subbasins_deltas_v01_v01"
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


# In[5]:

columns_of_interest = ["waterstress_raw_dimensionless_30spfaf06",
                       "waterstress_score_dimensionless_30spfaf06",
                       "waterstress_category_dimensionless_30spfaf06",
                       "waterstress_label_dimensionless_30spfaf06"]


# In[6]:

sql =  "CREATE TABLE {} AS".format(OUTPUT_TABLE_NAME)
sql += " SELECT *,"
for column_of_interest in columns_of_interest:
    subbasin_column = column_of_interest
    delta_column = column_of_interest.replace("30spfaf06","delta")
    final_column = column_of_interest.replace("30spfaf06","coalesced")
    print(final_column)
    sql += " CASE WHEN delta_id >= 0 THEN {}  ELSE {}".format(delta_column,subbasin_column)
    sql += " END AS {},".format(final_column)
    
sql = sql[:-1]   
sql += " FROM {}".format(INPUT_TABLE_NAME)

if TESTING:
    sql += " ORDER BY waterstress_label_dimensionless_coalesced"
    sql += " LIMIT 10"


# In[7]:

sql


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
# 0:13:48.316626
# 
