
# coding: utf-8

# In[1]:

""" Replace Null values with numbers to prepare for bigquery. 
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
SCRIPT_NAME = 'Y2018M07D30_RH_Replace_Null_Deltas_V01'
OUTPUT_VERSION = 3

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = "y2018m07d30_rh_coalesce_columns_v01_v08"


print("Input Table: " , INPUT_TABLE_NAME)


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
#connection = engine.connect()


# In[5]:

sql = "UPDATE {}".format(INPUT_TABLE_NAME)
sql += " SET riverdischarge_m_delta = -1,"
sql += " waterstress_raw_dimensionless_delta = -1,"
sql += " waterstress_score_dimensionless_delta = -1,"
sql += " waterstress_category_dimensionless_delta = -1,"
sql += " waterstress_label_dimensionless_delta = 'NoData',"
sql += " waterdepletion_raw_dimensionless_delta = -1,"
sql += " waterdepletion_score_dimensionless_delta = -1,"
sql += " waterdepletion_category_dimensionless_delta = -1,"
sql += " waterdepletion_label_dimensionless_delta = 'NoData',"
sql += " delta_id = -1"
sql += " WHERE delta_id IS NULL"


# In[6]:

sql


# In[7]:

result = engine.execute(sql)


# In[8]:

sql = "ALTER TABLE {} ALTER COLUMN delta_id SET DATA TYPE INT;".format(INPUT_TABLE_NAME)


# In[9]:

# will take 20 min to run.
result = engine.execute(sql)


# In[10]:

engine.dispose()


# In[11]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:21:14.401220  
# 0:42:08.612665  
# 0:42:04.108612  
# 0:52:44.430636  
# 0:50:01.565077

# In[ ]:



