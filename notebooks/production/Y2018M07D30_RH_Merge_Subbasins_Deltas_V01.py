
# coding: utf-8

# In[1]:

""" Merge subbasin results and delta results.
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
SCRIPT_NAME = 'Y2018M07D30_RH_Merge_Subbasins_Deltas_V01'
OUTPUT_VERSION = 4

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME_RIGHT = "y2018m07d27_rh_deltas_ws_categorization_label_v01_v03"
INPUT_TABLE_NAME_LEFT = "y2018m07d30_rh_add_deltaid_subbasins_v01_v02"
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)

print("Input Table Right: " , INPUT_TABLE_NAME_RIGHT,
      "\nInput Table Left: ", INPUT_TABLE_NAME_LEFT,
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

columns_to_keep_right = ["riverdischarge_m_30spfaf06", #added 20180808 to calculate sv and iav in deltas 
                         "waterstress_raw_dimensionless_30spfaf06",
                         "waterstress_score_dimensionless_30spfaf06",
                         "waterstress_category_dimensionless_30spfaf06",
                         "waterstress_label_dimensionless_30spfaf06",
                         "waterdepletion_raw_dimensionless_30spfaf06",
                         "waterdepletion_score_dimensionless_30spfaf06",
                         "waterdepletion_category_dimensionless_30spfaf06",
                         "waterdepletion_label_dimensionless_30spfaf06"]


# In[6]:

sql =  "CREATE TABLE {} AS".format(OUTPUT_TABLE_NAME)
sql += " SELECT"
sql += " l.*,"
for column_to_keep_right in columns_to_keep_right:
    new_name = column_to_keep_right.replace("30spfaf06","delta") 
    print(new_name)
    sql += " r.{} AS {},".format(column_to_keep_right,new_name)
sql = sql[:-1]
sql += " FROM {} l".format(INPUT_TABLE_NAME_LEFT)
sql += " LEFT JOIN {} r".format(INPUT_TABLE_NAME_RIGHT)
sql += " ON (l.delta_id = r.delta_id) AND"
sql += " (l.year = r.year) AND"
sql += " (l.month = r.month) AND"
sql += " (l.temporal_resolution = r.temporal_resolution)"
if TESTING:
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

sql_index2 = "CREATE INDEX {}year ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"year")


# In[12]:

result = engine.execute(sql_index2)


# In[13]:

sql_index3 = "CREATE INDEX {}month ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"month")


# In[14]:

result = engine.execute(sql_index3)


# In[15]:

engine.dispose()


# In[16]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:   
# 0:36:29.186526  
# 0:44:46.288626

# In[ ]:



