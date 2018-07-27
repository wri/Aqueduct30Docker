
# coding: utf-8

# In[1]:

""" Apply the mask for arid and lowwater use subbasins based on ols_ols10 (once).
-------------------------------------------------------------------------------

Join the results of the arid and lowwater use mask based on annual values (ols)
(ols_ols10_**) and the master table. 

The script uses the 2014 value for the right table. 


Author: Rutger Hofste
Date: 20180727
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
SCRIPT_NAME = 'Y2018M07D27_RH_Deltas_Update_WaterStress_AridLowOnce_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = "y2018m07d27_rh_deltas_merge_simplify_tables_v01_v01"
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

sql = "CREATE TABLE {} AS".format(OUTPUT_TABLE_NAME)

sql += " WITH cte AS ( "
sql += " SELECT *,"

# Water Stress Raw
sql += " CASE"
sql +=     " WHEN temporal_resolution = 'month'"
sql +=     " THEN ("
sql +=         " CASE"
sql +=         " WHEN ols_ols10_aridandlowwateruse_boolean_30spfaf06 = 0"
sql +=             " THEN ols_ols10_waterstress_dimensionless_30spfaf06"        
sql +=         " ELSE 1"
sql +=         " END )"
sql +=    " WHEN temporal_resolution = 'year'"
sql +=    " THEN ( "
sql +=         " CASE"
sql +=         " WHEN ols_ols10_aridandlowwateruse_boolean_30spfaf06 = 0"
sql +=             " THEN avg1y_ols_ols10_weighted_waterstress_dimensionless_30spfaf06"        
sql +=         " ELSE 1"
sql +=         " END )"
sql +=    " ELSE -9999"
sql +=    " END"
sql +=    " AS waterstress_raw_dimensionless_30spfaf06"
sql += " FROM {}".format(INPUT_TABLE_NAME)
sql += " )"

# Water Stress Scores
sql += " SELECT *,"
sql += " CASE"
sql += " WHEN ols_ols10_aridandlowwateruse_boolean_30spfaf06 = 0 AND waterstress_raw_dimensionless_30spfaf06 > 0"
sql +=     " THEN GREATEST(0,LEAST(((LN(waterstress_raw_dimensionless_30spfaf06) - LN(0.1))/LN(2))+1,5))" 
sql += " WHEN ols_ols10_aridandlowwateruse_boolean_30spfaf06 = 0 AND waterstress_raw_dimensionless_30spfaf06 <= 0"
sql +=     " THEN 0"
sql += " WHEN ols_ols10_aridandlowwateruse_boolean_30spfaf06 = 1 "
sql +=     " THEN -1"
sql += " ELSE -9999 "
sql += " END AS waterstress_score_dimensionless_30spfaf06,"
sql = sql[:-1]
sql += " FROM cte"


# In[6]:

sql


# In[7]:

result = engine.execute(sql)


# In[8]:

sql_index = "CREATE INDEX {}delta_id ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"delta_id")


# In[9]:

result = engine.execute(sql_index)


# In[10]:

sql_index2 = "CREATE INDEX {}year ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"year")


# In[11]:

result = engine.execute(sql_index2)


# In[12]:

sql_index3 = "CREATE INDEX {}month ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"month")


# In[13]:

result = engine.execute(sql_index3)


# In[14]:

engine.dispose()


# In[15]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:07:37.073694  

# In[ ]:




# In[ ]:



