
# coding: utf-8

# In[1]:

""" Calculate Annual Scores by averaging monthly values.
-------------------------------------------------------------------------------

Update 2020/02/05 output version 5 to 6. Input changed from 2 to 5


Author: Rutger Hofste
Date: 20180712
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
SCRIPT_NAME = 'Y2018M07D12_RH_Annual_Scores_From_Months_PostGIS_V01'
OUTPUT_VERSION = 6

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = "y2018m07d09_rh_apply_aridlowonce_mask_postgis_v01_v05" 
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

selection_columns = ["pfafid_30spfaf06",
                     "year",
                     "temporal_resolution"]
aggregate_columns = ["ols_ols10_waterstress_dimensionless_30spfaf06",
                     "ols_capped_ols10_waterstress_dimensionless_30spfaf06",
                     "ols_ols10_waterdepletion_dimensionless_30spfaf06",
                     "ols_capped_ols10_waterdepletion_dimensionless_30spfaf06"]


# In[6]:

sql =  "CREATE TABLE {} AS".format(OUTPUT_TABLE_NAME)
sql += " SELECT "
for selection_column in selection_columns:
    sql += " {},".format(selection_column)
sql +=     " AVG(ols_capped_ols10_waterstress_dimensionless_30spfaf06) AS avg1y_ols_ols10_waterstress_dimensionless_30spfaf06,"
sql +=     " CASE WHEN SUM (ols_capped_ols10_ptotww_m_30spfaf06) >0 "
sql +=         " THEN SUM(ols_capped_ols10_waterstress_dimensionless_30spfaf06 * ols_capped_ols10_ptotww_m_30spfaf06) / SUM (ols_capped_ols10_ptotww_m_30spfaf06) "
sql +=     " ELSE AVG(ols_capped_ols10_waterstress_dimensionless_30spfaf06)"
sql +=     " END"
sql +=     " AS avg1y_ols_capped_ols10_weighted_waterstress_dimensionless_30spfaf06,"

sql +=     " AVG(ols_capped_ols10_waterdepletion_dimensionless_30spfaf06) AS avg1y_ols_ols10_waterdepletion_dimensionless_30spfaf06,"
sql +=     " CASE WHEN SUM (ols_capped_ols10_ptotww_m_30spfaf06) >0 "
sql +=         " THEN SUM(ols_capped_ols10_waterdepletion_dimensionless_30spfaf06 * ols_capped_ols10_ptotww_m_30spfaf06) / SUM (ols_capped_ols10_ptotww_m_30spfaf06) "
sql +=     " ELSE AVG(ols_capped_ols10_waterdepletion_dimensionless_30spfaf06)"
sql +=     " END"
sql +=     " AS avg1y_ols_capped_ols10_weighted_waterdepletion_dimensionless_30spfaf06"
sql += " FROM {}".format(INPUT_TABLE_NAME)
sql += " WHERE temporal_resolution = 'month'" 
sql += " GROUP BY pfafid_30spfaf06, year, temporal_resolution"
sql += " ORDER BY pfafid_30spfaf06, year"


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

engine.dispose()


# In[14]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:02:42.553314  
# 0:02:33.060358  
# 0:02:33.656827  
# 0:02:08.691658  
# 0:02:46.805150
# 

# In[ ]:



