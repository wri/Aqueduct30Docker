
# coding: utf-8

# In[1]:

""" Fit linear trend and average on 1969-2014 timeseries of linear trends for deltas.
-------------------------------------------------------------------------------

Fit a linear trend and average on the water stress values calculated with a 10
year moving window ordinary linear regression. 


Author: Rutger Hofste
Date: 20180628
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
SCRIPT_NAME = 'Y2018M07D26_RH_Deltas_WS_Full_Range_Ols_V01'
OUTPUT_VERSION = 2

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
INPUT_TABLE_NAME = 'y2018m07d26_rh_deltas_water_stress_v01_v02'
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
    sql = text("DROP TABLE IF EXISTS {};".format(OUTPUT_TABLE_NAME))
    result = engine.execute(sql)


# In[5]:

temporal_reducers = ["","ma10_","ols10_"]
if TESTING:
    temporal_reducers = [""]


# In[6]:

input_columns = ["delta_id",
                 "temporal_resolution",
                 "year",
                 "month",
                 "area_m2_30spfaf06",
                 "area_count_30spfaf06"]


# In[7]:

indicators = ["waterstress_dimensionless","waterdepletion_dimensionless","riverdischarge_m","ptotww_m","ptotwn_m"]


# In[8]:

sql = "CREATE TABLE {} AS ".format(OUTPUT_TABLE_NAME)
sql += "SELECT *, "
"""
for input_column in input_columns:
    sql = sql + " {},".format(input_column)
""" 
for temporal_reducer in temporal_reducers:
    for indicator in indicators:
        indicator = "{}{}_30spfaf06".format(temporal_reducer,indicator)
        print(indicator)
        sql += " AVG({}) OVER(PARTITION BY delta_id, month, temporal_resolution ORDER BY year ROWS BETWEEN 55 PRECEDING AND CURRENT ROW) AS avg_{},".format(indicator,indicator)
        sql += " MIN({}) OVER(PARTITION BY delta_id, month, temporal_resolution ORDER BY year ROWS BETWEEN 55 PRECEDING AND CURRENT ROW) AS min_{},".format(indicator,indicator)
        sql += " MAX({}) OVER(PARTITION BY delta_id, month, temporal_resolution ORDER BY year ROWS BETWEEN 55 PRECEDING AND CURRENT ROW) AS max_{},".format(indicator,indicator)
        sql += " regr_slope({},year) OVER (PARTITION BY delta_id, month, temporal_resolution ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS slope_{},".format(indicator,indicator)
        sql += " regr_intercept({},year) OVER (PARTITION BY delta_id, month, temporal_resolution ORDER BY year ROWS BETWEEN 55 PRECEDING AND CURRENT ROW) AS intercept_{},".format(indicator,indicator)
        sql += (" regr_slope({},year) OVER (PARTITION BY delta_id, month, temporal_resolution ORDER BY year ROWS BETWEEN 55 PRECEDING AND CURRENT ROW) * year "
                     "+ regr_intercept({},year) OVER (PARTITION BY delta_id, month, temporal_resolution ORDER BY year ROWS BETWEEN 55 PRECEDING AND CURRENT ROW) AS ols_{},".format(indicator,indicator,indicator))

sql = sql[:-1]
sql = sql + " FROM {}".format(INPUT_TABLE_NAME)
if TESTING:
    sql += " WHERE delta_id = 2 "
    sql += " LIMIT 100"


# In[9]:

sql


# In[10]:

result = engine.execute(sql)


# In[11]:

sql_index = "CREATE INDEX {}delta_id ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"delta_id")


# In[12]:

sql_index


# In[13]:

result = engine.execute(sql_index)


# In[14]:

sql_index2 = "CREATE INDEX {}year ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"year")


# In[15]:

sql_index2


# In[16]:

result = engine.execute(sql_index2)


# In[17]:

sql_index3 = "CREATE INDEX {}month ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"month")


# In[18]:

sql_index3


# In[19]:

result = engine.execute(sql_index3)


# In[20]:

engine.dispose()


# In[21]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:03.559076

# In[ ]:



