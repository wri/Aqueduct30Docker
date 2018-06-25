
# coding: utf-8

# In[1]:

""" Cap the results of the ols at the minmax of the moving window.
-------------------------------------------------------------------------------

Linear Trends work very well when the data has a few valid values. For some
basins however, only a few values are non-zero. Take for example basin 
157650 that has negative ols10_riverdischarge values for the year 
1975. In order to avoid negative discharge values, ols results are capped at 
the minimum and maximum values of the moving window. 


Author: Rutger Hofste
Date: 20180625
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
SCRIPT_NAME = 'Y2018M06D25_RH_Cap_Linear_Trends_PostGis_30sPfaf06_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = 'y2018m06d01_rh_temporal_reducers_postgis_30spfaf06_v01_v03'
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

def create_query(input_table_name, output_table_name,stat_columns):
    sql = "CREATE TABLE {} AS ".format(output_table_name)
    sql = sql + "SELECT *,"
    for stat_column in stat_columns:
        sql = sql + " CASE"
        sql = sql + " WHEN ols10_{} < min10_{} THEN min10_{}".format(stat_column,stat_column,stat_column)
        sql = sql + " WHEN ols10_{} > max10_{} THEN max10_{}".format(stat_column,stat_column,stat_column)
        sql = sql + " ELSE ols10_{} ".format(stat_column)
        sql = sql + " END"
        sql = sql + " AS capped_ols10_{},".format(stat_column)
        
    sql = sql[:-1]    
    sql = sql + " FROM {}".format(input_table_name)
    return sql
    


# In[6]:

sectors = ["dom","ind","irr","liv","tot"]
demand_types = ["ww","wn"]
supply = ["riverdischarge"]

demand_column_names = []
for sector in sectors:
    for demand_type in demand_types:
        demand_column_name = "p{}{}_m_30spfaf06".format(sector,demand_type)
        demand_column_names.append(demand_column_name)
supply_column_names = ["{}_m_30spfaf06".format(supply[0])]
stat_columns = demand_column_names + supply_column_names
stat_columns


# In[7]:

sql = create_query(INPUT_TABLE_NAME, OUTPUT_TABLE_NAME,stat_columns)


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
# 0:08:28.266176

# In[ ]:



