
# coding: utf-8

# In[1]:

""" Determine moving average, min max and linear regression for deltas.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20180726
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
SCRIPT_NAME = 'Y2018M07D26_RH_Deltas_Temporal_Reducers_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
INPUT_TABLE_NAME = 'y2018m07d26_rh_deltas_total_demand_v01_v02'
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

input_columns = ["delta_id",
                 "temporal_resolution",
                 "year",
                 "month",
                 "area_m2_30spfaf06",
                 "area_count_30spfaf06"]

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


# In[6]:

def create_query(window,con,input_table_name,output_table_name,input_columns, stat_columns):
    """ Applies a moving average and saves the result in a new table. 
    -------------------------------------------------------------------------------
    
    Designed to work with aqueduct table structure that includes a year, month and
    temporal_resolution column. Will not work with other tables.     
    
    Args:
        window (integer) : Moving Average length e.g. 10 year.
        con (sqlAlchemy) : Database Connection. 
        input_table_name (string) : Input table name.
        output_table_name (string) : Output table name.
        input_columns (list) : list of column names used in the query and saved to
            output. e.g. year, month, pfafid etc. 
        stat_columns (list) : list of column names to apply the moving average and ols to.
            should be present in input table. 
    
    
    """
    sql = "CREATE TABLE {} AS ".format(output_table_name)
    sql = sql + "SELECT"    
    for input_column in input_columns:
        sql = sql + " {},".format(input_column)
    for stat_column in stat_columns:
        sql = sql + " {},".format(stat_column)
    for stat_column in stat_columns:
        sql = sql + " AVG({}) OVER(PARTITION BY delta_id, month, temporal_resolution ORDER BY year ROWS BETWEEN {:01.0f} PRECEDING AND CURRENT ROW) AS ma{:02.0f}_{},".format(stat_column,window-1,window,stat_column)
        sql = sql + " MIN({}) OVER(PARTITION BY delta_id, month, temporal_resolution ORDER BY year ROWS BETWEEN {:01.0f} PRECEDING AND CURRENT ROW) AS min{:02.0f}_{},".format(stat_column,window-1,window,stat_column)
        sql = sql + " MAX({}) OVER(PARTITION BY delta_id, month, temporal_resolution ORDER BY year ROWS BETWEEN {:01.0f} PRECEDING AND CURRENT ROW) AS max{:02.0f}_{},".format(stat_column,window-1,window,stat_column)
        sql = sql + " regr_slope({},year) OVER (PARTITION BY delta_id, month, temporal_resolution ORDER BY year ROWS BETWEEN {:01.0f} PRECEDING AND CURRENT ROW) AS slope{:02.0f}_{},".format(stat_column,window-1,window,stat_column)
        sql = sql + " regr_intercept({},year) OVER (PARTITION BY delta_id, month, temporal_resolution ORDER BY year ROWS BETWEEN {:01.0f} PRECEDING AND CURRENT ROW) AS intercept{:02.0f}_{},".format(stat_column,window-1,window,stat_column)
        sql = sql + (" regr_slope({},year) OVER (PARTITION BY delta_id, month, temporal_resolution ORDER BY year ROWS BETWEEN {:01.0f} PRECEDING AND CURRENT ROW) * year "
                     "+ regr_intercept({},year) OVER (PARTITION BY delta_id, month, temporal_resolution ORDER BY year ROWS BETWEEN {:01.0f} PRECEDING AND CURRENT ROW) AS ols{:02.0f}_{},".format(stat_column,window-1,stat_column,window-1,window,stat_column))
    
    
    sql = sql[:-1]
    sql = sql + " FROM {}".format(input_table_name)
    return sql


# In[7]:

sql = create_query( 10 ,connection ,INPUT_TABLE_NAME,OUTPUT_TABLE_NAME,input_columns, stat_columns)


# In[8]:

print(sql)


# In[9]:

result = engine.execute(sql)


# In[10]:

sql_index = "CREATE INDEX {}delta_id ON {} ({})".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME,"delta_id")


# In[11]:

sql_index


# In[12]:

result = engine.execute(sql_index)


# In[13]:

engine.dispose()


# In[14]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:03.793119

# In[ ]:



