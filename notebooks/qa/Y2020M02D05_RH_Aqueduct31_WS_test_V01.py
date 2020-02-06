
# coding: utf-8

# In[17]:

""" Test the water stress calculation for Aqueduct 3.1
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20200205
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
SCRIPT_NAME = 'Y2020M02D05_RH_Aqueduct31_WS_test_V01'
OUTPUT_VERSION = 1

BASIN = 216041 # Normal basin (Ebro)
# BASIN = 742826 # Basin with negative final water stress values in February
# BASIN = 635303 # Basin with negative water stress in february 1962
# BASIN = 291707 # Basin with water stress exceedign 1 in february


DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = "y2018m07d09_rh_apply_aridlowonce_mask_postgis_v01_v05"
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)

print("Input Table: " , INPUT_TABLE_NAME, 
      "\nOutput Table: " , OUTPUT_TABLE_NAME)


# In[18]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[19]:

# imports
import re
import os
import numpy as np
import pandas as pd
import aqueduct3
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)


# In[20]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
#connection = engine.connect()

if OVERWRITE_OUTPUT:
    sql = "DROP TABLE IF EXISTS {};".format(OUTPUT_TABLE_NAME)
    print(sql)
    result = engine.execute(sql)


# sql = "SELECT"
# sql += " *"
# sql += " FROM {}".format(INPUT_TABLE_NAME)
# sql += " WHERE pfafid_30spfaf06 = {}".format(BASIN)
# sql += " AND temporal_resolution = 'month'"
# sql += " AND month = 2"

# In[24]:

sql = "SELECT"
sql += " *"
sql += " FROM {}".format(INPUT_TABLE_NAME)
sql += " WHERE pfafid_30spfaf06 = {}".format(BASIN)
sql += " AND year = 2014"


# In[25]:

sql


# In[26]:

df_raw = pd.read_sql(sql=sql,con=engine)


# In[27]:

df_raw.dtypes


# In[28]:

ptotww = df_raw[["year",
                 "ols10_ptotww_m_30spfaf06",
                 "capped_ols10_ptotww_m_30spfaf06",
                 "ols10_riverdischarge_m_30spfaf06",
                 "capped_ols10_riverdischarge_m_30spfaf06",
                 "ols10_waterstress_dimensionless_30spfaf06",
                 "capped_ols10_waterstress_dimensionless_30spfaf06",
                 "ols_capped_ols10_waterstress_dimensionless_30spfaf06"]]


# In[30]:

ptotww


# In[ ]:




# In[ ]:



