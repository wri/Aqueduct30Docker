
# coding: utf-8

# In[1]:

""" Calculate water stress at subbasin level.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20180604
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
SCRIPT_NAME = 'Y2018M06D04_RH_Water_Stress_PostGIS_30sPfaf06_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
INPUT_TABLE_NAME = 'y2018m06d04_rh_arid_lowwateruse_postgis_30spfaf06_v01_v01'
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

sqls = []


if OVERWRITE_OUTPUT:
    sqls.append("DROP TABLE IF EXISTS {};".format(OUTPUT_TABLE_NAME))


# Water Stress = ptotww / (riverdischarge + ptotwn)

# In[5]:

if TESTING:
    sqls.append("CREATE TABLE {} AS SELECT * FROM {} WHERE pfafid_30spfaf06 < 130000 ;".format(OUTPUT_TABLE_NAME,INPUT_TABLE_NAME))
else:
    sqls.append("CREATE TABLE {} AS SELECT * FROM {};".format(OUTPUT_TABLE_NAME,INPUT_TABLE_NAME))


# In[6]:

sqls.append("ALTER TABLE {} ADD COLUMN waterstress_dimensionless_30spfaf06 double precision".format(OUTPUT_TABLE_NAME))


# In[7]:

sqls.append("UPDATE {}     SET waterstress_dimensionless_30spfaf06 = ma10_ptotww_m_30spfaf06 / (ma10_riverdischarge_m_30spfaf06 + ma10_ptotwn_m_30spfaf06)     WHERE aridandlowwateruse_boolean_30spfaf06 != 1;".format(OUTPUT_TABLE_NAME))


# In[8]:

sqls


# In[9]:

for sql in sqls:
    print(sql)
    result = engine.execute(sql)   


# In[10]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:02:51.356640
# 
# 

# In[ ]:



