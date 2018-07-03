
# coding: utf-8

# In[5]:

""" Compare several methods for setting arid and lowwateruse.
-------------------------------------------------------------------------------

Compare several methods for determining arid and lowwater use subbasins

Method 1:

Count number of months in which. 


Author: Rutger Hofste
Date: 20180702
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M07D02_RH_QA_Arid_Low_Method_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = 'y2018m06d28_rh_ws_full_range_ols_postgis_30spfaf06_v01_v02'
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)

OUTPUT_SCHEMA = "qa"

print("Input table: " + INPUT_TABLE_NAME,
      "\nOutput table: " + OUTPUT_SCHEMA +"/"+ OUTPUT_TABLE_NAME)


# In[6]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[7]:

# imports
import re
import os
import numpy as np
import pandas as pd
import aqueduct3
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)


# In[8]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
#connection = engine.connect()

if OVERWRITE_OUTPUT:
    sql = text("DROP TABLE IF EXISTS {}.{};".format(OUTPUT_SCHEMA,OUTPUT_TABLE_NAME))
    result = engine.execute(sql)


# In[ ]:

sql = sql + "SELECT *,"

