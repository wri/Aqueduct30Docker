
# coding: utf-8

# In[2]:

""" Create table with result, raw ma10 and ols with geometries.
-------------------------------------------------------------------------------

Create postGIS table for selected basins with all ma_10 indicators

Author: Rutger Hofste
Date: 20180622
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M06D22_RH_QA_result_PostGIS_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_LEFT_NAME = 'y2018m06d04_rh_water_stress_postgis_30spfaf06_v01_v03'
INPUT_TABLE_RIGHT_NAME = 'hybas06_v04'
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)
OUTPUT_SCHEMA = "test"


# In[3]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[4]:

# imports
import re
import os
import numpy as np
import pandas as pd
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)


# In[ ]:



