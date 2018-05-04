
# coding: utf-8

# In[ ]:

""" Zonal statistics for basin demand. Export in table format.
-------------------------------------------------------------------------------
Zonal statistics for basin area. Export in table format.

Strategy:

1. first riverdischarge in zones masked by previous script (max_fa)

2. mask endorheic basins with mask from previous script

3. sum riverdischarge in remaining pixels






Author: Rutger Hofste
Date: 20180504
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    TESTING (boolean) : Testing mode. Uses a smaller geography if enabled.
    
    SCRIPT_NAME (string) : Script name.
    EE_INPUT_ZONES_PATH (string) : earthengine input path for zones.
    EE_INPUT_VALUES_PATH (string) : earthengine input path for value images.
    INPUT_VERSION_ZONES (integer) : input version for zones images.
    INPUT_VERSION_VALUES (integer) : input version for value images.
    OUTPUT_VERSION (integer) : output version. 
    EXTRA_PROPERTIES (dictionary) : Extra properties to store in the resulting
        pandas dataframe. 
    

Returns:

"""

TESTING = 1
SCRIPT_NAME = "Y2018M05D04_RH_Zonal_Stats_Supply_EE_V01"

EE_INPUT_ZONES_PATH = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V01"
INPUT_VERSION_ZONES = 4

SPATIAL_RESOLUTIONS = ["30s"]
PFAF_LEVELS = [6]


# In[3]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[ ]:

# Imports
import pandas as pd
from datetime import timedelta
import os
import ee
import aqueduct3

ee.Initialize()


# In[ ]:

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler("./logs/{}.log".format(SCRIPT_NAME))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# In[ ]:

"""
Tactic

FA based approach: static


use riverdischarge mask

max_FA  (most downstream pixel)


mask sinks

add masked sinks







"""

