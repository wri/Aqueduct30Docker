
# coding: utf-8

# # Y2017M12D12_RH_Water_Stress_V02
# 
# * Purpose of script: Calculate water stress using maximum discharge and total withdrawals  
# * Kernel used: python27
# * Date created: 20171212

# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[7]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

SCRIPT_NAME = "Y2017M12D12_RH_Water_Stress_V02"

OUTPUT_VERSION = 1

PFAF_LEVEL = 6


# In[4]:

import ee
import subprocess
import pandas as pd
import logging
import subprocess


# In[5]:

ee.Initialize()


# In[8]:

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler("./logs/%sV%0.2d.log" %(SCRIPT_NAME,OUTPUT_VERSION))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# In[9]:

def createIndicatorDataFrame():
    df = pd.DataFrame()
    for temporalResolution in temporalResolutions:
        newRow = {}
        newRow["temporalResolution"] = temporalResolution
        newRow["icWW"] = "projects/WRI-Aquaduct/%s/global_historical_PTotWW_%s_m_pfaf%0.2d_1960_2014" %(EE_PATH,temporalResolution,PFAF_LEVEL)
        newRow["icQ"] = "projects/WRI-Aquaduct/%s/global_historical_availableriverdischarge_%s_millionm3_5minPfaf%0.1d_1960_2014" %(EE_PATH,temporalResolution,PFAF_LEVEL)


# In[10]:

temporalResolutions = ["year","month"]


# In[ ]:



