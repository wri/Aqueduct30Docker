
# coding: utf-8

# # Y2017M12D19_RH_Water_Stress_Reduced_EE_V01
# 
# * Purpose of script: Calculate water stress using reduced (Long/Short Mean/Trend) maximum discharge and total withdrawals. 
# * Kernel used: python27
# * Date created: 20171219

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[4]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

SCRIPT_NAME = "Y2017M12D19_RH_Water_Stress_Reduced_EE_V01"

OUTPUT_VERSION = 1

PFAF_LEVEL = 6

YEARMIN = 1960
YEARMAX = 2014

SHORT_TERM_MIN = 2004
SHORT_TERM_MAX = 2014

LONG_TERM_MIN = 1960
LONG_TERM_MAX = 2014

DIMENSIONS30SSMALL = "43200x19440"
CRS = "EPSG:4326"
CRS_TRANSFORM30S_SMALL = [0.008333333333333333, 0.0, -180.0, 0.0, -0.008333333333333333, 81.0]


# In[3]:

import ee
import logging
import pandas as pd
import subprocess


# In[5]:

ee.Initialize()


# In[6]:

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler("./logs/%sV%0.2d.log" %(SCRIPT_NAME,OUTPUT_VERSION))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# In[8]:

icResults = ee.ImageCollection("projects/WRI-Aquaduct/PCRGlobWB20V07/reduced_global_historical_combined_V05")


# In[9]:

temporalResolutions = ["month","year"]


# In[10]:

indicators = ["Q_millionm3","WW_millionm3","WN_millionm3"]


# In[11]:

reducerTypes = ["mean","trend"]


# In[12]:

intervals = ["long","short"]


# Water Stress (WS) is defined as total withdrawal (WW) / (Maximum Discharge Q + Local Consumption WN)
# 
# we calculate 4 stress metrics per temporalResolution:
# 
# 1. mean short
# 1. mean long
# 1. trend short
# 1. trend long
# 

# In[13]:

temporalRange = {}
temporalRange["short"] = [SHORT_TERM_MIN,SHORT_TERM_MAX]
temporalRange["long"] = [LONG_TERM_MIN,LONG_TERM_MAX]


# In[ ]:




# In[ ]:




# In[23]:

for temporalResolution in temporalResolutions:
    for reducerType in reducerTypes:
        for interval in intervals:
            yearMin = temporalRange[interval][0]
            yearMax = temporalRange[interval][1]
            print(temporalResolution,reducerType,interval)
            if temporalResolution == "month":
                month = 12
                icTemp = icResults.filter(ee.Filter.eq("temporalResolution",temporalResolution)).filter(ee.Filter.eq("reducer",reducerType)).filter(ee.Filter.eq("interval",interval))
                
                Q = ee.Image(icTemp.filter(ee.Filter.eq("indicator","Q")).first())
                WW = ee.Image(icTemp.filter(ee.Filter.eq("indicator","WW")).first())
                WN = ee.Image(icTemp.filter(ee.Filter.eq("indicator","WN")).first())
                
                if reducerType == "trend":
                    Q = Q.select(["newValue"])
                    WW = WW.select(["newValue"])
                    WN = WN.select(["newValue"])
                
                print(WW.bandNames().getInfo())
                
                #newRow = createRow()
                #df = df.append(newRow,ignore_index=True)  


# In[ ]:



