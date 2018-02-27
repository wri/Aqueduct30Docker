
# coding: utf-8

# ### Zonal Statistics using EE with PCRGlobWB data and hydroBasin level 6
# 
# * Purpose of script: This script will perform a zonal statistics calculation using earth engine, PCRGlobWB data and Hydrobasin level 6 at 30s resolution. 
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20171228

# In[ ]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[1]:

import ee


# In[2]:

ee.Initialize()


# In[4]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

HYDROBASINS = "projects/WRI-Aquaduct/PCRGlobWB20V07/hybas_lev00_v1c_merged_fiona_30s_V01"

SCRIPT_NAME = "Y2017M12D28_RH_zonal_stats_EE_toGCS_V02"


# In[5]:

icResults = ee.ImageCollection("%s/reduced_global_historical_combined_V05" %(EE_PATH))


# WSreducedTemp is water stress averaged over 12 months based on long and short trends (mean and trend).

# In[11]:

indicators = ["WSreducedTemp","WS"]


# In[12]:

indicator = "WS"


# 

# In[9]:

icWSreducedTemp = icResults.filter(ee.Filter.eq("indicator","WSreducedTemp"))


# In[10]:

print(icWSreducedTemp.size().getInfo())


# In[ ]:



