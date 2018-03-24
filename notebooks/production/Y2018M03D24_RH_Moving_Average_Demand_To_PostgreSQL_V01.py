
# coding: utf-8

# # Y2018M03D24_RH_Moving_Average_Demand_To_GCS_V01
# 
# * Purpose of script: store moving average results for demand in a CSV file in GCS.
# 
# * Script exports to: 
# * Kernel used: python35
# * Date created: 20180324
# 
# 
# 

# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[7]:

SCRIPT_NAME = "Y2018M03D24_RH_Moving_Average_Demand_To_PostgreSQL_V01"

INPUT_VERSION = 3


# In[4]:

import ee


# In[5]:

ee.Initialize()


# In[11]:

temp_image = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/area_30spfaf06_m2_V01V01")


# In[12]:

area_30spfaf06_m2 = temp_image.select(["sum"])


# In[13]:

zones_30spfaf06 = temp_image.select(["zones"])


# In[14]:

zones_30spfaf06.getInfo()


# In[8]:

ic = ee.ImageCollection("projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_PTotWN_month_m_pfaf06_1960_2014_movingaverage_10y_V{:02.0f}".format(INPUT_VERSION))


# In[ ]:



