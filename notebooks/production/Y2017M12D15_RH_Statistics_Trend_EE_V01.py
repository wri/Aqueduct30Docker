
# coding: utf-8

# # Y2017M12D15_RH_Statistics_Trend_EE_V01
# 
# * Purpose of script: Calculate short term and long term average and 2014_trend for Q, WW and WN per basin
# * Kernel used: python27
# * Date created: 20171215

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

SCRIPT_NAME = "Y2017M12D15_RH_Statistics_Trend_EE_V01"

OUTPUT_VERSION = 1

PFAF_LEVEL = 6

YEARMIN = 1960
YEARMAX = 2014

SHORT_TERM_MIN = 2004
SHORT_TERM_MAX = 2014

LONG_TERM_MIN = 1960
LONG_TERM_MAX = 2014


# In[ ]:



