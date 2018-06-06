
# coding: utf-8

# In[4]:

""" Check output of zonal stats demand. Convert to CSV.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20180605
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04


Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.

"""

SCRIPT_NAME = "Y2018M06D05_Rh_QA_Zonal_Stats_Demand_EE_V01"
OUTPUT_VERSION = 1
OVERWRITE =1 

TEMPORAL_RESOLUTION = "month"
YEAR = 1970
MONTH = 1

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M04D22_RH_Zonal_Stats_Demand_EE_V01/output_V01"

s3_output_path = "s3://wri-projects/Aqueduct30/qaData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)


print("Input S3: " + S3_INPUT_PATH,
      "\nOutput s3: " + s3_output_path)



# In[2]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[5]:

file_name = "global_historical_*_{}_m_5min_1960_2014_I*Y{:04.0f}M{:02.0f}_reduced_06_30s_mean.csv".format(TEMPORAL_RESOLUTION,YEAR,MONTH)


# In[7]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {s3_output_path} --recursive --exclude "*" --include {file_name}')


# In[8]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
#    

# In[ ]:



