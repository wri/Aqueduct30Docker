
# coding: utf-8

# In[1]:

""" Download pickled dataframes and convert to csv of sum sinks. 
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20180604
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.

"""

SCRIPT_NAME = "Y2018M05D15_RH_QA_Sum_Sinks_5min_EE_V01"
OUTPUT_VERSION = 1
OVERWRITE =1 

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M05D15_RH_Sum_Sinks_5min_EE_V01/output_V03"

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


# In[3]:

file_name = "global_sum_sinks_dimensionless_5minPfaf06.csv"


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {s3_output_path} --recursive --exclude "*" --include {file_name}')


# In[5]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:



