
# coding: utf-8

# In[1]:

""" Copy the groundwater table declining trend data to S3. 
-------------------------------------------------------------------------------

Aqueduct code / data has been moved to AWS / Github as of June 25th 2017. Prior
to that, analyses were done locally and code was stored in Google Drive. Version
control is therefore somewhat more difficult but luckily everything is 
annotated. 

Location of tables:
wri-projects/Aqueduct30/qaData/groundwater_table_declining_trend/
Y2018M06D08_RH_QA_Copy_GWTDT_Input_Data_V01/output_V01/data/tables

Author: Rutger Hofste
Date: 20180502
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04


"""

SCRIPT_NAME = "Y2018M06D08_RH_QA_Copy_GWTDT_Input_Data_V01"
OUTPUT_VERSION = 1
OVERWRITE =1

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/rawData/Deltares/groundwater/Final_Oct_2017/"
s3_output_path = "s3://wri-projects/Aqueduct30/qaData/aqueduct_food/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input s3 : " + S3_INPUT_PATH +
      "\nOutput s3: " + s3_output_path)


# In[2]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {s3_output_path} --recursive')


# In[4]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:26.164974
# 

# In[ ]:



