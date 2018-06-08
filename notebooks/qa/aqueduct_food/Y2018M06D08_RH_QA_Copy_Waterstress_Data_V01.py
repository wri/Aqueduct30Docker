
# coding: utf-8

# In[1]:

""" Copy water stress table to amazon S3.
-------------------------------------------------------------------------------

Aqueduct code / data has been moved to AWS / Github as of June 25th 2017. Prior
to that, analyses were done locally and code was stored in Google Drive. Version
control is therefore somewhat more difficult but luckily everything is 
annotated. 

Location of previous version of waterstress calculation:
Google Drive\02WRI\Projects\Aqueduct\Data\Aqueduct30\45_calculateWS02\
calculatedWS03.csv"

Manually copying 45_calculateWS02 to amazon S3 

Author: Rutger Hofste
Date: 20180502
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04


"""

SCRIPT_NAME = "Y2018M06D08_RH_QA_Copy_Waterstress_Data_V01"
OUTPUT_VERSION = 1
OVERWRITE =1

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/rawData/WRI/aqueduct_food_waterstress"
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

