
# coding: utf-8

# In[1]:

""" Ingest rasterized AQ30 indicators to earthengine.
-------------------------------------------------------------------------------

Compare Aqueduct 30 vs 21 and create change tables.

Author: Rutger Hofste
Date: 20190522
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0

SCRIPT_NAME = "Y2019M05D22_RH_AQ30VS21_Compare_Tables_V01"
OUTPUT_VERSION = 1


AQ21_GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2019M05D22_RH_AQ39VS21_Rasterize_AQ21_EE_V01/output_V02/"
AQ30_GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2019M05D21_RH_AQ30VS21_Rasterize_AQ30_EE_V01/output_V05/"

ec2_input_path_aq21 = "/volumes/data/{}/input_V{:02.0f}/aq21/".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_input_path_aq30 = "/volumes/data/{}/input_V{:02.0f}/aq30".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("AQ21_GCS_INPUT_PATH: " + AQ21_GCS_INPUT_PATH +
      "\nAQ30_GCS_INPUT_PATH: " + AQ30_GCS_INPUT_PATH )


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('rm -r {ec2_input_path_aq21}')
get_ipython().system('rm -r {ec2_input_path_aq30}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path_aq21}')
get_ipython().system('mkdir -p {ec2_input_path_aq30}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[6]:

get_ipython().system('gsutil -m cp -r {AQ21_GCS_INPUT_PATH} {ec2_input_path_aq21}')


# In[7]:

get_ipython().system('gsutil -m cp -r {AQ30_GCS_INPUT_PATH} {ec2_input_path_aq30}')


# In[10]:

import rasterio
import numpy as np


# In[14]:

aq21_input_path = "{}/{}{}".format(ec2_input_path_aq21,"Y2019M05D22_RH_AQ39VS21_Rasterize_AQ21_EE_V01/output_V02/","DEFAULT.tif")


# In[13]:

aq21_input_path 


# In[ ]:



