
# coding: utf-8

# In[1]:

""" Upload data to Google Cloud Storage
-------------------------------------------------------------------------------
This notebook will upload the geotiff files from the EC2 instance to Google Cloud Storage

Requires Google Cloud to be configured. Use 'gcloud init'.


Author: Rutger Hofste
Date: 20170802
Kernel: python36
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name


Returns:


"""

# Input Parameters

SCRIPT_NAME = "Y2017M08D02_RH_Upload_to_GoogleCS_V02"

EC2_INPUT_PATH = "/volumes/data/Y2017M07D31_RH_Convert_NetCDF_Geotiff_V02/output/"
GCS_OUTPUT_PATH = "gs://aqueduct30_v01/Y2017M08D02_RH_Upload_to_GoogleCS_V02"

# Output Parameters


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('gsutil -m cp {EC2_INPUT_PATH}*.tif {GCS_OUTPUT_PATH}')


# In[4]:

# Before copying the files I created a folder on GCS with the name /Y2017M08D02_RH_Upload_to_GoogleCS_V01. Number of files are >9000 so this will take a couple of minutes. You could use `gsutil ls -lR gs://aqueduct30_v01` to check progress


# In[5]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

