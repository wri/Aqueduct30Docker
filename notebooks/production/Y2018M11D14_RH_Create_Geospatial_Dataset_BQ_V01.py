
# coding: utf-8

# In[1]:

""" Create dataset to hold geospatial tables in Bigquery.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181114
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = 'Y2018M11D14_RH_Create_Geospatial_Dataset_BQ_V01'
OUTPUT_VERSION = 1

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "geospatial_v01"

print("\nBQ_OUTPUT_DATASET_NAME: ", BQ_OUTPUT_DATASET_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import os
import sqlalchemy
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[4]:

dataset_id = BQ_OUTPUT_DATASET_NAME


# In[5]:

dataset_ref = client.dataset(dataset_id)


# In[6]:

dataset = bigquery.Dataset(dataset_ref)


# In[7]:

dataset = client.create_dataset(dataset)


# In[8]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# previous runs:  
# 0:00:14.998675
# 
# 

# In[ ]:



