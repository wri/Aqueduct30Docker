
# coding: utf-8

# ### Ingest data on Google Earth Engine (WRI bucket)
# 
# * Purpose of script: This notebook will upload the geotiff files from the Google Cloud Storage to the WRI/aqueduct earthengine bucket. 
# * Author: Rutger Hofste
# * Kernel used: python36
# * Date created: 20170802

# ## Preparation

# * Authorize earthengine by running in your terminal: `earthengine authenticate`  
# * you need to have access to the WRI-Aquaduct (yep a Google employee made a typo) bucket to ingest the data. Rutger can grant access to write to this folder. 
# * Have access to the Google Cloud Storage Bucker

# Run in your terminal `gcloud config set project aqueduct30`

# In[1]:

import subprocess
import datetime
import os
import time
import re
import pandas as pd


# ## Functions

# In[35]:

def splitKey(key):
    # will yield the root file code and extension of a set of keys
    prefix, extension = key.split(".")
    fileName = prefix.split("/")[-1]
    parameter = fileName[:-12]
    M = fileName[-2:] #can also do this with regular expressions if you like
    Y = fileName[-7:-3]
    I = fileName[-11:-8]
    outDict = {"fileName":fileName,"extension":extension,"parameter":parameter,"M":M,"Y":Y,"I":I}
    return outDict

def splitParameter(parameter):
    values = parameter.split("_")
    keys = ["geographic_range","temporal_range","indicator","temporal_resolution","units","spatial_resolution","temporal_range_min","temporal_range_max"]
    # ['global', 'historical', 'PDomWN', 'month', 'millionm3', '5min', '1960', '2014']
    outDict = dict(zip(keys, values))
    return outDict


# ## Script

# In[ ]:




# In[3]:

eeBasePath = "projects/WRI-Aquaduct/test2"


# In[4]:

command = ("earthengine create folder %s") %eeBasePath


# In[5]:

print(command)


# In[6]:

subprocess.check_output(command,shell=True)


# In[7]:

command = ("/opt/google-cloud-sdk/bin/gsutil ls gs://aqueduct30_v01/Y2017M08D02_RH_Upload_to_GoogleCS_V01/")


# In[8]:

keys = subprocess.check_output(command,shell=True)


# In[9]:

keys = keys.decode('UTF-8').splitlines()


# Removing first item from the list. The first item contains a folder without file name

# In[22]:

keys2 = keys[1:]


# In[23]:

df = pd.DataFrame()
i = 0
for key in keys2:
    print(key)
    i = i+1
    outDict = splitKey(key)
    df2 = pd.DataFrame(outDict,index=[i])
    df = df.append(df2)    


# In[24]:

df.head()


# In[25]:

df.tail()


# In[27]:

df.shape


# In[29]:

parameters = df.parameter.unique()


# In[30]:

print(parameters)


# In[36]:

splitParameter("global_historical_PDomWN_month_millionm3_5min_1960_2014")


# In[ ]:



