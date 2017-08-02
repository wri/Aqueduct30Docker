
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

# In[2]:

def splitKey(key):
    # will yield the root file code and extension of a set of keys
    prefix, extension = key.split(".")
    fileName = prefix.split("/")[-1]
    parameter = fileName[:-12]
    month = fileName[-2:] #can also do this with regular expressions if you like
    year = fileName[-7:-3]
    identifier = fileName[-11:-8]
    outDict = {"fileName":fileName,"extension":extension,"parameter":parameter,"month":month,"year":year,"identifier":identifier}
    return outDict

def splitParameter(parameter):
    values = parameter.split("_")
    keys = ["geographic_range","temporal_range","indicator","temporal_resolution","units","spatial_resolution","temporal_range_min","temporal_range_max"]
    # ['global', 'historical', 'PDomWN', 'month', 'millionm3', '5min', '1960', '2014']
    outDict = dict(zip(keys, values))
    outDict["parameter"] = parameter
    return outDict


# ## Script

# In[ ]:




# In[3]:

eeBasePath = "projects/WRI-Aquaduct/PCRGlobWB20V05"


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

# In[10]:

keys2 = keys[1:]


# In[11]:

df = pd.DataFrame()
i = 0
for key in keys2:
    print(key)
    i = i+1
    outDict = splitKey(key)
    df2 = pd.DataFrame(outDict,index=[i])
    df = df.append(df2)    


# In[12]:

df.head()


# In[13]:

df.tail()


# In[14]:

df.shape


# In[15]:

parameters = df.parameter.unique()


# In[16]:

print(parameters)


# In[17]:

print(len(parameters))


# In[18]:

for parameter in parameters:
    eeLocation = eeBasePath + "/" + parameter
    command = ("earthengine create collection %s") %eeLocation
    #subprocess.check_output(command,shell=True)
    print(command)
    


# Now that the folder and collections have been created we can start ingesting the data. It is crucial to store the relevant metadata with the images. 

# In[19]:

df_parameter = pd.DataFrame()
i = 0
for parameter in parameters:
    print(parameter)
    i = i+1
    outDict_parameter = splitParameter(parameter)
    df_parameter2 = pd.DataFrame(outDict_parameter,index=[i])
    df_parameter = df_parameter.append(df_parameter2)   
    


# In[20]:

df_parameter.head()


# In[21]:

df_parameter.shape


# In[22]:

df_complete = df.merge(df_parameter,how='left',left_on='parameter',right_on='parameter')


# In[26]:

df_complete.shape


# In[23]:

df_complete.head()


# In[24]:

df_complete.tail()


# In[28]:

list(df_complete.columns.values)


# Missing : NoData 
# Missing : exportDescription
