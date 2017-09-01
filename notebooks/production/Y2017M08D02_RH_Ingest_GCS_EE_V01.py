
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
from datetime import timedelta
import re
import pandas as pd


# ## Settings

# In[2]:

GCS_BASE = "gs://aqueduct30_v01/Y2017M08D02_RH_Upload_to_GoogleCS_V01/"


# In[3]:

EE_BASE = "projects/WRI-Aquaduct/PCRGlobWB20V07"


# ## Functions

# In[4]:

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
    #values = parameter.split("_")
    values = re.split("_|-", parameter) #soilmoisture uses a hyphen instead of underscore between the years
    keys = ["geographic_range","temporal_range","indicator","temporal_resolution","units","spatial_resolution","temporal_range_min","temporal_range_max"]
    # ['global', 'historical', 'PDomWN', 'month', 'millionm3', '5min', '1960', '2014']
    outDict = dict(zip(keys, values))
    outDict["parameter"] = parameter
    return outDict


# ## Script

# In[5]:

command = ("earthengine create folder %s") %EE_BASE


# In[6]:

print(command)


# In[7]:

subprocess.check_output(command,shell=True)


# In[8]:

command = ("/opt/google-cloud-sdk/bin/gsutil ls %s") %(GCS_BASE)


# In[9]:

keys = subprocess.check_output(command,shell=True)


# In[10]:

keys = keys.decode('UTF-8').splitlines()


# Removing first item from the list. The first item contains a folder without file name

# In[11]:

keys2 = keys[1:]


# In[12]:

df = pd.DataFrame()
i = 0
for key in keys2:
    i = i+1
    outDict = splitKey(key)
    df2 = pd.DataFrame(outDict,index=[i])
    df = df.append(df2)    


# In[13]:

df.head()


# In[14]:

df.tail()


# In[15]:

df.shape


# In[16]:

parameters = df.parameter.unique()


# In[17]:

print(parameters)


# We will store the geotiff images of each NetCDF4 file in imageCollections. The imageCollections will have the same name and content as the original NetCDF4files. 
# 

# In[18]:

for parameter in parameters:
    eeLocation = EE_BASE + "/" + parameter
    command = ("earthengine create collection %s") %eeLocation
    # Uncomment the following command if you run this script for the first time
    subprocess.check_output(command,shell=True)
    print(command)
    


# Now that the folder and collections have been created we can start ingesting the data. It is crucial to store the relevant metadata with the images. 

# In[19]:

df_parameter = pd.DataFrame()
i = 0
for parameter in parameters:
    i = i+1
    outDict_parameter = splitParameter(parameter)
    df_parameter2 = pd.DataFrame(outDict_parameter,index=[i])
    df_parameter = df_parameter.append(df_parameter2)   
    


# In[20]:

df_parameter.head()


# In[21]:

df_parameter.tail()


# In[22]:

df_parameter.shape


# In[23]:

df_complete = df.merge(df_parameter,how='left',left_on='parameter',right_on='parameter')


# Adding NoData value, ingested_by and exportdescription

# In[24]:

df_complete["nodata"] = -9999
df_complete["ingested_by"] ="RutgerHofste"
df_complete["exportdescription"] = df_complete["indicator"] + "_" + df_complete["temporal_resolution"]+"Y"+df_complete["year"]+"M"+df_complete["month"]


# In[25]:

df_complete.head()


# In[26]:

df_complete.tail()


# In[27]:

list(df_complete.columns.values)


# In[30]:

def uploadEE(index,row):
    target = EE_BASE +"/"+ row.parameter + "/" + row.fileName
    source = GCS_BASE + row.fileName + "." + row.extension
    metadata = "--nodata_value=%s --time_start %s-%s-01 -p extension=%s -p filename=%s -p identifier=%s -p month=%s -p parameter=%s -p year=%s -p geographic_range=%s -p indicator=%s -p spatial_resolution=%s -p temporal_range=%s -p temporal_range_max=%s -p temporal_range_min=%s -p temporal_resolution=%s -p units=%s -p ingested_by=%s -p exportdescription=%s" %(row.nodata,row.year,row.month,row.extension,row.fileName,row.identifier,row.month,row.parameter,row.year,row.geographic_range,row.indicator,row.spatial_resolution,row.temporal_range,row.temporal_range_max,row.temporal_range_min, row.temporal_resolution, row.units, row.ingested_by, row.exportdescription)
    command = "/opt/anaconda3/bin/earthengine upload image --asset_id %s %s %s" % (target, source,metadata)
    try:
        response = subprocess.check_output(command, shell=True)
        outDict = {"command":command,"response":response,"error":0}
        df_errors2 = pd.DataFrame(outDict,index=[index])
        pass
    except:
        try:
            outDict = {"command":command,"response":response,"error":1}
        except:
            outDict = {"command":command,"response":-9999,"error":2}
        df_errors2 = pd.DataFrame(outDict,index=[index])
        print("error")
    return df_errors2



# In[ ]:

df_errors = pd.DataFrame()
start_time = time.time()
for index, row in df_complete.iterrows():
    elapsed_time = time.time() - start_time 
    print(index,"%.2f" %((index/9289.0)*100), "elapsed: ", str(timedelta(seconds=elapsed_time)))
    df_errors2 = uploadEE(index,row)
    df_errors = df_errors.append(df_errors2)
    
    


# In[37]:

get_ipython().system('mkdir /volumes/data/temp')


# In[38]:

df_errors.to_csv("/volumes/data/temp/df_errors.csv")


# In[39]:

get_ipython().system('aws s3 cp  /volumes/data/temp/df_errors.csv s3://wri-projects/Aqueduct30/temp/df_errors.csv')


# Retry the ones with errors

# In[40]:

df_retry = df_errors.loc[df_errors['error'] != 0]


# In[41]:

for index, row in df_retry.iterrows():
    response = subprocess.check_output(row.command, shell=True)
    


# In[53]:

uniques = df_errors["error"].unique()


# In[ ]:



