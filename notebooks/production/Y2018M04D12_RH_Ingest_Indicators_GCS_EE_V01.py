
# coding: utf-8

# In[1]:

""" Ingest the remaining indicators into earth engine.
-------------------------------------------------------------------------------
ingest the converted indicators that were shared in ascii format to Earth
Engine. 

Requirements:
    Authorize earthengine by running in your terminal: earthengine 
                                                       authenticate

    you need to have access to the WRI-Aquaduct (yep a Google employee made a
    typo) bucket to ingest the data. Rutger can grant access to write to this 
    folder. 

    Have access to the Google Cloud Storage Bucker

Make sure to set the project to Aqueduct30 by running
`gcloud config set project aqueduct30`

Code follows the Google for Python Styleguide. Exception are the scripts that 
use earth engine since this is camelCase instead of underscore.

Author: Rutger Hofste
Date: 20180412
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    TESTING (Boolean) : Toggle Testing Mode.
    OVERWRITE (Boolean) : Overwrite old folder !CAUTION!
    SCRIPT_NAME (string) : Script name.
    GCS_BASE (string) : Google Cloud Storage namespace.
    EE_BASE (string) : Earth Engine folder to store the imageCollections
    OUTPUT_FILE_NAME (string) : File Name for a csv file containing the failed tasks. 
    S3_OUTPUT_PATH (string) : Amazon S3 Output path.

Returns:


"""

# Input Parameters
TESTING = 0
OVERWRITE = 0 # !CAUTION!
SCRIPT_NAME = "Y2018M04D12_RH_Ingest_Indicators_GCS_EE_V01"
GCS_BASE = "gs://aqueduct30_v01/Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V02/"
EE_BASE = "projects/WRI-Aquaduct/PCRGlobWB20V08"
OUTPUT_FILE_NAME = "df_errorsV01.csv"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/{}/output".format(SCRIPT_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

#imports
import subprocess
import datetime
import os
import time
import re
import pandas as pd
from datetime import timedelta
import aqueduct3


# In[ ]:




# In[4]:

# ETL


# Not required if order of script is unchanged
"""
if OVERWRITE:
    command = "earthengine rm -r {}".format(EE_BASE)
    print(command)
    subprocess.check_output(command,shell=True)

command = "earthengine create folder {}".format(EE_BASE)
print(command)
subprocess.check_output(command,shell=True)
"""


ec2_output_path = "/volumes/data/{}/output".format(SCRIPT_NAME)


# In[5]:

get_ipython().system('mkdir -p {ec2_output_path}')


# In[6]:

def get_GCS_keys(GCS_BASE):
    """ get list of keys from Google Cloud Storage
    -------------------------------------------------------------------------------
    
    Args:
        GCS_BASE (string) : Google Cloud Storage namespace containing files.
        
    Returns:
        df (pd.DataFrame) : DataFrame with properties useful to Aqueduct. 
    
    """
    command = "/opt/google-cloud-sdk/bin/gsutil ls {}".format(GCS_BASE)
    keys = subprocess.check_output(command,shell=True)
    keys = keys.decode('UTF-8').splitlines()
    
    df = keys_to_df(keys)
    
    return df

def keys_to_df(keys):
    """ helper function for 'get_GCS_keys'
    
        
    Args:
        keys (list) : list of strings with keys.
        
    Returns:
        df (pd.DataFrame) : Pandas DataFrame with all relvant properties for
                            Aqueduct 3.0.
    """
    
    df = pd.DataFrame()
    i = 0
    for key in keys:
        i = i+1
        schema = ["geographic_range","indicator","spatial_resolution","temporal_range_min","temporal_range_max"]
        out_dict = aqueduct3.split_key(key,schema)
        df2 = pd.DataFrame(out_dict,index=[i])
        df = df.append(df2)    
    return df


# In[7]:

# Script
df = get_GCS_keys(GCS_BASE)
df.shape


# In[8]:

df


# In[9]:

df_complete = df.copy()


# In[10]:

df_complete["nodata_value"] = -9999
df_complete["ingested_by"] ="RutgerHofste"
df_complete["exportdescription"] = df_complete["indicator"]
df_complete["script_used"] = SCRIPT_NAME
df_complete = df_complete.apply(pd.to_numeric, errors='ignore')


# In[11]:

df_complete


# In[12]:

if TESTING:
    df_complete = df_complete[1:3]


# In[13]:

df_complete


# In[14]:

df_errors = pd.DataFrame()
start_time = time.time()
for index, row in df_complete.iterrows():
    elapsed_time = time.time() - start_time 
    print(index,"%.2f" %((index/df_complete.shape[0])*100), "elapsed: ", str(timedelta(seconds=elapsed_time)))
    
    geotiff_gcs_path = GCS_BASE + row.file_name + "." + row.extension
    output_ee_asset_id = EE_BASE +"/"+ row.file_name
    properties = row.to_dict()
    
    df_errors2 = aqueduct3.upload_geotiff_to_EE_imageCollection(geotiff_gcs_path, output_ee_asset_id, properties,index)
    df_errors = df_errors.append(df_errors2)


# In[15]:

df_errors.to_csv("{}/{}".format(ec2_output_path,OUTPUT_FILE_NAME))


# In[16]:

df_retry = df_errors.loc[df_errors['error'] != 0]
uniques = df_errors["error"].unique()


# In[17]:

df_retry


# In[20]:

df_retry.loc[1]


# In[18]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

