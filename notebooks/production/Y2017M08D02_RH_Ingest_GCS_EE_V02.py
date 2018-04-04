
# coding: utf-8

# In[8]:

""" Ingest data on Google Earth Engine
-------------------------------------------------------------------------------
This notebook will upload the geotiff files from the Google Cloud Storage to
the WRI/aqueduct earthengine bucket. 

Requirements:
    Authorize earthengine by running in your terminal: earthengine 
                                                       authenticate

    you need to have access to the WRI-Aquaduct (yep a Google employee made a
    typo) bucket to ingest the data. Rutger can grant access to write to this 
    folder. 

    Have access to the Google Cloud Storage Bucker

Run in your terminal `gcloud config set project aqueduct30`

Code follows the Google for Python Styleguide. Exception are the scripts that 
use earth engine since this is camelCase instead of underscore.


Author: Rutger Hofste
Date: 20170802
Kernel: python36
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name


Returns:


"""

# Input Parameters

SCRIPT_NAME = "Y2017M08D02_RH_Ingest_GCS_EE_V02"
GCS_BASE = "gs://aqueduct30_v01/Y2017M08D02_RH_Upload_to_GoogleCS_V02/"
EE_BASE = "projects/WRI-Aquaduct/PCRGlobWB20V08"

# Output Parameters


# In[9]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[10]:

# Imports
import subprocess
import datetime
import os
import time
from datetime import timedelta
import re
import pandas as pd


# In[12]:

# ETL

command = ("earthengine create folder %s") %EE_BASE
print(command)
subprocess.check_output(command,shell=True)


# In[31]:

# Functions

def split_key(key):
    """ Split key into dictionary
    -------------------------------------------------------------------------------
    WARNING: This function is dependant on the name convention of PCRGLOBWB
    Do not use with other keys
    
    Args:
        key (string) : key containing information about parameter, year month etc.
        
    Returns:
        out_dict (dictionary): Dictionary containing all information contained
                               in key.      

    """
    
    # will yield the root file code and extension of a set of keys
    prefix, extension = key.split(".")
    file_name = prefix.split("/")[-1]
    parameter = file_name[:-12]
    month = file_name[-2:] #can also do this with regular expressions if you like
    year = file_name[-7:-3]
    identifier = file_name[-11:-8]
    out_dict = {"file_name":file_name,"extension":extension,"parameter":parameter,"month":month,"year":year,"identifier":identifier}
    return out_dict

def split_parameter(parameter):
    """Split parameter 
    -------------------------------------------------------------------------------
    WARNING: This function is dependant on the name convention of PCRGLOBWB
    Do not use with other keys
    
    Args:
        parameter (string) : parameter string.
    
    Returns:
        out_dict (dictionary) : dictionary containing all information contained
                                in parameter key.
    
    """
    
    values = re.split("_|-", parameter) #soilmoisture uses a hyphen instead of underscore between the years
    keys = ["geographic_range","temporal_range","indicator","temporal_resolution","units","spatial_resolution","temporal_range_min","temporal_range_max"]
    # ['global', 'historical', 'PDomWN', 'month', 'millionm3', '5min', '1960', '2014']
    out_dict = dict(zip(keys, values))
    out_dict["parameter"] = parameter
    return out_dict

def upload_geotiff_to_EE_imageCollection(geotiff_gcs_path,output_ee_asset_id,properties):
    """Upload geotiff to earthengine image collection
    -------------------------------------------------------------------------------
    
    Ingest a geotiff to earth engine imageCollection and set metadata. A dictionary
    of properties will be used to define the metadata of the image.
    
    Args:
        geotiff_gcs_path (string) : Google Cloud Storage path of geotiff.
        output_ee_asset_id (string) : Earth Engine output asset id. Full path 
                                      including imageCollection asset id.
        properties (dictionary) : Dictionary with metadata. the 'nodata_value' key
                                  can be used to set a NoData Value.
        
    
    Returns:
        command (string) : command string parsed to subprocess module.
        
    
    TODO:
    update function to work with dictionary of properties
    
    """
    
    
    
    
    
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


def dictionary_to_upload_command(d):
    """ Convert a dictionary to command that can be appended to upload command
    -------------------------------------------------------------------------------
    
    Args:
        d (dictionary) : Dictionary with metadata.
    
    Returns:
        command (string) : string to append to upload string.    
    
    """
    command = ""
    for key, value in d.items():
            
        if key == "nodata_value":
            command = command + " --nodata_value={}".format(value)
        else:
            command = command + " -p {}={}".format(key,value)

    return command

def create_imageCollection(ic_id):
    """ Creates an imageCollection using command line
    -------------------------------------------------------------------------------
    Args:
        ic_id (string) : asset_id of image Collection.
        
    Returns:
        command (string) : command parsed to subprocess module 
        result (string) : subprocess result 
        
    """
    command = "earthengine create collection {}".format(ic_id)
    result = subprocess.check_output(command,shell=True)
    return command, result


# ## Script

# In[15]:

command = "/opt/google-cloud-sdk/bin/gsutil ls {}".format(GCS_BASE)
keys = subprocess.check_output(command,shell=True)
keys = keys.decode('UTF-8').splitlines()


# In[24]:

df = pd.DataFrame()
i = 0
for key in keys:
    i = i+1
    out_dict = split_key(key)
    df2 = pd.DataFrame(out_dict,index=[i])
    df = df.append(df2)    


# In[25]:

df.head()


# In[26]:

df.tail()


# In[27]:

df.shape


# In[28]:

parameters = df.parameter.unique()


# In[29]:

print(parameters)


# In[33]:

for parameter in parameters:
    ic_id = EE_BASE + "/" + parameter
    command, result = create_imageCollection(ic_id)
    print(command)
    


# Now that the folder and collections have been created we can start ingesting the data. It is crucial to store the relevant metadata with the images. 

# In[36]:

df_parameter = pd.DataFrame()
i = 0
for parameter in parameters:
    i = i+1
    out_dict_parameter = split_parameter(parameter)
    df_parameter2 = pd.DataFrame(out_dict_parameter,index=[i])
    df_parameter = df_parameter.append(df_parameter2)   
    


# In[37]:

df_parameter.head()


# In[38]:

df_parameter.tail()


# In[39]:

df_parameter.shape


# In[40]:

df_complete = df.merge(df_parameter,how='left',left_on='parameter',right_on='parameter')


# Adding NoData value, ingested_by and exportdescription

# In[41]:

df_complete["nodata"] = -9999
df_complete["ingested_by"] ="RutgerHofste"
df_complete["exportdescription"] = df_complete["indicator"] + "_" + df_complete["temporal_resolution"]+"Y"+df_complete["year"]+"M"+df_complete["month"]


# In[42]:

df_complete.head()


# In[43]:

df_complete.tail()


# In[27]:

list(df_complete.columns.values)


# In[46]:

df_errors = pd.DataFrame()
start_time = time.time()
for index, row in df_complete.iterrows():
    elapsed_time = time.time() - start_time 
    print(index,"%.2f" %((index/9289.0)*100), "elapsed: ", str(timedelta(seconds=elapsed_time)))
    
    upload_geotiff_to_EE_imageCollection(geotiff_gcs_path,output_ee_asset_id,properties)
    
    #df_errors2 = uploadEE(index,row)
    #df_errors = df_errors.append(df_errors2)
    
    


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

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

