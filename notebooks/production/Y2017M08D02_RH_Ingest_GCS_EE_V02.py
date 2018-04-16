
# coding: utf-8

# In[7]:

""" Ingest PCRGLOBWB timeseries data on Google Earth Engine
-------------------------------------------------------------------------------
This notebook will upload the geotiff files from the Google Cloud Storage to
the WRI/aqueduct earthengine bucket. An errorlog will be stored on Amazon S3.

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
Date: 20170802
Kernel: python27
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:    
    TESTING (Boolean) : Toggle Testing Mode.
    OVERWRITE (Boolean) : Overwrite old folder !CAUTION!
    SCRIPT_NAME (string) : Script name.
    
    PREVIOUS_SCRIPT_NAME (string) : Previous script name. 
    INPUT_VERSION (integer) : Input version. 
    
    OUTPUT_VERSION (integer) : Output version. 
    
    OUTPUT_FILE_NAME (string) : File Name for a csv file containing the failed tasks. 

Returns:


"""

# Input Parameters
TESTING = 1
OVERWRITE = 0 # !CAUTION!
SCRIPT_NAME = "Y2017M08D02_RH_Ingest_GCS_EE_V02"
PREVIOUS_SCRIPT_NAME = "Y2017M07D31_RH_Convert_NetCDF_Geotiff_V02"

INPUT_VERSION = 1
OUTPUT_VERSION = 9

OUTPUT_FILE_NAME = "df_errorsV01.csv"

# ETL
gcs_input_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(PREVIOUS_SCRIPT_NAME,INPUT_VERSION)
ee_output_path = "projects/WRI-Aquaduct/PCRGlobWB20V{:02.0f}".format(OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input gcs: " +  gcs_input_path +
      "\nOutput ee: " + ee_output_path +
      "\nOutput S3: " + s3_output_path )


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[1]:

# Imports
import subprocess
import datetime
import os
import time
import re
import pandas as pd
from datetime import timedelta
import aqueduct3


# In[4]:

# ETL

ec2_output_path = "/volumes/data/{}/output".format(SCRIPT_NAME)

if OVERWRITE:
    command = "earthengine rm -r {}".format(EE_BASE)
    print(command)
    subprocess.check_output(command,shell=True)

command = "earthengine create folder {}".format(EE_BASE)
print(command)
subprocess.check_output(command,shell=True)


# In[114]:

def split_key(key,schema,separator='_|-'):
    """ Split a key using the PCRGLOBWB Schema to get the metadata. 
    -------------------------------------------------------------------------------
    PCRGLOBWB uses a semi-standardized naming convention. Geotiffs cannot store
    metadata but a straight-forward solution is to store metadata in the filename. 
    
    the naming convention used by the University of Utrecht uses hyphens and 
    underscores to separate metadata. Provide the structure of the filename in 
    list of strings format. 
    
    Example:    
    global_q4seasonalvariabilitywatersupply_5min_1960-2014.asc uses a schema of:
    ["geographic_range",
     "indicator",
     "spatial_resolution",
     "temporal_range_min",
     "temporal_range_max"]
     
    filename and extension are stored as extra key value pairs in the output_dict.
    
    Args:
        key (string) : file path including extension
        schema (list) : list of strings.
        separator (regex) : separator used in filename e.g. '_','-' or '_|-' etc.
            defaults to '_|-'
    
    Returns:
        output_dict2 (dictionary) : dictionary with PCRGLOBWB shema, filename 
                                   and extension.     
    """
    
    # check if a pcrglobwb identifier is present.
    pattern = "I\d{3}Y\d{4}M\d{2}"
    
    pcrglobwb_dict = {}
    
    if re.search(pattern,key):
        result = re.search(pattern,key)
        pcrglobwb_id = result.group(0)
        pcrglobwb_dict["identifier"] = pcrglobwb_id[1:4]
        pcrglobwb_dict["year"] = pcrglobwb_id[5:9]
        pcrglobwb_dict["month"] = pcrglobwb_id[10:12]  
        key = re.sub(pattern,"",key)
        
    else:
        pass
    
    prefix, extension = key.split(".")
    file_name = prefix.split("/")[-1]
    values = re.split(separator, file_name)
    keyz = schema
    output_dict = dict(zip(keyz, values))
    output_dict["file_name"]=file_name
    output_dict["extension"]=extension
    
    # Python 3.5 or above 
    output_dict2 = {**output_dict, **pcrglobwb_dict}
    
    return output_dict2


# In[115]:

# Script
keys = aqueduct3.get_GCS_keys(gcs_input_path)


# In[119]:

key = keys[1]


# In[120]:

split_key(key)


# In[73]:

teststring = "global_historical_PLivWN_year_millionm3_5min_1960_2014I020Y1980M12.tif"


# In[97]:

pattern = "I\d{3}Y\d{4}M\d{2}."


# In[123]:

schema = ["geographic_range",
     "indicator",
     "spatial_resolution",
     "temporal_range_min",
     "temporal_range_max"]


# In[95]:

out_dict = preprocess_key(teststring,pattern)


# In[96]:

out_dict


# In[47]:

print(test)


# In[110]:

a = {"rutger":42,"test":1}


# In[111]:

b = {"blah":1,"foo":2}


# In[112]:

a.update(b)


# In[113]:

a


# In[24]:

re.split(pattern2,teststring)


# In[19]:

test = re.match(pattern,teststring)


# In[7]:

# Create ImageCollections
parameters = df.parameter.unique()
for parameter in parameters:
    ic_id = EE_BASE + "/" + parameter
    command, result = aqueduct3.create_imageCollection(ic_id)
    print(command,result)


# In[8]:

# Prepare Dataframe
df_parameter = pd.DataFrame()
i = 0
for parameter in parameters:
    i = i+1
    out_dict_parameter = split_parameter(parameter)
    df_parameter2 = pd.DataFrame(out_dict_parameter,index=[i])
    df_parameter = df_parameter.append(df_parameter2)   
    


# In[9]:

df_parameter.shape


# In[10]:

df_complete = df.merge(df_parameter,how='left',left_on='parameter',right_on='parameter')


# Adding NoData value, ingested_by and exportdescription

# In[11]:

df_complete["nodata_value"] = -9999
df_complete["ingested_by"] ="RutgerHofste"
df_complete["exportdescription"] = df_complete["indicator"] + "_" + df_complete["temporal_resolution"]+"Y"+df_complete["year"]+"M"+df_complete["month"]
df_complete["script_used"] = SCRIPT_NAME
df_complete = df_complete.apply(pd.to_numeric, errors='ignore')


# In[12]:

df_complete.head()


# In[13]:

df_complete.tail()


# In[14]:

list(df_complete.columns.values)


# In[15]:

if TESTING:
    df_complete = df_complete[1:3]


# In[16]:

df_errors = pd.DataFrame()
start_time = time.time()
for index, row in df_complete.iterrows():
    elapsed_time = time.time() - start_time 
    print(index,"%.2f" %((index/df_complete.shape[0])*100), "elapsed: ", str(timedelta(seconds=elapsed_time)))
    
    geotiff_gcs_path = GCS_BASE + row.file_name + "." + row.extension
    output_ee_asset_id = EE_BASE +"/"+ row.parameter + "/" + row.file_name
    properties = row.to_dict()
    
    df_errors2 = aqueduct3.upload_geotiff_to_EE_imageCollection(geotiff_gcs_path, output_ee_asset_id, properties,index)
    df_errors = df_errors.append(df_errors2)


# In[17]:

get_ipython().system('mkdir -p {ec2_output_path}')


# In[18]:

df_errors.to_csv("{}/{}".format(ec2_output_path,OUTPUT_FILE_NAME))


# In[19]:

get_ipython().system('aws s3 cp  {ec2_output_path} {S3_OUTPUT_PATH} --recursive')


# Retry the ones with errors

# In[20]:

df_retry = df_errors.loc[df_errors['error'] != 0]


# In[21]:

for index, row in df_retry.iterrows():
    response = subprocess.check_output(row.command, shell=True)
    


# In[22]:

uniques = df_errors["error"].unique()


# In[24]:

df_retry


# In[23]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:




# In[ ]:

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
    Do not use with other keys.
    
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
        out_dict = split_key(key)
        df2 = pd.DataFrame(out_dict,index=[i])
        df = df.append(df2)    
    return df

