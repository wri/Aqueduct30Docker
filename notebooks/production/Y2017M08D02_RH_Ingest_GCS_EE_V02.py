
# coding: utf-8

# In[1]:

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

SEPARATOR = "_|-"
SCHEMA = ["geographic_range",
     "temporal_range",
     "indicator",
     "temporal_resolution",
     "unit",
     "spatial_resolution",
     "temporal_range_min",
     "temporal_range_max"]

extra_properties = {"nodata_value":-9999,
                    "ingested_by" : "RutgerHofste",
                    "script_used": SCRIPT_NAME}

# ETL
gcs_input_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(PREVIOUS_SCRIPT_NAME,INPUT_VERSION)
ee_output_path = "projects/WRI-Aquaduct/PCRGlobWB20V{:02.0f}".format(OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

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


# In[20]:

# Imports
import subprocess
import datetime
import os
import time
import re
import pandas as pd
from datetime import timedelta
import aqueduct3


# ETL
def main():
    start_time = time.time()
    

    if OVERWRITE:
        command = "earthengine rm -r {}".format(ee_output_path)
        print(command)
        subprocess.check_output(command,shell=True)

    command = "earthengine create folder {}".format(ee_output_path)
    print(command)
    subprocess.check_output(command,shell=True)

    # Script

    
    keys = aqueduct3.get_GCS_keys(gcs_input_path)
    df = aqueduct3.keys_to_df(keys,separator,schema)

    df = df.assign(**extra_properties)
    df["exportdescription"] = df["indicator"] + "_" + df["temporal_resolution"]+"Y"+df["year"]+"M"+df["month"]
    df = df.apply(pd.to_numeric, errors='ignore')
    
    # Create ImageCollections
    parameters = df.parameter.unique()
    for parameter in parameters:
        ic_id = ee_output_path + "/" + parameter
        command, result = aqueduct3.create_imageCollection(ic_id)
        print(command,result)

    if TESTING:
        df_complete = df_complete[1:3]
        
        
    df_errors = pd.DataFrame()
    
    for index, row in df_complete.iterrows():
        elapsed_time = time.time() - start_time 
        print(index,"%.2f" %((index/df_complete.shape[0])*100), "elapsed: ", str(timedelta(seconds=elapsed_time)))
        geotiff_gcs_path = GCS_BASE + row.file_name + "." + row.extension
        output_ee_asset_id = EE_BASE +"/"+ row.parameter + "/" + row.file_name
        properties = row.to_dict()

        df_errors2 = aqueduct3.upload_geotiff_to_EE_imageCollection(geotiff_gcs_path, output_ee_asset_id, properties,index)
        df_errors = df_errors.append(df_errors2)    



if __name__ == "__main__":
    main()


# Adding NoData value, ingested_by and exportdescription

# In[ ]:




# In[ ]:

get_ipython().system('mkdir -p {ec2_output_path}')


# In[ ]:

df_errors.to_csv("{}/{}".format(ec2_output_path,OUTPUT_FILE_NAME))


# In[ ]:

get_ipython().system('aws s3 cp  {ec2_output_path} {S3_OUTPUT_PATH} --recursive')


# Retry the ones with errors

# In[ ]:

df_retry = df_errors.loc[df_errors['error'] != 0]


# In[ ]:

for index, row in df_retry.iterrows():
    response = subprocess.check_output(row.command, shell=True)
    


# In[ ]:

uniques = df_errors["error"].unique()


# In[ ]:

df_retry


# In[ ]:

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

