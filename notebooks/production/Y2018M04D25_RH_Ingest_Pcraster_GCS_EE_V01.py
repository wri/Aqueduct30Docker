
# coding: utf-8

# In[1]:

""" Ingest pcraster generated files from Cloud Storage to Earth Engine.
-------------------------------------------------------------------------------
rasters uploaded include:
- streamorder 5min



Author: Rutger Hofste
Date: 20180502
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle Testing Mode.
    OVERWRITE (Boolean) : Overwrite old folder !CAUTION!
    SCRIPT_NAME (string) : Script name.
    PREVIOUS_SCRIPT_NAME (string) : Previous script name.
    INPUT_VERSION (integer) : Input version.
    OUTPUT_VERSION (integer) : Output version. 
    OUTPUT_FILE_NAME (string) : File Name for a csv file containing the failed tasks. 

    GCS_BASE (string) : Google Cloud Storage output namespace.   
    EE_BASE (string) : Earth Engine folder to store the assets.
    
    OUTPUT_FILE_NAME (string) : File Name for a csv file containing the failed tasks. 
    SEPARATOR (regex) : Regular expression of separators used in geotiff
      filenames.     
    SCHEMA (list) : A list of strings containing the schema. See 
      aqueduct3.split_key() for more info.
    EXTRA_PROPERTIES (Dictionary) : Extra properties to add to assets. nodata_value,
      script used are common properties.


Returns:


"""
TESTING = 0
OVERWRITE = 0 
SCRIPT_NAME = "Y2018M04D25_RH_Ingest_Pcraster_GCS_EE_V01"
PREVIOUS_SCRIPT_NAME = "Y2018M04D25_RH_Convert_Pcrasters_Map_Geotiff_V01"

INPUT_VERSION = 2
OUTPUT_VERSION = 1
OUTPUT_FILE_NAME = "df_errors.csv"

X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160

SEPARATOR = "_|-"
SCHEMA = ["geographic_range",
          "indicator",
          "unit",
          "spatial_resolution",
          "input_version"
          ]

EXTRA_PROPERTIES = {"nodata_value":-9999,
                    "ingested_by" : "RutgerHofste",
                    "script_used": SCRIPT_NAME,
                    "output_version":OUTPUT_VERSION}

gcs_input_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(PREVIOUS_SCRIPT_NAME,INPUT_VERSION)
ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)


print("Input gcs: " +  gcs_input_path +
      "\nOutput ee: " + ee_output_path +
      "\nOutput S3: " + s3_output_path +
      "\nOutput ec2: " + ec2_output_path)



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


# In[4]:

def main():
    start_time = time.time()
    get_ipython().system('mkdir -p {ec2_output_path}')
    keys = aqueduct3.get_GCS_keys(gcs_input_path)
    df = aqueduct3.keys_to_df(keys,SEPARATOR,SCHEMA)
    df = df.assign(**EXTRA_PROPERTIES) #Python >3.5
    
    # EXTRA FOR AUX FILES ONLY, replace nodata_value for ldd.
    df.loc[df['file_name'] == "global_lddsound_numpad_05min", "nodata_value"] = 255
    
    df["exportdescription"] = df["indicator"]
    df = df.apply(pd.to_numeric, errors='ignore')

    # Earth Engine Preparations
    # Create folder
    if OVERWRITE:
        command = "earthengine rm -r {}".format(ee_output_path)
        print(command)
        subprocess.check_output(command,shell=True)

    command = "earthengine create folder {}".format(ee_output_path)
    print(command)
    subprocess.check_output(command,shell=True)


    if TESTING:
            df = df[1:3] 

    df_errors = pd.DataFrame()
    for index, row in df.iterrows():
        elapsed_time = time.time() - start_time 
        print(index,"{:02.2f}".format((float(index)/df.shape[0])*100) + "elapsed: ", str(timedelta(seconds=elapsed_time)))

        geotiff_gcs_path = gcs_input_path + row.file_name + "." + row.extension
        output_ee_asset_id = ee_output_path + "/" + row.file_name
        properties = row.to_dict()

        df_errors2 = aqueduct3.upload_geotiff_to_EE_imageCollection(geotiff_gcs_path, output_ee_asset_id, properties,index)
        df_errors = df_errors.append(df_errors2) 

    # Storing error dataframe on ec2 and S3
    df_errors.to_csv("{}/{}".format(ec2_output_path,OUTPUT_FILE_NAME))
    get_ipython().system('aws s3 cp  {ec2_output_path} {s3_output_path} --recursive')

    # Retry Failed Tasks Once
    df_retry = df_errors.loc[df_errors['error'] != 0]
    for index, row in df_retry.iterrows():
        response = subprocess.check_output(row.command, shell=True)

    return df,df_errors

if __name__ == "__main__":
    df,df_errors = main()


# In[6]:

df


# In[5]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:00:10.314571
# 

# In[ ]:



