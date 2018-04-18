
# coding: utf-8

# In[1]:

""" Ingest the non-time-series indicators into earth engine. 
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
    PREVIOUS_SCRIPT_NAME (string) : Previous script name. 
    INPUT_VERSION (integer) : Input version.
    EE_OUTPUT_VERSION (integer) : Output version for earthengine. 
    OUTPUT_VERSION (integer) : Output version.    
    OUTPUT_FILE_NAME (string) : File Name for a csv file containing the failed tasks. 

Returns:


"""

# Input Parameters
TESTING = 0
OVERWRITE = 0 # !CAUTION will overwrite entire EE_FOLDER!!
SCRIPT_NAME = "Y2018M04D12_RH_Ingest_Indicators_GCS_EE_V01"
PREVIOUS_SCRIPT_NAME = "Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V02"

INPUT_VERSION = 7
EE_OUTPUT_VERSION = 9
OUTPUT_VERSION = 1

OUTPUT_FILE_NAME = "df_errors.csv"

SEPARATOR = "_|-"

SCHEMA = ["geographic_range",
    "temporal_range",
    "indicator",
    "temporal_resolution",
    "unit",
    "spatial_resolution",
    "temporal_range_min",
    "temporal_range_max"]

EXTRA_PROPERTIES = {"nodata_value":-9999,
                    "ingested_by" : "RutgerHofste",
                    "script_used": SCRIPT_NAME,
                    "output_version":OUTPUT_VERSION}



gcs_input_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(PREVIOUS_SCRIPT_NAME,INPUT_VERSION)
ee_output_path = "projects/WRI-Aquaduct/PCRGlobWB20V{:02.0f}".format(EE_OUTPUT_VERSION)
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

def main():
    start_time = time.time()
    get_ipython().system('mkdir -p {ec2_output_path}')
    keys = aqueduct3.get_GCS_keys(gcs_input_path)
    df = aqueduct3.keys_to_df(keys,SEPARATOR,SCHEMA)
    df = df.assign(**EXTRA_PROPERTIES) #Python >3.5
    df["exportdescription"] = df["indicator"] + "_" + df["temporal_resolution"]
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


# In[4]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:00:29.149991

# In[ ]:



