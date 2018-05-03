
# coding: utf-8

# In[1]:

""" Ingest hydrobasin rasters in earthengine.
-------------------------------------------------------------------------------
Ingests rasterized hydrobasin geotiffs in earthengine.


Author: Rutger Hofste
Date: 20180420
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name.
    PREVIOUS_SCRIPT_NAME (string) : Previous script name.
    INPUT_VERSION (integer) : Input version.
    OUTPUT_VERSION (integer) : Output version.
    OUTPUT_FILE_NAME (string) : Ouput filename of error dataframe. 
        make sure to add a '.csv' extension.
    X_DIMENSION_5MIN (integer) : Longitudinal dimension at 5 arc minutes.
    Y_DIMENSION_5MIN (integer) : Latitudinal dimension at 5 arc minutes.
    X_DIMENSION_30S (integer) : Longitudinal dimension at 30 arc seconds.
    Y_DIMENSION_30S (integer) : Latitudinal dimension at 30 arc seconds.
    SEPARATOR (regex) : Regular expression used to convert filename into 
        metadata.
    SCHEMA (list) : List of strings with metadata keys. 
    EXTRA_PROPERTIES (dictionary) :  Dictionary with additional key value pairs
        that will be stored as properties in earthengine image. 


Returns:


"""

# Input Parameters

OVERWRITE = 0 

SCRIPT_NAME = "Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01"
PREVIOUS_SCRIPT_NAME = "Y2017M08D02_RH_Merge_HydroBasins_V02"
INPUT_VERSION = 4
OUTPUT_VERSION = 2
OUTPUT_FILE_NAME = "df_errors.csv"

X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160
X_DIMENSION_30S = 43200
Y_DIMENSION_30S = 21600

SEPARATOR = "_|-"
SCHEMA = ["indicator",
          "pfafstetter_level",
          "WWF_version",
          "geographic_range",
          "algorithm_used_for_merge",
          "spatial_resolution",
          "output_version"]

EXTRA_PROPERTIES = {"WWF_lakes":"standard_no_lakes",
                    "nodata_value":-9999,
                    "ingested_by" : "RutgerHofste",
                    "script_used": SCRIPT_NAME,
                    "output_version":OUTPUT_VERSION}
                    

# ETL
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
gcs_input_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(PREVIOUS_SCRIPT_NAME,INPUT_VERSION)
ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input gcs: " +  gcs_input_path+
      "\nOutput ec2: " + ec2_output_path +
      "\nOutput ee: " + ee_output_path +
      "\nOutput S3: " + s3_output_path)


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
    # Limiting to tiffs for now.
    keys = list(filter(lambda x: x.endswith('.tif'), keys))
    df = aqueduct3.keys_to_df(keys,SEPARATOR,SCHEMA)
    df = df.assign(**EXTRA_PROPERTIES)
    df["exportdescription"] = df["file_name"]
    df = df.apply(pd.to_numeric, errors='ignore')

    # Earth Engine Preparations
    # Create folder (create parent if non existent)
    if OVERWRITE:
        command = "earthengine rm -r {}".format(ee_output_path)
        print(command)
        subprocess.check_output(command,shell=True)

    command = "earthengine create folder {}".format(ee_output_path)
    print(command)
    subprocess.check_output(command,shell=True)
    
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


# In[5]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:18.907899

# In[ ]:



