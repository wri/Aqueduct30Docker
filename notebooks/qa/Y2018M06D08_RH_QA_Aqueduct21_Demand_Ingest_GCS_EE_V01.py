
# coding: utf-8

# In[1]:

""" QA for water stress in several basin
-------------------------------------------------------------------------------

This first step was done manually using arcMap since the .gdb is not ogc 
compliant.

Input data: wri-projects/Aqueduct2x/Aqueduct21Data/demand
Algorithm used: ArcMap batch copy raster

Uploaded to:

s3://wri-projects/Aqueduct30/qaData/Y2018M06D08_RH_QA_Aqueduct21_Demand_Ingest_GCS_EE_V01/
output_V01/

and copied to 

gs://aqueduct30_v01/Y2018M06D08_RH_QA_Aqueduct21_Demand_Ingest_GCS_EE_V01/
output_V01/

Author: Rutger Hofste
Date: 20180604
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M06D08_RH_QA_Aqueduct21_Demand_Ingest_GCS_EE_V01'
OUTPUT_VERSION = 2

OUTPUT_FILE_NAME = "df_errors.csv"

SEPARATOR = "_|-"

SCHEMA = ["indicator"]

EXTRA_PROPERTIES = {"nodata_value":-1.79769e+308,
                    "ingested_by" : "RutgerHofste",
                    "script_used": SCRIPT_NAME,
                    "output_version":OUTPUT_VERSION}



GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2018M06D08_RH_QA_Aqueduct21_Demand_Ingest_GCS_EE_V01/output_V01/"

ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/qaData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)



print("Input gcs: " +  GCS_INPUT_PATH+
      "\nOutput ee: "+ ee_output_path)


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

if OVERWRITE_OUTPUT:
    command = "earthengine rm -r {}".format(ee_output_path)
    subprocess.check_output(command,shell=True)
    


# In[5]:

keys = aqueduct3.get_GCS_keys(GCS_INPUT_PATH)


# In[6]:

def main():
    start_time = time.time()
    get_ipython().system('mkdir -p {ec2_output_path}')
    keys = aqueduct3.get_GCS_keys(GCS_INPUT_PATH)
    # Limiting to tiffs for now.
    keys = list(filter(lambda x: x.endswith('.tif'), keys))
    df = aqueduct3.keys_to_df(keys,SEPARATOR,SCHEMA)
    df = df.assign(**EXTRA_PROPERTIES)
    df["exportdescription"] = df["file_name"]
    df = df.apply(pd.to_numeric, errors='ignore')

    # Earth Engine Preparations
    # Create folder (create parent if non existent)
    
    result = aqueduct3.earthengine.create_ee_folder_recursive(ee_output_path,OVERWRITE_OUTPUT)
    
    df_errors = pd.DataFrame()
    for index, row in df.iterrows():
        geotiff_gcs_path = GCS_INPUT_PATH + row.file_name + "." + row.extension
        output_ee_asset_id = ee_output_path + "/" + row.file_name
        properties = row.to_dict()
        df_errors2 = aqueduct3.upload_geotiff_to_EE_imageCollection(geotiff_gcs_path, output_ee_asset_id, properties,index)
        df_errors = df_errors.append(df_errors2) 
    df_errors.to_csv("{}/{}".format(ec2_output_path,OUTPUT_FILE_NAME))
    get_ipython().system('aws s3 cp  {ec2_output_path} {s3_output_path} --recursive')
    return df,df_errors
                             
if __name__ == "__main__":
    df,df_errors = main()


# In[7]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 
