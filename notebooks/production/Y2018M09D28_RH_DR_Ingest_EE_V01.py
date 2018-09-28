
# coding: utf-8

# In[1]:

""" Ingest drought risk data in earthengine.
-------------------------------------------------------------------------------

files have been manually renamed to 
hazard
exposure
vulnaribility
risk
desertcoldareamask


Author: Rutger Hofste
Date: 201809028
Kernel: python27
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    



"""

SCRIPT_NAME = "Y2018M09D28_RH_DR_Ingest_EE_V01"
OUTPUT_VERSION = 3

OUTPUT_FILE_NAME = "df_errors.csv"
SEPARATOR = "_|-"

NODATA_VALUE =-9999

EXTRA_PROPERTIES = {"nodata_value":NODATA_VALUE ,
                    "ingested_by" : "RutgerHofste",
                    "script_used": SCRIPT_NAME,
                    "output_version":OUTPUT_VERSION,
                    "doi":"https://doi.org/10.1016/j.gloenvcha.2016.04.012"}


S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/rawData/JRC/DroughtRisk/data/raw/"
GCS_OUTPUT_PATH = "gs://aqueduct30_v01/{}/".format(SCRIPT_NAME)


ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("S3_INPUT_PATH: " + S3_INPUT_PATH +
      "\nec2_output_path" + ec2_output_path +
      "\nGCS_OUTPUT_PATH: " + GCS_OUTPUT_PATH +
      "\nee_output_path: " + ee_output_path )


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
import numpy as np
from datetime import timedelta
from osgeo import gdal
import aqueduct3


# In[4]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive')


# In[6]:

files = os.listdir(ec2_input_path)


# In[7]:

# Convert geotiff to all Float64 and with -9999 as NoData value
for one_file in files:
    input_file_path = os.path.join(ec2_input_path,one_file)
    output_file_path = os.path.join(ec2_output_path,one_file)
    xsize,ysize,geotransform,geoproj,Z = aqueduct3.read_gdal_file(input_file_path)
    
    Z[Z<-9990]= NODATA_VALUE 
    datatype=gdal.GDT_Float64
    
    aqueduct3.write_geotiff(output_file_path,geotransform,geoproj,Z,NODATA_VALUE,datatype)
    


# In[8]:

get_ipython().system('gsutil cp -r {ec2_output_path} {GCS_OUTPUT_PATH}')


# In[9]:

aqueduct3.earthengine.create_ee_folder_recursive(ee_output_path)


# In[10]:

files = os.listdir(ec2_input_path)


# In[11]:

for one_file in files:    
    filename, extension = one_file.split(".")
    geotiff_gcs_path = GCS_OUTPUT_PATH  + "output_V{:02.0f}".format(OUTPUT_VERSION) + "/" + one_file
    output_ee_asset_id = ee_output_path + "/" + filename
    metadata_command = aqueduct3.dictionary_to_EE_upload_command(EXTRA_PROPERTIES)
    command = "/opt/anaconda3/bin/earthengine upload image --asset_id {} {}".format(output_ee_asset_id,geotiff_gcs_path)
    command = command + metadata_command
    print(command)
    response = subprocess.check_output(command, shell=True)


# In[12]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:00:38.999067

# In[ ]:



