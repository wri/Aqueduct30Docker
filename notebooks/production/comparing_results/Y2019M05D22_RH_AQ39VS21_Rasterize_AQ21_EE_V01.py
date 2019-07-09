
# coding: utf-8

# In[1]:

""" Rasterize Aqueduct 21 and store to Google Cloud Storage.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20190522
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0

SCRIPT_NAME = "Y2019M05D22_RH_AQ39VS21_Rasterize_AQ21_EE_V01"
OUTPUT_VERSION = 3


GDAL_RASTERIZE_PATH = "/opt/anaconda3/envs/python35/bin/gdal_rasterize"
X_DIMENSION_30S = 43200
Y_DIMENSION_30S = 21600

S3_TABLE_INPUT_PATH = "s3://wri-projects/Aqueduct2x/Aqueduct21Data/aqueduct_global_maps_21_shp"
TABLE_INPUT_FILE_NAME = "aqueduct_global_dl_20150409.shp"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_output_path =  "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 

GCS_OUTPUT_PATH = "gs://aqueduct30_v01/{}/".format(SCRIPT_NAME)


INDICATORS = ["DEFAULT",#Overall Water Risk
              "BWS_s", #Baseline Water Stress
              "WSV_s", #Inter-annual Variability
              "SV_s", #Seasonal Variability
              "HFO_s", #Flood Occurrence
              "DRO_s", #Drought Severity
              "STOR_s", #Upstream Storage
              "GW_s", #Groundwater Stress
              "WRI_s", #Return Flow Ratio
              "ECO_S_s", #Upstream Protected Land
              "MC_s", #Media Coverage 
              "WCG_s", #Access to Water 
              "ECO_V_s"] # Threatened Amphibians

print("\nGCS_OUTPUT_PATH: " + GCS_OUTPUT_PATH)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version

get_ipython().magic('matplotlib inline')


# In[3]:

import os
import subprocess
import numpy as np
import pandas as pd
import geopandas as gpd


# In[4]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[5]:

get_ipython().system('aws s3 cp {S3_TABLE_INPUT_PATH} {ec2_input_path} --recursive')


# In[6]:

table_input_path = "{}/{}".format(ec2_input_path,TABLE_INPUT_FILE_NAME)


# In[7]:

gdf_geom = gpd.read_file(table_input_path)


# In[8]:

gdf_geom.head()


# In[9]:

for indicator_aq21 in INDICATORS:
    print(indicator_aq21)
    
    column = indicator_aq21
    layer = "aqueduct_global_dl_20150409"
    destination_path_shp = ec2_input_path
    destination_path_tif = "{}/{}.tif".format(ec2_output_path,indicator_aq21)
    command = "{} -a {} -at -ot Float32 -of GTiff -te -180 -90 180 90 -ts {} {} -co COMPRESS=DEFLATE -co PREDICTOR=1 -co ZLEVEL=6 -l {} -a_nodata -32767 {} {}".format(GDAL_RASTERIZE_PATH,column,X_DIMENSION_30S,Y_DIMENSION_30S,layer,destination_path_shp,destination_path_tif)
    print(command)
    response = subprocess.check_output(command,shell=True)


# In[10]:

get_ipython().system('gsutil -m cp -r {ec2_output_path} {GCS_OUTPUT_PATH}')


# In[11]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous run:  
# 0:25:19.202351
