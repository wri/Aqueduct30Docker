
# coding: utf-8

# In[1]:

""" Rasterize Aqueduct 30 and store to Google Cloud Storage.
-------------------------------------------------------------------------------

Recap: The quantiles approach has been applied to all weightings, not just
the default one. Therefore there are slight variations in the histogram for
"def" or default weighting. 

Rasterizing the master geom at 30s takes a long time. Consider using the 
simplified version of the master geomtery. 

Update:
- In order to compare overall water risk, it is important to export the
mask with the fraction of valid data.

The column is renamed to owr_wf (overall water risk weight fraction)

Author: Rutger Hofste
Date: 20190521
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""
TESTING = 0

SCRIPT_NAME = "Y2019M05D21_RH_AQ30VS21_Rasterize_AQ30_EE_V01"
OUTPUT_VERSION = 5

GDAL_RASTERIZE_PATH = "/opt/anaconda3/envs/python35/bin/gdal_rasterize"
X_DIMENSION_30S = 43200
Y_DIMENSION_30S = 21600

S3_TABLE_INPUT_PATH = "s3://wri-projects/Aqueduct30/finalData/Y2019M01D14_RH_Aqueduct_Results_V01/output_V04/annual"
TABLE_INPUT_FILE_NAME = "annual_pivot.pkl"

#S3_GEOM_INPUT_PATH = "s3://wri-projects/Aqueduct30/finalData/Y2019M01D14_RH_Aqueduct_Results_V01/output_V04/master_geom"
#GEOM_INPUT_FILE_NAME = "master_geom.shp"

S3_GEOM_INPUT_PATH = "s3://wri-projects/Aqueduct30/rawData/Vizzuality/Y2019M05D21_RH_Simplified_Master_Geom_V01/output_V01"
GEOM_INPUT_FILE_NAME = "y2018m12d06_rh_master_shape_v02_2.gpkg"


ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_process_path =  "/volumes/data/{}/process_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 

GCS_OUTPUT_PATH = "gs://aqueduct30_v01/{}/".format(SCRIPT_NAME)

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
from google.cloud import bigquery



# In[4]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_process_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_process_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[5]:

get_ipython().system('aws s3 cp {S3_TABLE_INPUT_PATH}/{TABLE_INPUT_FILE_NAME} {ec2_input_path} ')


# In[6]:

get_ipython().system('aws s3 cp {S3_GEOM_INPUT_PATH} {ec2_input_path} --recursive')


# In[7]:

table_input_path = "{}/{}".format(ec2_input_path,TABLE_INPUT_FILE_NAME)
geom_input_path = "{}/{}".format(ec2_input_path,GEOM_INPUT_FILE_NAME)


# In[8]:

df_table = pd.read_pickle(table_input_path)


# In[9]:

gdf_geom = gpd.read_file(geom_input_path)


# In[10]:

df_table.head()


# In[11]:

df_table.drop(columns=["aq30_id","pfaf_id","gid_1","aqid"],inplace=True)


# In[12]:

gdf_geom.head()


# In[13]:

gdf_merge = gdf_geom.merge(df_table,on="string_id",how="left")


# In[14]:

gdf_merge.head()


# In[15]:

pd.set_option('display.max_rows', 500)


# In[16]:

df_table.dtypes


# In[17]:

gdf_selection = gdf_merge[["string_id","w_awr_def_tot_score","w_awr_def_tot_weight_fraction","bws_score","iav_score","sev_score","geometry"]]


# In[18]:

gdf_selection.rename(columns={"w_awr_def_tot_score":"owr_score",
                              "w_awr_def_tot_weight_fraction":"owr_wf"},inplace=True)


# In[19]:

process_path = "{}/{}.shp".format(ec2_process_path,SCRIPT_NAME)


# In[20]:

if TESTING:
    gdf_selection =gdf_selection[0:1000]


# In[21]:

gdf_selection.to_file(driver="ESRI Shapefile",filename=process_path)


# In[22]:

indicators = ["owr_score","owr_wf","bws_score","iav_score","sev_score"]


# In[23]:

for indicator in indicators:
    print(indicator)
    column = indicator
    layer = SCRIPT_NAME
    destination_path_shp = process_path
    destination_path_tif = "{}/{}.tif".format(ec2_output_path,indicator)
    command = "{} -a {} -at -ot Integer64 -of GTiff -te -180 -90 180 90 -ts {} {} -co COMPRESS=DEFLATE -co PREDICTOR=1 -co ZLEVEL=6 -l {} -a_nodata -9999 {} {}".format(GDAL_RASTERIZE_PATH,column,X_DIMENSION_30S,Y_DIMENSION_30S,layer,destination_path_shp,destination_path_tif)
    print(command)
    response = subprocess.check_output(command,shell=True)
    


# In[24]:

get_ipython().system('gsutil -m cp -r {ec2_output_path} {GCS_OUTPUT_PATH}')


# In[25]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

