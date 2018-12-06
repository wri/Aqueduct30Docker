
# coding: utf-8

# In[1]:

""" Simplify and create composite index.
-------------------------------------------------------------------------------

This script simplifies the union shapefile to only contain useful id's:
"pfaf_id","gid_1","gid_0","aqid"

it will create a new composite index based on 
[pfaf_id]-[gid_1]-[aqid] in string format.


Author: Rutger Hofste
Date: 20181206
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = "Y2018M12D06_RH_Master_Shape_Composite_Index_V01"
OUTPUT_VERSION = 2

NODATA_VALUE = -9999

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M12D04_RH_Union_ArcMap_V01/steps/step2_union_whymap/output"
INPUT_FILE_NAME = "hybasgadm_u_whymap_arcgis_50m_v01.shp"

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 

s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)


print("S3_INPUT_PATH: ",S3_INPUT_PATH,
      "\nec2_input_path: ",ec2_input_path,
      "\nec2_output_path: ",ec2_output_path,
      "\nBQ_OUTPUT_DATASET_NAME: ", BQ_OUTPUT_DATASET_NAME,
      "\nBQ_OUTPUT_TABLE_NAME: ",BQ_OUTPUT_TABLE_NAME,
      "\ns3_output_path: ", s3_output_path
      )


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive ')


# In[5]:

import os
import multiprocessing
import pandas as pd
import geopandas as gpd
import numpy as np
from google.cloud import bigquery

pd.set_option('display.max_columns', 500)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[6]:

input_path = "{}/{}".format(ec2_input_path,INPUT_FILE_NAME)


# In[7]:

gdf = gpd.read_file(input_path)


# In[8]:

gdf.rename(columns={"PFAF_ID":"pfaf_id",
                    "GID_0":"gid_0",
                    "GID_1":"gid_1"},
           inplace=True)


# In[9]:

# ArcMap uses empty strings (WTF!) instead of Nones. Replacing


# In[10]:

gdf["pfaf_id"][gdf["pfaf_id"]== 0] = NODATA_VALUE
gdf["aqid"][gdf["aqid"]== 0] = NODATA_VALUE
gdf["gid_0"][gdf["gid_0"] == ""] = NODATA_VALUE
gdf["gid_1"][gdf["gid_1"] == ""] = NODATA_VALUE


# In[11]:

gdf_simple = gdf[["pfaf_id","gid_1","gid_0","aqid","geometry"]]


# In[12]:

def create_composite_index(row):
    if row.pfaf_id == -9999:
        pfaf_id = "None"
    else:
        pfaf_id = int(row.pfaf_id)

    if row.gid_1 == -9999:
        gid_1 = "None"
    else:
        gid_1 = row.gid_1
        
    if row.aqid == -9999:
        aqid = "None"
    else:
        aqid = int(row.aqid)
    
    
    string_id = "{}-{}-{}".format(pfaf_id,gid_1,aqid)
    return string_id


# In[13]:

gdf_simple["string_id"] = gdf_simple.apply(create_composite_index,axis=1)


# In[14]:

output_file_path = "{}/{}".format(ec2_output_path,SCRIPT_NAME)


# In[15]:

gdf_simple.to_file(output_file_path + ".shp",driver="ESRI Shapefile")


# In[16]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[17]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:03:35.008306
# 

# In[ ]:



