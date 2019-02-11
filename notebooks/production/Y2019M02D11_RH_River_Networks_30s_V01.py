
# coding: utf-8

# In[16]:

""" Merge WWF's River Networks at 30s resolution
-------------------------------------------------------------------------------


Author: Rutger Hofste
Date: 20190211
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04
"""

SCRIPT_NAME = "Y2019M02D11_RH_River_Networks_30s_V01"
OUTPUT_VERSION = 3

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/rawData/WWF/HydroSheds30sComplete/RiverNetworks30s_unzipped"
FILENAME = "RiverNetwork30s"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)


print("Input s3: " + S3_INPUT_PATH +
      "\nInput ec2: " + ec2_input_path +
      "\nOutput ec2: " + ec2_output_path+
      "\nOutput S3: " + s3_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

get_ipython().system('aws s3 cp --recursive {S3_INPUT_PATH} {ec2_input_path} ')


# In[5]:

import os
import geopandas as gpd


# In[6]:

files = os.listdir(ec2_input_path)


# In[12]:

gdf_total = gpd.GeoDataFrame()
for one_file in files:
    input_file_name = "{}{}/{}.shp".format(ec2_input_path,one_file,one_file)
    print(input_file_name)
    gdf = gpd.read_file(filename=input_file_name)
    gdf_total = gdf_total.append(gdf)
destination_path = "{}/global_rivernetwork_30s.shp".format(ec2_output_path)


# In[13]:

gdf_total.to_file(filename = destination_path,
                  driver="ESRI Shapefile")


# In[17]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[18]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:21:16.346083

# In[ ]:



