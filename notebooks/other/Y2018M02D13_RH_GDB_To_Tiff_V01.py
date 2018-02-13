
# coding: utf-8

# ### Y2018M02D13_RH_GDB_To_Tiff_V01
# 
# * Purpose of script: Convert geodatabase to geotiffs and upload to GCS
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20180213
# 
# Data was shared by Tianyi Luo in GDB format. Rasters have been exported to Geotiff format and zipped. 

# In[68]:

import os
if 'GDAL_DATA' not in os.environ:
    os.environ['GDAL_DATA'] = r'/usr/share/gdal/2.1'
from osgeo import gdal,ogr,osr
'GDAL_DATA' in os.environ
# If false, the GDAL_DATA variable is set incorrectly. You need this variable to obtain the spatial reference
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import time
import subprocess
import json
get_ipython().magic('matplotlib notebook')


# In[62]:

SCRIPT_NAME = "Y2018M02D13_RH_GDB_To_Tiff_V01"
OUTPUT_VERSION = 1



S3_INPUT_PATH = ("s3://wri-projects/Aqueduct30/processData/{}/input/").format(SCRIPT_NAME)
S3_OUTPUT_PATH = ("s3://wri-projects/Aqueduct30/processData/{}/output/").format(SCRIPT_NAME)

EC2_INPUT_PATH  = ("/volumes/data/{}/input/").format(SCRIPT_NAME)
EC2_OUTPUT_PATH = ("/volumes/data/{}/output/").format(SCRIPT_NAME)

INPUT_FILE_NAME = "aq21demand.zip"


GCS_OUTPUT_PATH = "gs://aqueduct30_v01/{}".format(SCRIPT_NAME)

EE_BASE = "projects/WRI-Aquaduct/aqueduct21V01"



# In[19]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[20]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# Functions

# In[53]:

outDict = {}
def splitKey(key):
    # will yield the root file code and extension of a set of keys
    prefix, extension = key.split(".")
    fileName = prefix.split("/")[-1]
    outDict = {"fileName":fileName,"extension":extension}
    return outDict


# In[ ]:




# In[ ]:




# In[22]:

file_location = "{}{}".format(EC2_INPUT_PATH, INPUT_FILE_NAME)


# In[23]:

destination_folder = EC2_INPUT_PATH


# In[24]:

print(file_location)


# In[25]:

get_ipython().system(' ls /volumes/data/Y2018M02D13_RH_GDB_To_Tiff_V01/input/')


# In[26]:

get_ipython().system(' unzip {file_location} -d {destination_folder}')


# In[27]:

get_ipython().system('gsutil version')


# In[28]:

get_ipython().system('gcloud config set project aqueduct30')


# In[30]:

get_ipython().system('gsutil -m cp {EC2_INPUT_PATH}*.tif {GCS_OUTPUT_PATH}')


# In[31]:

print(GCS_OUTPUT_PATH)


# In[35]:

command = ("earthengine create folder %s") %EE_BASE


# In[36]:

print(command)


# In[37]:

subprocess.check_output(command,shell=True)


# In[39]:

command = ("/opt/google-cloud-sdk/bin/gsutil ls %s") %(GCS_OUTPUT_PATH)


# In[40]:

keys = subprocess.check_output(command,shell=True)


# In[41]:

keys = keys.decode('UTF-8').splitlines()


# In[42]:

print(keys)


# In[97]:

df = pd.DataFrame()
i = 0
for key in keys:
    i = i+1
    outDict = splitKey(key)
    df2 = pd.DataFrame(outDict,index=[i])
    df2["source"] = key
    df = df.append(df2)  


# In[104]:

properties = {}
properties["script_used"] = SCRIPT_NAME
properties["ingested_by"] = "'Rutger Hofste'"
properties["aqueduct_version"] = "'2.1'"
properties["version"] = OUTPUT_VERSION
properties["units"] = "m3"


# In[105]:

propertyString = ""
for key, value in properties.items():
    propertyString = propertyString + " -p " + str(key) + "=" + str(value)


# In[106]:

df.head()


# In[108]:

for index, row in df.iterrows():
    asset_id = EE_BASE + "/" + row["fileName"]     
    command =  "earthengine upload image --asset_id {} {} {}".format(asset_id, row["source"], propertyString)
    subprocess.check_output(command,shell=True)
    print(command)


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



