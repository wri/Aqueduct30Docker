
# coding: utf-8

# ### Spatially Join FAO Names to hydrobasins level 6
# 
# * Purpose of script: Spatially join FAO Names hydrobasins to the official HydroBasins level 6 polygons
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170825

# In[19]:

S3_INPUT_PATH_FAO = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Buffer_FAONames_V01/output/"
S3_INPUT_PATH_HYBAS = "s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Merge_HydroBasins_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D25_RH_spatial_join_FAONames_V01/output/"
EC2_INPUT_PATH = "/volumes/data/Y2017M08D25_RH_spatial_join_FAONames_V01/input/"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M08D25_RH_spatial_join_FAONames_V01/output/"
INPUT_FILE_NAME_FAO = "hydrobasins_fao_fiona_merged_buffered_v01.shp"
INPUT_FILE_NAME_HYBAS = "hybas_lev06_v1c_merged_fiona_V01.shp"


# In[10]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[11]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_FAO} {EC2_INPUT_PATH} --recursive --quiet')


# In[12]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_HYBAS} {EC2_INPUT_PATH} --recursive --quiet --exclude *.tif')


# In[13]:

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
get_ipython().magic('matplotlib notebook')


# In[15]:

gdfFAO = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME_FAO))


# In[17]:

list(gdfFAO)


# In[20]:

gdfHybas = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME_HYBAS))


# In[21]:

list(gdfHybas)


# In[22]:

gdfHybasTest = gdfHybas.iloc[100:200]
gdfFAOTest = gdfFAO.iloc[100:200]


# In[25]:

type(gdfFAOTest)


# In[ ]:

gdfJoined = gpd.sjoin(gdfHybas, gdfFAO ,how="left", op='intersects')


# In[ ]:



