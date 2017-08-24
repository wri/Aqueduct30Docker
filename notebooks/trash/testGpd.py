
# coding: utf-8

# In[64]:

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/test/testGpd/"
EC2_INPUT_PATH = "/volumes/data/temp/"
EC2_OUTPUT_PATH = "/volumes/data/temp/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/test/output/"


# In[59]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[31]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[14]:

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
get_ipython().magic('matplotlib notebook')
import os
import folium


# In[3]:

from shapely.wkt import loads
from shapely.geometry import Point


# In[49]:

gdfFAO = gpd.read_file('/volumes/data/temp/FAO/faoBuffered.shp')
gdfHybas = gpd.read_file('/volumes/data/temp/Hybas/hybas_lev06_v1c_merged_fiona_Cropped_V01.shp')


# In[51]:

gdfHybas.head()


# In[66]:

gdfHybas.set_index('PFAF_ID')


# In[73]:

gdfHybasBuffer = gdfHybas['geometry'].buffer(-0.005,resolution=16)


# In[70]:

gdfHybas[]


# In[60]:

gdfHybasBuffer.to_file(os.path.join(EC2_OUTPUT_PATH,'output.shp'))


# In[65]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')

