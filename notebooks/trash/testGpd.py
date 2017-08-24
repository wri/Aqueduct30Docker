
# coding: utf-8

# In[1]:

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/test/testGpd/"
EC2_INPUT_PATH = "/volumes/data/temp/"
EC2_OUTPUT_PATH = "/volumes/data/temp/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/test/output/"


# In[2]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[3]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive --quiet')


# In[4]:

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
get_ipython().magic('matplotlib notebook')
import os
import folium
from shapely.wkt import loads
from shapely.geometry import Point


# In[5]:

gdfFAO = gpd.read_file('/volumes/data/temp/FAO/faoBuffered.shp')
gdfHybas = gpd.read_file('/volumes/data/temp/Hybas/hybas_lev06_v1c_merged_fiona_Cropped_V01.shp')


# In[6]:

gdfHybas = gdfHybas.set_index('PFAF_ID')


# In[7]:

gdfHybas.head()


# In[8]:

gsHybasBuffer = gdfHybas['geometry'].buffer(-0.005,resolution=16)


# in order to use merge, I needed to convert the geoSeries to a geoDataFrame

# In[9]:

gdfHybasBuffer =gpd.GeoDataFrame(geometry=gsHybasBuffer)


# The old geometry will be replaced by the new geometry (buffered)

# In[10]:

gdfHybas = gdfHybas.drop('geometry',1)


# In[11]:

gdfHybasBuffer.head()


# In[12]:

gdfHybas.head()


# Contrary to pandas, geopandas does not automatically merge based on index. Therefore I copy the indices to new columns. Hopefully merging on index by default will be supported in the future. 

# In[13]:

gdfHybas['PFAF_ID2'] = gdfHybas.index
gdfHybasBuffer['PFAF_ID2'] = gdfHybasBuffer.index


# In[14]:

gdfHybasNew = gdfHybasBuffer.merge(gdfHybas,how="outer",on="PFAF_ID2")


# In[15]:

gdfHybasNew.head()


# Note that geopandas did not preserve the Index. Hopefully that will get fixed in the future as well. 

# In[16]:

gdfHybasBuffer.to_file(os.path.join(EC2_OUTPUT_PATH,'output.shp'))


# In[17]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive --quiet')

