
# coding: utf-8

# ### Merge Upstream Downstream with FAO names 
# 
# * Purpose of script: Create a shapefile and csv file with both the upstream / downstream relation and the FAO basin names
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170829

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

S3_INPUT_PATH_FAO ="s3://wri-projects/Aqueduct30/processData/Y2017M08D25_RH_spatial_join_FAONames_V01/output/"
S3_INPUT_PATH_DOWNSTREAM = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Downstream_V01/output/"
S3_INPUT_PATH_HYBAS = "s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Merge_HydroBasins_V01/output/"

SCRIPT_NAME = "Y2017M08D29_RH_Merge_FAONames_Upstream_V01"

OUTPUT_VERSION = 3

INPUT_FILE_NAME_FAO = "hybas_lev06_v1c_merged_fiona_withFAO_V01.csv"
INPUT_FILE_NAME_DOWNSTREAM = "hybas_lev06_v1c_merged_fiona_upstream_downstream_V01.csv"
INPUT_FILE_NAME_HYBAS = "hybas_lev06_v1c_merged_fiona_V01.shp"

EC2_INPUT_PATH = "/volumes/data/%s/input/" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output/" %(SCRIPT_NAME)

OUTPUT_FILE_NAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V%0.2d" %(OUTPUT_VERSION)

S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/%s/output/" %(SCRIPT_NAME)


# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_FAO} {EC2_INPUT_PATH} --recursive ')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_DOWNSTREAM} {EC2_INPUT_PATH} --recursive ')


# In[6]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_HYBAS} {EC2_INPUT_PATH} --recursive --exclude *.tif')


# In[7]:

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


# In[8]:

dfFAO = pd.read_csv(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME_FAO))
dfFAO = dfFAO.set_index("PFAF_ID", drop=False)


# In[9]:

dfFAO.head()


# In[10]:

dfDownstream = pd.read_csv(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME_DOWNSTREAM))
dfDownstream = dfDownstream.set_index("PFAF_ID", drop=False)


# In[11]:

dfDownstream.head()


# In[12]:

dfDownstream.drop("Unnamed: 0",1,inplace=True)


# In[13]:

gdfHybas = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME_HYBAS))
gdfHybas = gdfHybas.set_index("PFAF_ID", drop=False)


# In[14]:

dfHybas = pd.DataFrame(gdfHybas["PFAF_ID"])


# Merging the the downstream and FAO datasets, adding Hybas geometry and export both Excel sheet and dataset.

# In[15]:

dfOut = dfDownstream.merge(dfFAO,how="outer")


# In[16]:

dfOut = dfOut.set_index("PFAF_ID",drop=False)


# In[17]:

gdfHybas.dtypes


# In[ ]:

gdfHybasSimple = gpd.GeoDataFrame(dfHybas, geometry=gdfHybas.geometry)


# In[ ]:

gdfHybasSimple.to_file(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME+".shp"))


# In[ ]:

dfOut.to_csv(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME+".csv"))


# In[ ]:

dfOut.to_pickle(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME+".pkl"))


# In[ ]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

