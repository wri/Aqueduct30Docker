
# coding: utf-8

# In[12]:

""" Create shapefile and csv table for hybas, fao and upstream/downstream
-------------------------------------------------------------------------------

This script needs a revision or needs to be archived. The database is based
on stroing lists and is a non normalized version. violates N-1.


Author: Rutger Hofste
Date: 20170829
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.
    DATABASE_ENDPOINT (string) : RDS or postGreSQL endpoint.
    DATABASE_NAME (string) : Database name.
    TABLE_NAME_AREA_30SPFAF06 (string) : Table name used for areas. Must exist
        on same database as used in rest of script.
    S3_INPUT_PATH_RIVERDISCHARGE (string) : AWS S3 input path for 
        riverdischarge.    
    S3_INPUT_PATH_DEMAND (string) : AWS S3 input path for 
        demand.    

"""

S3_INPUT_PATH_FAO ="s3://wri-projects/Aqueduct30/processData/Y2017M08D25_RH_spatial_join_FAONames_V01/output_V07/"
S3_INPUT_PATH_DOWNSTREAM = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Downstream_V01/output_V02/"
S3_INPUT_PATH_HYBAS = "s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Merge_HydroBasins_V02/output_V04/"

SCRIPT_NAME = "Y2017M08D29_RH_Merge_FAONames_Upstream_V01"
OUTPUT_VERSION = 3

INPUT_FILE_NAME_FAO = "hybas_lev06_v1c_merged_fiona_withFAO_V07.pkl"
INPUT_FILE_NAME_DOWNSTREAM = "hybas_lev06_v1c_merged_fiona_upstream_downstream_V01.pkl"
INPUT_FILE_NAME_HYBAS = "hybas_lev06_v1c_merged_fiona_V04.shp"

OUTPUT_FILE_NAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V%0.2d" %(OUTPUT_VERSION)

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input ec2: " + ec2_input_path,
      "\nOutput ec2: " + ec2_output_path,
      "\nInput s3 FAO: " + S3_INPUT_PATH_FAO,
      "\nInput s3 Downstream: " + S3_INPUT_PATH_DOWNSTREAM,
      "\nInput s3 Hybas: " + S3_INPUT_PATH_HYBAS,
      "\nOutput s3: " + s3_output_path)



# In[5]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[6]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')

get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[7]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_FAO} {ec2_input_path} --recursive ')


# In[8]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_DOWNSTREAM} {ec2_input_path} --recursive ')


# In[9]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_HYBAS} {ec2_input_path} --recursive --exclude *.tif')


# In[10]:

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


# In[14]:

dfFAO = pd.read_pickle(os.path.join(ec2_input_path,INPUT_FILE_NAME_FAO))


# In[17]:

dfFAO.head()


# In[18]:

dfDownstream = pd.read_pickle(os.path.join(ec2_input_path,INPUT_FILE_NAME_DOWNSTREAM))


# In[ ]:

dfDownstream = dfDownstream.set_index("PFAF_ID", drop=False)


# In[19]:

dfDownstream.head()


# In[20]:

dfDownstream.drop("Unnamed: 0",1,inplace=True)


# In[21]:

gdfHybas = gpd.read_file(os.path.join(ec2_input_path,INPUT_FILE_NAME_HYBAS))
gdfHybas = gdfHybas.set_index("PFAF_ID", drop=False)


# In[22]:

dfHybas = pd.DataFrame(gdfHybas["PFAF_ID"])


# Merging the the downstream and FAO datasets, adding Hybas geometry and export both Excel sheet and dataset.

# In[23]:

dfOut = dfDownstream.merge(dfFAO,how="outer")


# In[16]:

dfOut = dfOut.set_index("PFAF_ID",drop=False)


# In[17]:

gdfHybas.dtypes


# In[18]:

gdfHybasSimple = gpd.GeoDataFrame(dfHybas, geometry=gdfHybas.geometry)


# In[19]:

gdfHybasSimple.to_file(os.path.join(ec2_output_path,OUTPUT_FILE_NAME+".shp"))


# In[20]:

dfOut.to_csv(os.path.join(ec2_output_path,OUTPUT_FILE_NAME+".csv"))


# In[21]:

dfOut.to_pickle(os.path.join(ec2_output_path,OUTPUT_FILE_NAME+".pkl"))


# In[22]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[23]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:02:27.045097
