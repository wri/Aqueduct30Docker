
# coding: utf-8

# In[15]:

""" Rasterize Aqueduct 30 and store to Google Cloud Storage.
-------------------------------------------------------------------------------

Recap: The quantiles approach has been applied to all weightings, not just
the default one. Therefore there are slight variations in the histogram for
"def" or default weighting. 


Author: Rutger Hofste
Date: 20190521
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = "Y2019M05D21_RH_AQ30VS21_Rasterize_AQ30_EE_V01"
OUTPUT_VERSION = 1

X_DIMENSION_30S = 43200
Y_DIMENSION_30S = 21600

S3_TABLE_INPUT_PATH = "s3://wri-projects/Aqueduct30/finalData/Y2019M01D14_RH_Aqueduct_Results_V01/output_V04/annual"
TABLE_INPUT_FILE_NAME = "annual_normalized.pkl"

S3_GEOM_INPUT_PATH = "s3://wri-projects/Aqueduct30/finalData/Y2019M01D14_RH_Aqueduct_Results_V01/output_V04/master_geom"
GEOM_INPUT_FILE_NAME = "master_geom.shp"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 

s3_output_path = "s3://wri-projects/Aqueduct30/Aq30vs21/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\ns3_output_path: " + s3_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version

get_ipython().magic('matplotlib inline')


# In[23]:

import os
import numpy as np
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery



# In[4]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[6]:

get_ipython().system('aws s3 cp {S3_TABLE_INPUT_PATH}/{TABLE_INPUT_FILE_NAME} {ec2_input_path} ')


# In[16]:

get_ipython().system('aws s3 cp {S3_GEOM_INPUT_PATH} {ec2_input_path} --recursive')


# In[22]:

table_input_path = "{}/{}".format(ec2_input_path,TABLE_INPUT_FILE_NAME)
geom_input_path = "{}/{}".format(ec2_input_path,GEOM_INPUT_FILE_NAME)


# In[19]:

df_table = pd.read_pickle(table_input_path)


# In[25]:

gdf_geom = gpd.read_file(geom_input_path)


# In[ ]:

# Hier gebleven


# In[ ]:




# In[20]:

industry_short = "def"
indicator = "awr"
group_short = "tot"


# In[21]:

df_table.head()


# In[ ]:

df_sel =  df.loc[(df["group_short"] == group_short) & (df["indicator"] == indicator) & (df["industry_short"] == industry_short)]


# In[ ]:




# In[ ]:



