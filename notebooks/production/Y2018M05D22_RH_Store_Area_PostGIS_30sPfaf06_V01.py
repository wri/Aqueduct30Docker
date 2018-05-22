
# coding: utf-8

# In[1]:

""" Store area of 30sPfaf06 basins in postGIS database. 
-------------------------------------------------------------------------------

Store the area of 30sPfaf06 zones in the postGIS table. 

Author: Rutger Hofste
Date: 20180522
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04


Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.

"""

TESTING = 1
OVERWRITE = 1 
SCRIPT_NAME = "Y2018M05D22_RH_Store_Area_PostGIS_30sPfaf06_V01"
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
#TABLE_NAME = "area30spfaf06v01"

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M04D20_RH_Zonal_Stats_Area_EE_V01/output_V02"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nInput ec2: " + ec2_input_path,
      "\nInput s3: " + S3_INPUT_PATH)



# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()


# In[4]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_input_path}')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive --exclude="*" --include="*.pkl"')


# In[6]:

import os
import numpy as np
import pandas as pd
import aqueduct3
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)


# In[7]:

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
connection = engine.connect()

"""
if OVERWRITE:
    sql = text("DROP TABLE IF EXISTS {};".format(TABLE_NAME))
    result = engine.execute(sql)
"""


# In[8]:

input_file_names = os.listdir(ec2_input_path)


# In[9]:

input_file_names


# In[10]:

def pre_process_df(df,hybas_level,spatial_resolution):
    df["zones"] = df["zones"].astype(np.int64)
    df["area_m2_{}pfaf{:02.0f}".format(spatial_resolution,hybas_level)] = df["sum"]
    df.drop("sum",axis=1,inplace=True)
    df.sort_index(axis=1, inplace=True)
    df.set_index("zones",inplace=True)
    df.index.name = "pfafid_{}pfaf{:02.0f}".format(spatial_resolution,hybas_level)
    return df


# In[11]:

hybas_levels = [0,6]
spatial_resolutions = ["30s","5min"]


# In[12]:

for hybas_level in hybas_levels:
    for spatial_resolution in spatial_resolutions:
        input_file_name = "df_hybas_lev{:02.0f}_{}.pkl".format(hybas_level,spatial_resolution)
        input_file_path = "{}/{}".format(ec2_input_path,input_file_name)
        df = pd.read_pickle(input_file_path)
        df_cleaned = pre_process_df(df,hybas_level,spatial_resolution)
        table_name = "area_m2_{}pfaf{:02.0f}".format(spatial_resolution,hybas_level)
        df_cleaned.to_sql(table_name,engine,if_exists='replace', index=True,chunksize=100)


# In[13]:

df_cleaned.head()


# In[14]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:14:43.240018
# 
