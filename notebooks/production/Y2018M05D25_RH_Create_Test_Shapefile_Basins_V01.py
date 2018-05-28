
# coding: utf-8

# In[1]:

""" Create shapefile and geodataframe from list of pfaf_ids for testing
-------------------------------------------------------------------------------


Author: Rutger Hofste
Date: 20180327
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = "Y2018M05D25_RH_Create_Test_Shapefile_Basins_V01"
OUTPUT_VERSION = 1

DICTJE = {172265:"Large stream in only one cell. perpendicular contributing areas",
          172263: "Large stream in a few cells, perpendicular contributing areas",
          172261: "Tiny basin smaller than one 5min cell",
          172250: "Large basin with main stream",
          172306: "Large basin with main stream and other stream in most downstream cell",
          172521: "Small basin with a confluence within basin. Stream_order increases in most downstream cell but is part of basin",
          172144: "Basin with an endorheic basin in one of its upstream cells"}


DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = "hybas06_v04"
OUTPUT_FILE_NAME = "hybas06_v04_Selection"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)


print("Input ec2: " + ec2_input_path,
      "\nOutput ec2: " + ec2_output_path,
      "\nOutput s3: " + s3_output_path)


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

# imports
import re
import os
import numpy as np
import pandas as pd
import geopandas as gpd
import aqueduct3
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)


# In[5]:

def postGisToGdf(connection,tableName):
    """this function gets a geoDataFrame from a postGIS database instance
    
    
    Args:
        connection (sqlalchemy.engine.base.Connection) : postGIS enabled database connection 
        tableName (string) : table name
 
    Returns:
        gdf (geoPandas.GeoDataFrame) : the geodataframe from PostGIS
        
    todo:
        allow for SQL filtering
    
    
    """   
    gdf =gpd.GeoDataFrame.from_postgis("select * from %s" %(tableName),connection,geom_col='geom' )
    gdf.crs =  {'init' :'epsg:4326'}
    return gdf


# In[6]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
connection = engine.connect()


# In[7]:

gdf = postGisToGdf(connection,INPUT_TABLE_NAME)


# In[8]:

gdf_selection = gdf[gdf["pfaf_id"].isin(DICTJE.keys())]


# In[9]:

df = pd.DataFrame.from_dict(DICTJE,orient="index")
df.columns = ["comment"]
df["pfaf_id"] = df.index


# In[10]:

gdf_selection_merged = gdf_selection.merge(df,on="pfaf_id")


# In[11]:

gdf_selection_merged


# In[12]:

output_file_path = "{}/{}".format(ec2_output_path,OUTPUT_FILE_NAME)


# In[13]:

gdf_selection_merged.to_file(filename=output_file_path,driver="ESRI Shapefile")


# In[14]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# File manually zipped and ingested on Earthengine
