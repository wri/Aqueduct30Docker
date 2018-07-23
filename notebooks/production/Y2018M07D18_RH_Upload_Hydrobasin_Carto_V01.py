
# coding: utf-8

# In[1]:

""" Upload simplified hydrobasins to mapbox for visualization purposes.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20180718
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04


"""

SCRIPT_NAME = "Y2018M07D18_RH_Upload_Hydrobasin_Carto_V01"
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

POSTGIS_INPUT_TABLE_NAME = "hybas06_v04"

OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()


print("POSTGIS_INPUT_TABLE_NAME: ", POSTGIS_INPUT_TABLE_NAME,
      "\nOUTPUT_TABLE_NAME: ",OUTPUT_TABLE_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().magic('matplotlib inline')
import os
import json
import mapboxgl
import sqlalchemy
import pandas as pd
import geopandas as gpd
from cartoframes import CartoContext, Credentials
from cartoframes.contrib import vector


# In[4]:

F = open("/.carto_builder","r")
carto_api_key = F.read().splitlines()[0]
F.close()
creds = Credentials(key=carto_api_key, 
                    username='wri-playground')
cc = CartoContext(creds=creds)


# In[5]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))


# In[6]:

sql = "SELECT pfaf_id, geom FROM {}".format(POSTGIS_INPUT_TABLE_NAME)


# In[7]:

gdf =gpd.GeoDataFrame.from_postgis(sql,engine,geom_col='geom' )


# In[8]:

gdf.head()


# In[9]:

cc.write(gdf,
         encode_geom=True,
         table_name= OUTPUT_TABLE_NAME,
         overwrite=True)


# In[10]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

