
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
OUTPUT_VERSION = 4

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

POSTGIS_INPUT_TABLE_NAME = "hybas06_v04"

OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()
OUTPUT_TABLE_NAME_SIMPLIFIED = "{}_simplified_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

print("POSTGIS_INPUT_TABLE_NAME: ", POSTGIS_INPUT_TABLE_NAME,
      "\nOUTPUT_TABLE_NAME: ",OUTPUT_TABLE_NAME,
      "\nOUTPUT_TABLE_NAME_SIMPLIFIED: ",OUTPUT_TABLE_NAME_SIMPLIFIED)


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

gdf.dtypes


# In[10]:

gdf.shape


# In[11]:

cc.write(gdf,
         encode_geom=True,
         table_name= OUTPUT_TABLE_NAME,
         privacy='public',
         overwrite=True)


# In[12]:

# Create index


# In[13]:

sql_index = "CREATE INDEX idx_{}_pfaf_id ON {} (pfaf_id)".format(OUTPUT_TABLE_NAME,OUTPUT_TABLE_NAME)


# In[14]:

cc.query(sql_index)


# In[15]:

# Create a version with simplified geometries


# In[16]:

sql_simplified = "SELECT pfaf_id, ST_Simplify(geom, 0.01) as geom FROM {}".format(POSTGIS_INPUT_TABLE_NAME)


# In[17]:

gdf_simplified =gpd.GeoDataFrame.from_postgis(sql_simplified,engine,geom_col='geom' )


# In[18]:

gdf_simplified.head()


# In[19]:

gdf.dtypes


# In[20]:

gdf.shape


# In[21]:

cc.write(gdf_simplified,
         encode_geom=True,
         table_name= OUTPUT_TABLE_NAME_SIMPLIFIED,
         privacy='public',
         overwrite=True)


# In[22]:

sql_index = "CREATE INDEX idx_{}_pfaf_id ON {} (pfaf_id)".format(OUTPUT_TABLE_NAME_SIMPLIFIED,OUTPUT_TABLE_NAME_SIMPLIFIED)


# In[23]:

print(sql_index)


# In[24]:

cc.query(sql_index)


# In[ ]:




# In[25]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:16:48.651013  
# 0:17:37.825289  
# 0:07:32.631313

# In[ ]:



