
# coding: utf-8

# In[11]:

""" Upload simplified hydrobasins to mapbox for visualization purposes.
-------------------------------------------------------------------------------

Upload postgis table to mapbox via geopandas and geojson.

Author: Rutger Hofste
Date: 20180703
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name.
    PREVIOUS_SCRIPT_NAME (string) : Previous script name. 
    INPUT_VERSION (integer) : Input version.
    OUTPUT_VERSION (integer) : Output version.  

Returns:


"""

SCRIPT_NAME = "Y2018M07D03_RH_Upload_Hydrobasin_Mapbox_V01"
INPUT_VERSION = 1
OUTPUT_VERSION = 3

EXCLUDE_BASIN = 353020

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
INPUT_TABLE_NAME = "hybas06_v04"

# ETL

output_dataset_name = "{}_V{:02.0f}".format(INPUT_TABLE_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)


print("Input table: " + INPUT_TABLE_NAME +
      "\nOutput table: " + output_dataset_name)


# In[20]:




# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[24]:

import os
import mapbox
import geojson
import sqlalchemy
import time
import geopandas as gpd


# In[13]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

F = open("/.mapbox","r")
token = F.read().splitlines()[0]
F.close()
os.environ["MAPBOX_ACCESS_TOKEN"] = token

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
#connection = engine.connect()


# In[5]:

sql = "select * from {}".format(INPUT_TABLE_NAME)


# In[6]:

# load geodataframe from postGIS
gdf =gpd.GeoDataFrame.from_postgis(sql,engine,geom_col='geom' )


# In[7]:

gdf_out = gdf[["pfaf_id","geom"]]


# In[8]:

gdf_out_clean = gdf_out[gdf_out["pfaf_id"] != EXCLUDE_BASIN]


# In[9]:

assert gdf_out_clean.shape[0] == 16395


# In[10]:

assert gdf_out.crs == None


# In[16]:

output_file_path = "{}/{}.geojson".format(ec2_output_path,INPUT_TABLE_NAME)


# In[17]:

print(output_file_path)


# In[18]:

gdf_out_clean.to_file(output_file_path,driver="GeoJSON",encoding="UTF-8")


# In[19]:

service = mapbox.Uploader()


# In[21]:

mapid = output_dataset_name


# In[22]:

with open(output_file_path, 'rb') as src:
    upload_resp = service.upload(src, mapid)


# In[23]:

upload_resp.status_code


# In[26]:

upload_id = upload_resp.json()['id']


# In[27]:

upload_id


# In[29]:

# Upload is processed on the server.


# In[30]:

engine.dispose()


# In[31]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 
