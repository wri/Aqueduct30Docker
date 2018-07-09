
# coding: utf-8

# In[19]:

""" Upload simplified hydrobasins to mapbox for visualization purposes.
-------------------------------------------------------------------------------

Upload postgis table to mapbox via geopandas and geojson.

Learned so far:

- do not use mapbox datasets. Editing in browser is not useful for our
purpose.
- fid needs to be in string format and match the id in the features.
- when using the mapbox uploads sdk, minimum zoom level is 5
- use tippecanoe to create vector tiles with custom zoom levels. 

install tippecanoe

cd /opt
git clone git@github.com:mapbox/tippecanoe.git
cd tippecanoe
apt-get install update
apt-get install build-essential libsqlite3-dev zlib1g-dev
make
make install


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
OUTPUT_VERSION = 4

MIN_ZOOM_LIMIT = 1
MAX_ZOOM_LIMIT = 12

EXCLUDE_BASIN = 353020

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
INPUT_TABLE_NAME = "hybas06_v04"

# ETL

output_dataset_name = "{}_V{:02.0f}".format(INPUT_TABLE_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)


print("Input table: " + INPUT_TABLE_NAME +
      "\nOutput table: " + output_dataset_name)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[22]:

import os
import mapbox
import geojson
import sqlalchemy
import time
import subprocess
import geopandas as gpd


# In[4]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[5]:

F = open("/.mapbox","r")
token = F.read().splitlines()[0]
F.close()
os.environ["MAPBOX_ACCESS_TOKEN"] = token

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
#connection = engine.connect()


# In[6]:

sql = "select * from {}".format(INPUT_TABLE_NAME)


# In[7]:

# load geodataframe from postGIS
gdf =gpd.GeoDataFrame.from_postgis(sql,engine,geom_col='geom' )


# In[8]:

gdf_out = gdf[["pfaf_id","geom"]]


# In[9]:

gdf_out_clean = gdf_out[gdf_out["pfaf_id"] != EXCLUDE_BASIN]


# In[10]:

assert gdf_out_clean.shape[0] == 16395


# In[11]:

assert gdf_out.crs == None


# In[12]:

output_file_path = "{}/{}".format(ec2_output_path,INPUT_TABLE_NAME)


# In[13]:

print(output_file_path)


# In[14]:

gdf_out_clean.to_file(output_file_path+".geojson",driver="GeoJSON",encoding="UTF-8")


# In[ ]:

# convert to vectortiles using tippecanoe


# In[15]:

output_file_path_tiles = "{}.mbtiles".format(output_file_path)


# In[16]:

print(output_file_path_tiles)


# In[20]:

command = "tippecanoe -o {} -Z {} -z {} {}.geojson".format(output_file_path_tiles,MIN_ZOOM_LIMIT,MAX_ZOOM_LIMIT,output_file_path)


# In[23]:

result =subprocess.check_output(command,shell=True)


# In[24]:

service = mapbox.Uploader()


# In[25]:

mapid = output_dataset_name


# In[26]:

print(mapid)


# In[27]:

with open(output_file_path_tiles, 'rb') as src:
    upload_resp = service.upload(src, mapid)


# In[28]:

upload_resp.status_code


# In[29]:

upload_id = upload_resp.json()['id']


# In[30]:

upload_id


# In[31]:

# Upload is processed on the server.


# In[32]:

engine.dispose()


# In[33]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:17:41.230438
# 

# In[ ]:



