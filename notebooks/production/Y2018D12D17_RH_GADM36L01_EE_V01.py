
# coding: utf-8

# In[1]:

""" Ingest GADM level 1 data to earthengine. 
-------------------------------------------------------------------------------

Hier gebleven. Gedoe met max vertices. Ofwel simplified version uploaded of
rasterize. 

Author: Rutger Hofste
Date: 20181217
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

SCRIPT_NAME = "Y2018D12D17_RH_GADM36L01_EE_V01"
OUTPUT_VERSION = 6

# Database settings
RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"
INPUT_TABLE_NAME = "y2018m11d12_rh_gadm36_level1_to_rds_v01_v04"

GCS_OUTPUT_PATH = "gs://aqueduct30_v01/{}".format(SCRIPT_NAME)

GDAL_RASTERIZE_PATH = "/opt/anaconda3/envs/python35/bin/gdal_rasterize"
X_DIMENSION_30S = 43200
Y_DIMENSION_30S = 21600

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nInput ec2: " + ec2_input_path,
      "\nInput postGIS table : " + INPUT_TABLE_NAME,
      "\nOutput GCS:" + GCS_OUTPUT_PATH)


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

import sqlalchemy
import geopandas as gpd
import aqueduct3
import subprocess


# In[5]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))
connection = engine.connect()


# In[6]:

q = """
SELECT
    gid_1_id,
    gid_0,
    name_0,
    gid_1,
    name_1,
    varname_1,
    nl_name_1,
    type_1,
    engtype_1,
    cc_1,
    hasc_1,
    ST_SimplifyPreserveTopology(geom,0.0001) as geom --approximately 11.11 meter at equator.
FROM
    {}
ORDER BY
    gid_1_id
""".format(INPUT_TABLE_NAME)


# In[7]:

gdf =gpd.GeoDataFrame.from_postgis(q,connection,geom_col='geom' )


# In[8]:

gdf.sort_index(axis=1,inplace=True)


# In[9]:

gdf.head()


# In[10]:

destination_path_shp = "{}/{}.shp".format(ec2_output_path,SCRIPT_NAME)


# In[11]:

destination_path_shp


# In[12]:

gdf.to_file(filename=destination_path_shp,driver="ESRI Shapefile")


# In[13]:

get_ipython().system('gsutil -m cp -r {ec2_output_path} {GCS_OUTPUT_PATH}')


# In[14]:

command = "earthengine create folder projects/WRI-Aquaduct/{}".format(SCRIPT_NAME)
response = subprocess.check_output(command,shell=True)


# In[15]:

command = "earthengine create folder projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
response = subprocess.check_output(command,shell=True)


# In[18]:

source_path = "{}/output_V{:02.0f}/{}.shp".format(GCS_OUTPUT_PATH,OUTPUT_VERSION,SCRIPT_NAME)


# In[19]:

source_path


# In[20]:

command = "/opt/anaconda3/envs/python35/bin/earthengine upload table --asset_id='projects/WRI-Aquaduct/{}/output_V{:02.0f}/gadm36l01' '{}' --max_vertices=1000000".format(SCRIPT_NAME,OUTPUT_VERSION,source_path)


# In[21]:

response = subprocess.check_output(command,shell=True)


# In[22]:

command


# In[23]:

# rasterize at 30s resolution


# In[24]:

#destination_path_tif = "{}/{}.tif".format(ec2_output_path,SCRIPT_NAME)


# In[25]:

"""
field = "gadm01id"
x_dimension = X_DIMENSION_30S
y_dimension = Y_DIMENSION_30S
layer_name = SCRIPT_NAME
input_path = destination_path_shp
output_path = destination_path_tif
"""


# In[26]:

#command = "{} -a {} -ot Integer64 -of GTiff -te -180 -90 180 90 -ts {} {} -co COMPRESS=DEFLATE -co PREDICTOR=1 -co ZLEVEL=6 -l {} -a_nodata -9999 {} {}".format(GDAL_RASTERIZE_PATH,field,x_dimension,y_dimension,layer_name,input_path,output_path)


# In[27]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:   
# 0:06:44.576493
# 

# In[ ]:



