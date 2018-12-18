
# coding: utf-8

# In[1]:

""" Ingest GADM level 1 data to earthengine. 
-------------------------------------------------------------------------------

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
OUTPUT_VERSION = 2

# Database settings
RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"
INPUT_TABLE_NAME = "y2018m11d12_rh_gadm36_level1_to_rds_v01_v02"

GCS_OUTPUT_PATH = "gs://aqueduct30_v01/{}".format(SCRIPT_NAME)

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
""".format(INPUT_TABLE_NAME)


# In[7]:

gdf =gpd.GeoDataFrame.from_postgis(q,connection,geom_col='geom' )


# In[8]:

gdf.head()


# In[9]:

destination_path = "{}/{}.shp".format(ec2_output_path,SCRIPT_NAME)


# In[10]:

gdf.to_file(filename=destination_path,driver="ESRI Shapefile")


# In[11]:

get_ipython().system('gsutil -m cp -r {ec2_output_path} {GCS_OUTPUT_PATH}')


# In[12]:

command = "earthengine create folder projects/WRI-Aquaduct/{}".format(SCRIPT_NAME)
response = subprocess.check_output(command,shell=True)


# In[13]:

command = "earthengine create folder projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
response = subprocess.check_output(command,shell=True)


# In[14]:

source_path = "{}/{}.shp".format(GCS_OUTPUT_PATH,SCRIPT_NAME)


# In[15]:

source_path


# In[16]:

command = "earthengine upload table --asset_id=projects/WRI-Aquaduct/{}/output_V{:02.0f}/gadm36l01 {}".format(SCRIPT_NAME,OUTPUT_VERSION,source_path)
response = subprocess.check_output(command,shell=True)


# In[17]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

