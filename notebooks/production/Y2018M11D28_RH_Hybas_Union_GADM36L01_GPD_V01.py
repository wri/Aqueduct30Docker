
# coding: utf-8

# In[19]:

""" Union of hydrobasin and GADM 36 level 1 using geopandas.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181128
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
SCRIPT_NAME = "Y2018M11D28_RH_Hybas_Union_GADM36L01_GPD_V01"
OUTPUT_VERSION = 1

RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"
RDS_INPUT_TABLE_LEFT = "y2018m11d12_rh_gadm36_level1_to_rds_v01_v02"
RDS_INPUT_TABLE_RIGHT = "hybas06_v04"

ec2_output_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)


print("\nec2_output_path:", ec2_output_path,
      "\ns3_output_path: ", s3_output_path)



# In[3]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[18]:

get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

import os
import sqlalchemy
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery


# In[5]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))


# In[6]:

sql = """
SELECT
  gid_1,
  name_1,
  gid_0,
  name_0,
  varname_1,
  nl_name_1,
  type_1,
  engtype_1,
  cc_1,
  hasc_1,
  geom,
  ST_AsText(geom) AS wkt
FROM
  {}
""".format(RDS_INPUT_TABLE_LEFT)


# In[7]:

gdf_left = gpd.read_postgis(sql=sql,
                            con=engine)


# In[8]:

gdf_left.shape


# In[9]:

sql = """
SELECT
  pfaf_id,
  geom,
  ST_AsText(geom) AS wkt
FROM
  {}
""".format(RDS_INPUT_TABLE_RIGHT)


# In[10]:

gdf_right = gpd.read_postgis(sql=sql,
                             con=engine)


# In[11]:

gdf_right.shape


# In[ ]:

gdf_union = gpd.overlay(gdf_left, gdf_right, how='union')


# In[ ]:

gdf_union.crs = "+init=epsg:4326"


# In[17]:

output_file_path = "{}/{}.gpkg".format(ec2_output_path,SCRIPT_NAME)


# In[ ]:

gdf.to_file(filename=output_file_path,driver="GPKG")


# In[ ]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 
