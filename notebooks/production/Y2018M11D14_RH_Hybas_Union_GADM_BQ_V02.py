
# coding: utf-8

# In[1]:

""" Union of Hybas and GADM in Bigquey.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181114
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M11D14_RH_Hybas_Union_GADM_BQ_V02'
OUTPUT_VERSION = 1

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "geospatial_v01"

RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"

RDS_INPUT_TABLE_LEFT = "hybas06_v04"
RDS_INPUT_TABLE_RIGHT = "y2018m11d12_rh_gadm36_level1_to_rds_v01_v02"


BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

print("\nRDS_DATABASE_ENDPOINT: ", RDS_DATABASE_ENDPOINT,
      "\nRDS_DATABASE_NAME: ", RDS_DATABASE_NAME,
      "\nRDS_INPUT_TABLE_LEFT: ",RDS_INPUT_TABLE_LEFT,
      "\nRDS_INPUT_TABLE_RIGHT: ",RDS_INPUT_TABLE_LEFT,
      "\nBQ_OUTPUT_TABLE_NAME: ", BQ_OUTPUT_TABLE_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import os
import sqlalchemy
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[4]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))


# In[7]:

sql = """
SELECT
  pfaf_id,
  geom
FROM
  {}
""".format(RDS_INPUT_TABLE_LEFT)


# In[9]:

gdf_left = gpd.read_postgis(sql=sql,
                            con=engine)


# In[14]:

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
  geom
FROM
  {}
""".format(RDS_INPUT_TABLE_RIGHT)


# In[16]:

gdf_right = gpd.read_postgis(sql=sql,
                            con=engine)


# In[ ]:

gdf_union = gpd.overlay(gfd_left,gdf_right,how="union")

