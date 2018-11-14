
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
SCRIPT_NAME = 'Y2018M11D14_RH_Hybas_Union_GADM_BQ_V01'
OUTPUT_VERSION = 1

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "geospatial_v01"

RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"
BQ_INPUT_TABLE_NAME = "y2018m11d14_rh_icepbasins_to_bq_v01_v01"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

print("\nRDS_DATABASE_ENDPOINT: ", RDS_DATABASE_ENDPOINT,
      "\nRDS_DATABASE_NAME: ", RDS_DATABASE_NAME,
      "\nBQ_INPUT_TABLE_NAME: ",BQ_INPUT_TABLE_NAME,
      "\nBQ_OUTPUT_DATASET_NAME: ", BQ_OUTPUT_DATASET_NAME,
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

job_config = bigquery.QueryJobConfig()


# In[ ]:

"""
SELECT
    t1.pfaf_id,
    t2.gid_1,
    ST_Intersection(ST_GeogFromText(t1.wkt),ST_GeogFromText(t2.wkt)) as geom
FROM 
    `aqueduct30.geospatial_v01.y2018m11d12_rh_hybas_rds_to_bq_v01_v01` t1,
    `aqueduct30.geospatial_v01.y2018m11d12_rh_gadm36_level1_rds_to_bq_v01_v01` t2
WHERE
    ST_Intersects(ST_GeogFromText(t1.wkt),ST_GeogFromText(t2.wkt))
"""


# In[5]:

q = """
SELECT
    t1.df1,
    t2.df2,
    ST_Intersection(ST_GeogFromText(t1.wkt),ST_GeogFromText(t2.wkt)) as geom
FROM 
    `aqueduct30.spatial_test.df1` t1,
    `aqueduct30.spatial_test.df2` t2
WHERE
    ST_Intersects(ST_GeogFromText(t1.wkt),ST_GeogFromText(t2.wkt))

"""


# In[6]:

destination_dataset_ref = client.dataset('sandbox')


# In[7]:

destination_table_ref = destination_dataset_ref.table('union_test01')


# In[8]:

job_config.destination = destination_table_ref


# In[9]:

query_job = client.query(query=q,
                         job_config=job_config)


# In[11]:

rows = query_job.result()


# In[ ]:



