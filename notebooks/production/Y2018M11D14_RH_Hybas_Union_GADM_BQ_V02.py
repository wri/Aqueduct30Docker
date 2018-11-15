
# coding: utf-8

# In[3]:

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
BQ_DATASET_NAME = "geospatial_geog_v01"

BQ_INPUT_TABLE_LEFT = "y2018m11d12_rh_hybas_rds_to_bq_v01_v01"
BQ_INPUT_TABLE_RIGHT = "y2018m11d12_rh_gadm36_level1_rds_to_bq_v01_v01"

BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

print("\nBQ_DATASET_NAME: ", BQ_DATASET_NAME,
      "\nBQ_INPUT_TABLE_LEFT: ",BQ_INPUT_TABLE_LEFT,
      "\nBQ_INPUT_TABLE_RIGHT: ",BQ_INPUT_TABLE_RIGHT,
      "\nBQ_OUTPUT_TABLE_NAME: ", BQ_OUTPUT_TABLE_NAME)


# In[4]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[5]:

import os
import sqlalchemy
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[13]:

q = """
-- input data
with polys1 AS (
  SELECT
  pfaf_id,
  geog
  FROM `{}.{}`
),
polys2 AS (
  SELECT
  gid_1,
  geog
  FROM `{}.{}`
),
-- left and right unions
union1 AS (
  SELECT ST_UNION_AGG(geog) FROM polys1
),
union2 AS (
  SELECT ST_UNION_AGG(geog) FROM polys2
),
-- various combinations of intersections
pairs AS (
  SELECT pfaf_id, gid_1, ST_INTERSECTION(a.geog, b.geog) geog FROM polys1 a, polys2 b WHERE ST_INTERSECTS(a.geog, b.geog)
  UNION ALL
  SELECT pfaf_id, NULL, ST_DIFFERENCE(geog, (SELECT * FROM union2)) geog FROM polys1
  UNION ALL 
  SELECT NULL, gid_1, ST_DIFFERENCE(geog, (SELECT * FROM union1)) geog FROM polys2
)
SELECT * FROM pairs WHERE NOT ST_IsEmpty(geog)
""".format(BQ_DATASET_NAME,BQ_INPUT_TABLE_LEFT,BQ_DATASET_NAME,BQ_INPUT_TABLE_RIGHT)


# In[7]:

job_config = bigquery.QueryJobConfig()


# In[8]:

destination_dataset_ref = client.dataset(BQ_DATASET_NAME)


# In[9]:

destination_table_ref = destination_dataset_ref.table(BQ_OUTPUT_TABLE_NAME)


# In[10]:

job_config.destination = destination_table_ref


# In[14]:

query_job = client.query(query=q,
                         job_config=job_config)


# In[15]:

rows = query_job.result()


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:



