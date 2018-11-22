
# coding: utf-8

# In[1]:

""" Union of hybasgadm and Whymap in Bigquery.
-------------------------------------------------------------------------------

Performance has been significantly improved with the help of Google Experts on
the Bigquery forum.

Author: Rutger Hofste
Date: 20181116
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M11D14_RH_hybasgadm_Union_whymap_BQ_V01'
OUTPUT_VERSION = 2

BQ_PROJECT_ID = "aqueduct30"
BQ_DATASET_NAME = "geospatial_geog_v01"

BQ_INPUT_TABLE_LEFT = "y2018m11d14_rh_hybas_union_gadm_bq_v02_v02"
BQ_INPUT_TABLE_RIGHT = "y2018m11d14_rh_whymap_rds_to_bq_v01_v01"

BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nBQ_DATASET_NAME: ", BQ_DATASET_NAME,
      "\nBQ_INPUT_TABLE_LEFT: ",BQ_INPUT_TABLE_LEFT,
      "\nBQ_INPUT_TABLE_RIGHT: ",BQ_INPUT_TABLE_RIGHT,
      "\nBQ_OUTPUT_TABLE_NAME: ", BQ_OUTPUT_TABLE_NAME,
      "\nec2_output_path:",ec2_output_path,
      "\ns3_output_path:",s3_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

import os
import sqlalchemy
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[5]:

q = """
  -- input data
WITH
  polys1 AS (
  SELECT
    t1.id_pfafgadm,
    t1.g as g
  FROM
    `{}.{}` t1 ),
  polys2 AS (
  SELECT
    t2.aqid,
    t2.geog as g
  FROM
    `{}.{}` t2 ),
  -- intersections
  intersections AS (
    SELECT id_pfafgadm, aqid, ST_INTERSECTION(a.g, b.g) i, a.g AS g1, b.g AS g2 
    FROM polys1 a, polys2 b WHERE ST_INTERSECTS(a.g, b.g)
  ),
  -- per-row union of intersections with this row
  diff1 AS (
    SELECT id_pfafgadm, ST_UNION_AGG(i) i FROM intersections GROUP BY id_pfafgadm
  ),
  diff2 AS (
    SELECT aqid, ST_UNION_AGG(i) i FROM intersections GROUP BY aqid
  ),
  -- various combinations of intersections
  pairs AS (
    SELECT id_pfafgadm, aqid, i AS g FROM intersections
    UNION ALL
    SELECT p.id_pfafgadm, NULL, IF(i IS NULL, g, ST_DIFFERENCE(g, i)) FROM polys1 p LEFT JOIN diff1 d ON p.id_pfafgadm = d.id_pfafgadm
    UNION ALL 
    SELECT NULL, p.aqid, IF(i IS NULL, g, ST_DIFFERENCE(g, i)) FROM polys2 p LEFT JOIN diff2 d ON p.aqid = d.aqid
  )
  SELECT CONCAT(COALESCE(CAST(id_pfafgadm AS STRING),'nodata'),
         "-",
         COALESCE(CAST(aqid AS STRING),'nodata')) AS id_pfafgadmwhymap, 
         *
  FROM pairs WHERE NOT ST_IsEmpty(g)
""".format(BQ_DATASET_NAME,BQ_INPUT_TABLE_LEFT,BQ_DATASET_NAME,BQ_INPUT_TABLE_RIGHT)


# In[6]:

job_config = bigquery.QueryJobConfig()


# In[7]:

destination_dataset_ref = client.dataset(BQ_DATASET_NAME)


# In[8]:

destination_table_ref = destination_dataset_ref.table(BQ_OUTPUT_TABLE_NAME)


# In[9]:

job_config.destination = destination_table_ref


# In[10]:

query_job = client.query(query=q,
                         job_config=job_config)


# In[11]:

rows = query_job.result()


# In[12]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:41:16.307653
# 

# In[ ]:



