
# coding: utf-8

# In[1]:

""" Union of hybasgadm and Whymap in Bigquery.
-------------------------------------------------------------------------------

Performance has been significantly improved with the help of Google Experts on
the Bigquery forum.

Author: Rutger Hofste
Date: 20181121
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M11D21_RH_Hybasgadmwhymap_Union_Icep_BQ_V01'
OUTPUT_VERSION = 1

BQ_PROJECT_ID = "aqueduct30"
BQ_DATASET_NAME = "geospatial_geog_v01"

BQ_INPUT_TABLE_LEFT = "y2018m11d14_rh_hybasgadm_union_whymap_bq_v01_v02"
BQ_INPUT_TABLE_RIGHT = "y2018m11d14_rh_icepbasins_to_bq_v01_v01"

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

import os
import sqlalchemy
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[4]:

q = """
  -- input data
WITH
  polys1 AS (
  SELECT
    t1.id_pfafgadmwhymap,
    t1.g as g
  FROM
    `{}.{}` t1 ),
  polys2 AS (
  SELECT
    t2.icepbasinid,
    t2.geog as g
  FROM
    `{}.{}` t2 ),
  -- intersections
  intersections AS (
    SELECT id_pfafgadmwhymap, icepbasinid, ST_INTERSECTION(a.g, b.g) i, a.g AS g1, b.g AS g2 
    FROM polys1 a, polys2 b WHERE ST_INTERSECTS(a.g, b.g)
  ),
  -- per-row union of intersections with this row
  diff1 AS (
    SELECT id_pfafgadmwhymap, ST_UNION_AGG(i) i FROM intersections GROUP BY id_pfafgadmwhymap
  ),
  diff2 AS (
    SELECT icepbasinid, ST_UNION_AGG(i) i FROM intersections GROUP BY icepbasinid
  ),
  -- various combinations of intersections
  pairs AS (
    SELECT id_pfafgadmwhymap, icepbasinid, i AS g FROM intersections
    UNION ALL
    SELECT p.id_pfafgadmwhymap, NULL, IF(i IS NULL, g, ST_DIFFERENCE(g, i)) FROM polys1 p LEFT JOIN diff1 d ON p.id_pfafgadmwhymap = d.id_pfafgadmwhymap
    UNION ALL 
    SELECT NULL, p.icepbasinid, IF(i IS NULL, g, ST_DIFFERENCE(g, i)) FROM polys2 p LEFT JOIN diff2 d ON p.icepbasinid = d.icepbasinid
  )
  SELECT CONCAT(COALESCE(CAST(id_pfafgadmwhymap AS STRING),'nodata'),
         "-",
         COALESCE(CAST(icepbasinid AS STRING),'nodata')) AS id_pfafgadmwhymapicep, 
         *
  FROM pairs WHERE NOT ST_IsEmpty(g)

""".format(BQ_DATASET_NAME,BQ_INPUT_TABLE_LEFT,BQ_DATASET_NAME,BQ_INPUT_TABLE_RIGHT)


# In[5]:

job_config = bigquery.QueryJobConfig()


# In[6]:

destination_dataset_ref = client.dataset(BQ_DATASET_NAME)


# In[7]:

destination_table_ref = destination_dataset_ref.table(BQ_OUTPUT_TABLE_NAME)


# In[8]:

job_config.destination = destination_table_ref


# In[9]:

query_job = client.query(query=q,
                         job_config=job_config)


# In[ ]:

rows = query_job.result()


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 
