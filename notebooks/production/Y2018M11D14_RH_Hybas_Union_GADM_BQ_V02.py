
# coding: utf-8

# In[140]:

""" Union of Hybas and GADM in Bigquey.
-------------------------------------------------------------------------------

Performance has been significantly improved with the help of Google Experts on
the Bigquery forum.

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


# In[112]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[3]:

import os
import sqlalchemy
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[18]:

q = """
  -- input data
WITH
  polys1 AS (
  SELECT
    t1.pfaf_id,
    t1.geog as g
  FROM
    `{}.{}` t1 ),
  polys2 AS (
  SELECT
    t1.gid_1,
    t1.gid_0,
    t1.geog as g
  FROM
    `{}.{}` t1 ),
  -- intersections
  intersections AS (
    SELECT pfaf_id, gid_1, ST_INTERSECTION(a.g, b.g) i, a.g AS g1, b.g AS g2 
    FROM polys1 a, polys2 b WHERE ST_INTERSECTS(a.g, b.g)
  ),
  -- per-row union of intersections with this row
  diff1 AS (
    SELECT pfaf_id, ST_UNION_AGG(i) i FROM intersections GROUP BY pfaf_id
  ),
  diff2 AS (
    SELECT gid_1, ST_UNION_AGG(i) i FROM intersections GROUP BY gid_1
  ),
  -- various combinations of intersections
  pairs AS (
    SELECT pfaf_id, gid_1, i AS g FROM intersections
    UNION ALL
    SELECT p.pfaf_id, NULL, IF(i IS NULL, g, ST_DIFFERENCE(g, i)) FROM polys1 p LEFT JOIN diff1 d ON p.pfaf_id = d.pfaf_id
    UNION ALL 
    SELECT NULL, p.gid_1, IF(i IS NULL, g, ST_DIFFERENCE(g, i)) FROM polys2 p LEFT JOIN diff2 d ON p.gid_1 = d.gid_1
  )
  SELECT * FROM pairs WHERE NOT ST_IsEmpty(g)
""".format(BQ_DATASET_NAME,BQ_INPUT_TABLE_LEFT,BQ_DATASET_NAME,BQ_INPUT_TABLE_RIGHT)


# In[19]:

job_config = bigquery.QueryJobConfig()


# In[20]:

destination_dataset_ref = client.dataset(BQ_DATASET_NAME)


# In[21]:

destination_table_ref = destination_dataset_ref.table(BQ_OUTPUT_TABLE_NAME)


# In[22]:

job_config.destination = destination_table_ref


# In[23]:

query_job = client.query(query=q,
                         job_config=job_config)


# In[24]:

rows = query_job.result()


# In[122]:

q = """
SELECT
    pfaf_id as id,
    gid_1 as name,
    ST_AsGeoJSON(g) geom   
FROM 
    {}.{} 
LIMIT 100""".format(BQ_DATASET_NAME,BQ_OUTPUT_TABLE_NAME)


# In[123]:

df = pd.read_gbq(query=q,
                 dialect='standard')


# In[124]:

df.head()


# In[125]:

import json
from shapely.geometry import MultiPolygon


# In[133]:

df["geom_shapely"] = df["geom"].apply(lambda x: MultiPolygon([shape(json.loads(x))]),1)


# In[135]:

df = df.drop("geom",1)


# In[136]:

gdf = gpd.GeoDataFrame(data=df,geometry="geom_shapely")


# In[ ]:

g


# In[137]:

output_file_path = "{}/{}_V{:02.0f}.gpkg".format(ec2_output_path,SCRIPT_NAME,OUTPUT_VERSION)


# In[138]:

gdf.to_file(filename=output_file_path,
            driver="GPKG")


# In[141]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[142]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:13:06.158866    
#     

# In[ ]:



