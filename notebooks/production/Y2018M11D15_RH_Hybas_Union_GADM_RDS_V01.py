
# coding: utf-8

# In[1]:

""" Union of Hybas and GADM in Bigquey.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181115
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0
SCRIPT_NAME = 'Y2018M11D15_RH_Hybas_Union_GADM_RDS_V01'
OUTPUT_VERSION = 1

RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"

RDS_INPUT_TABLE_LEFT = "hybas06_v04"
RDS_INPUT_TABLE_RIGHT = "y2018m11d12_rh_gadm36_level1_to_rds_v01_v02"

RDS_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

print("\nRDS_DATABASE_ENDPOINT: ", RDS_DATABASE_ENDPOINT,
      "\nRDS_DATABASE_NAME: ", RDS_DATABASE_NAME,
      "\nRDS_INPUT_TABLE_LEFT: ",RDS_INPUT_TABLE_LEFT,
      "\nRDS_INPUT_TABLE_RIGHT: ",RDS_INPUT_TABLE_RIGHT,
      "\nRDS_OUTPUT_TABLE_NAME: ", RDS_OUTPUT_TABLE_NAME)




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


# In[4]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))


# In[5]:

sql = """
CREATE TABLE {} AS
-- input data
with polys1 AS (
  SELECT
    t1.pfaf_id,
    t1.geom
  FROM 
    {} t1
),
polys2 AS (
  SELECT
    t1.gid_1,
    t1.geom
  FROM 
    {} t1
),
-- left and right unions
union1 AS (
  SELECT ST_UNION(geom) FROM polys1
),
union2 AS (
  SELECT ST_UNION(geom) FROM polys2
),
-- various combinations of intersections
pairs AS (
  SELECT pfaf_id, gid_1, ST_INTERSECTION(a.geom, b.geom) geom FROM polys1 a, polys2 b WHERE ST_INTERSECTS(a.geom, b.geom) 
  UNION ALL
  SELECT pfaf_id, NULL, ST_DIFFERENCE(geom, (SELECT * FROM union2)) geog FROM polys1
  UNION ALL 
  SELECT NULL, gid_1, ST_DIFFERENCE(geom, (SELECT * FROM union1)) geog FROM polys2
)
SELECT * FROM pairs WHERE NOT ST_IsEmpty(geom)
""".format(RDS_OUTPUT_TABLE_NAME,RDS_INPUT_TABLE_LEFT,RDS_INPUT_TABLE_RIGHT)


# In[ ]:

result = engine.execute(sql)


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

