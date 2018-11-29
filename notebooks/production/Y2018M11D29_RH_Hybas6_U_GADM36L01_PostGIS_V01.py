
# coding: utf-8

# In[1]:

""" Union of hydrobasin and GADM 36 level 1 using postGIS
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181128
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0
SCRIPT_NAME = "Y2018M11D29_RH_Hybas6_U_GADM36L01_PostGIS_V01"
OUTPUT_VERSION = 2

RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"
RDS_INPUT_TABLE_LEFT = "hybas06_v04"
RDS_INPUT_TABLE_RIGHT = "y2018m11d12_rh_gadm36_level1_to_rds_v01_v02"

OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()



print("\nRDS_INPUT_TABLE_LEFT:", RDS_INPUT_TABLE_LEFT,
      "\nRDS_INPUT_TABLE_RIGHT: ", RDS_INPUT_TABLE_RIGHT,
      "\nOUTPUT_TABLE_NAME: ", OUTPUT_TABLE_NAME)


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


# In[4]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))


# In[5]:

q = """
CREATE TABLE public.{} AS 
-- input data
with polys1 AS (
  SELECT 
    pfaf_id as df1,
    geom as g
  FROM {}
),
polys2 AS (
  SELECT 
    gid_1 as df2,
    geom as g
  FROM {}
),
-- intersections
intersections AS (
  SELECT df1, df2, ST_INTERSECTION(a.g, b.g) i, a.g AS g1, b.g AS g2 
  FROM polys1 a, polys2 b WHERE ST_INTERSECTS(a.g, b.g)
),
-- per-row union of intersections with this row
diff1 AS (
  SELECT df1, ST_UNION(i) i FROM intersections GROUP BY df1
),
diff2 AS (
  SELECT df2, ST_UNION(i) i FROM intersections GROUP BY df2
),
-- various combinations of intersections
pairs AS (
  SELECT df1, df2, i AS g FROM intersections
  UNION ALL
  SELECT 
    p.df1,
    NULL,
    CASE
      WHEN i IS NULL THEN g 
      ELSE ST_DIFFERENCE(g, i)
    END
  FROM polys1 p LEFT JOIN diff1 d ON p.df1 = d.df1
  UNION ALL
  SELECT
    NULL,
    p.df2,
    CASE
      WHEN i IS NULL THEN g
      ELSE ST_DIFFERENCE(g, i)
    END
  FROM polys2 p LEFT JOIN diff2 d ON p.df2 = d.df2  
)
SELECT 
  df1 as pfaf_id,
  df2 as gid_1,
  g as geom
FROM pairs WHERE NOT ST_IsEmpty(g);
""".format(OUTPUT_TABLE_NAME,RDS_INPUT_TABLE_LEFT,RDS_INPUT_TABLE_RIGHT)


# In[ ]:

result = engine.execute(q)


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:11:30.825157
# 
