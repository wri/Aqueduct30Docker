
# coding: utf-8

# In[1]:

""" Convert union of hybas, gadm and whymap to geopackage.
-------------------------------------------------------------------------------

Performance has been significantly improved with the help of Google Experts on
the Bigquery forum.

Author: Rutger Hofste
Date: 20181126
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""


SCRIPT_NAME = 'Y2018M11D26_RH_Hybasgadmwhymap_Union_To_Gpkg_V01'
OUTPUT_VERSION = 2

BQ_PROJECT_ID = "aqueduct30"
BQ_DATASET_NAME = "geospatial_geog_v01"

BQ_INPUT_TABLE = "y2018m11d14_rh_hybasgadm_union_whymap_bq_v01_v02"

ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nBQ_DATASET_NAME: ", BQ_DATASET_NAME,
      "\nBQ_INPUT_TABLE: ",BQ_INPUT_TABLE,
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
import geojson
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery
from shapely.geometry import MultiPolygon, shape
from shapely import wkt

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[5]:

q = """
SELECT
  id_pfafgadmwhymap,
  id_pfafgadm,
  aqid,
  ST_AsTEXT(g) AS wkt
FROM
  `aqueduct30.{}.{}`
WHERE NOT ST_IsEmpty(g)
""".format(BQ_DATASET_NAME,BQ_INPUT_TABLE)


# In[6]:

df = pd.read_gbq(query=q,dialect='standard')


# In[ ]:

df.head()


# In[ ]:

df["geom"] = df["wkt"].apply(lambda x: MultiPolygon([wkt.loads(x)]))


# In[ ]:

df = df.drop(columns=["wkt"])


# In[ ]:

gdf = gpd.GeoDataFrame(df, geometry="geom")


# In[ ]:

gdf.crs = "+init=epsg:4326"


# In[ ]:

gdf.shape


# In[ ]:

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
