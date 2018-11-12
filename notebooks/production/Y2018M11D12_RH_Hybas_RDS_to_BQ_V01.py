
# coding: utf-8

# In[1]:

""" Upload hydrobasin geospatial data to bigquery
-------------------------------------------------------------------------------


"""

TESTING = 0
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M11D12_RH_Hybas_RDS_to_BQ_V01'
OUTPUT_VERSION = 1

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"

RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"
RDS_INPUT_TABLE_NAME = "hybas06_v04"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()


print("\nRDS_DATABASE_ENDPOINT: ", RDS_DATABASE_ENDPOINT,
      "\nRDS_DATABASE_NAME: ", RDS_DATABASE_NAME,
      "\nRDS_INPUT_TABLE_NAME: ",RDS_INPUT_TABLE_NAME,
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

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))


# In[5]:

sql = """
SELECT
  pfaf_id,
  geom,
  ST_AsText(geom) AS wkt
FROM
  {}
""".format(RDS_INPUT_TABLE_NAME)


# In[6]:

gdf = gpd.read_postgis(sql=sql,
                       con=engine)


# In[7]:

gdf.shape


# In[8]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME)


# In[9]:

df = pd.DataFrame(gdf.drop("geom",1))


# In[10]:

if TESTING:
    df = df.sample(1000)


# In[ ]:

df.to_gbq(destination_table=destination_table,
          project_id=BQ_PROJECT_ID,
          chunksize=1000,
          if_exists="replace")


# In[ ]:

engine.dispose()


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

