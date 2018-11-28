
# coding: utf-8

# In[1]:

""" Export table with geographies as WKT to CSV file on GCS. 
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181128
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_
    NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.
    DATABASE_ENDPOINT (string) : RDS or postGreSQL endpoint.
    DATABASE_NAME (string) : Database name.
    
    TABLE_NAME_AREA_30SPFAF06 (string) : Table name used for areas. Must exist
        on same database as used in rest of script.
    S3_INPUT_PATH_RIVERDISCHARGE (string) : AWS S3 input path for 
        riverdischarge.    
    S3_INPUT_PATH_DEMAND (string) : AWS S3 input path for 
        demand.     

"""

TESTING = 1
SCRIPT_NAME = "Y2018M11D28_RH_hybasgadmwhymap_To_GCS_V01"
OUTPUT_VERSION = 1


BQ_PROJECT_ID = "aqueduct30"
BQ_INPUT_DATASET_NAME = "geospatial_geog_v01"
BQ_OUTPUT_DATASET_NAME = "geospatial_wkt_v01"
BQ_INPUT_TABLE_NAME = "y2018m11d14_rh_hybasgadm_union_whymap_bq_v01_v02"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

GCS_OUTPUT_PATH = "gs://aqueduct30_v01/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)


print("\nBQ_INPUT_DATASET_NAME: ", BQ_OUTPUT_DATASET_NAME,
      "\nBQ_INPUT_TABLE_NAME: ", BQ_INPUT_TABLE_NAME, 
      "\nBQ_OUTPUT_DATASET_NAME:", BQ_OUTPUT_DATASET_NAME,
      "\nBQ_OUTPUT_TABLE_NAME: ",BQ_OUTPUT_TABLE_NAME,
      "\nGCS_OUTPUT_PATH: ", GCS_OUTPUT_PATH
      )



# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[4]:

job_config = bigquery.QueryJobConfig()


# In[5]:

destination_dataset_ref = client.dataset(BQ_OUTPUT_DATASET_NAME)


# In[6]:

destination_table_ref = destination_dataset_ref.table(BQ_OUTPUT_TABLE_NAME)


# In[7]:

job_config.destination = destination_table_ref


# In[8]:

q = """
SELECT
  id_pfafgadmwhymap,
  ST_ASTEXT(g) as wkt
FROM
  `aqueduct30.{}.{}`
""".format(BQ_INPUT_DATASET_NAME,BQ_INPUT_TABLE_NAME)


# In[9]:

query_job = client.query(query=q,
                         job_config=job_config)


# In[10]:

# Exports geographies as WKT to new table


# In[11]:

rows = query_job.result()


# In[12]:

# Export to GCS (https://cloud.google.com/bigquery/docs/exporting-data)


# In[17]:

destination_uri = "{}/output_*.csv".format(GCS_OUTPUT_PATH)


# In[18]:

extract_job = client.extract_table(
    destination_table_ref,
    destination_uri,
    # Location must match that of the source table.
    location='US')  # API request


# In[19]:

extract_job.result()


# In[20]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 
