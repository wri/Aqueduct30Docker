
# coding: utf-8

# In[1]:

"""Queries drought risk and stores in Carto.
-------------------------------------------------------------------------------

"""


SCRIPT_NAME = 'Y2018M09D28_RH_QA_DR_Carto_V01'
OUTPUT_VERSION = 2

BQ_PROJECT_ID = "aqueduct30"
BQ_INPUT_DATASET_NAME = "aqueduct30v01"
BQ_INPUT_TABLE_NAME = "y2018m09d28_rh_dr_cat_label_v01_v02"

CARTO_OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)

print("Input BQ Table : " + BQ_INPUT_TABLE_NAME +
      "\nCARTO_OUTPUT_TABLE_NAME: " + CARTO_OUTPUT_TABLE_NAME)


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
import cartoframes
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery
get_ipython().magic('load_ext google.cloud.bigquery')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[4]:

F = open("/.carto_builder","r")
carto_api_key = F.read().splitlines()[0]
F.close()
creds = cartoframes.Credentials(key=carto_api_key, 
                    username='wri-playground')
cc = cartoframes.CartoContext(creds=creds)


# In[5]:

sql = """SELECT
  PFAF_ID,
  droughthazard_dimensionless,
  droughthazard_count,
  droughthazard_score,
  droughthazard_cat,
  droughthazard_label,
  droughtrisk_dimensionless,
  droughtrisk_count,
  droughtrisk_score,
  droughtrisk_cat,
  droughtrisk_label,
  droughtexposure_dimensionless,
  droughtexposure_count,
  droughtexposure_score,
  droughtexposure_cat,
  droughtexposure_label,
  droughtvulnerability_dimensionless,
  droughtvulnerability_count,
  droughtvulnerability_score,
  droughtvulnerability_cat,
  droughtvulnerability_label
FROM
  `{}.{}`
""".format(BQ_INPUT_DATASET_NAME,BQ_INPUT_TABLE_NAME)


# In[6]:

print(sql)


# In[7]:

df = pd.read_gbq(query=sql,dialect="standard")


# In[8]:

df.shape


# In[9]:

# Upload result data to Carto
cc.write(df=df,
         table_name=CARTO_OUTPUT_TABLE_NAME,
         overwrite=True,
         privacy="public")


# In[10]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:21.165826

# In[ ]:



