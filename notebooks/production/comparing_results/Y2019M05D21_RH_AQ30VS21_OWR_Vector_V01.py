
# coding: utf-8

# In[8]:

"""Compare overall water risk using the vecotor results.
-------------------------------------------------------------------------------

Recap: The quantiles approach has been applied to all weightings, not just
the default one. Therefore there are slight variations in the histogram for
"def" or default weighting. 


Author: Rutger Hofste
Date: 20190521
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = "Y2019M05D21_RH_AQ30VS21_OWR_Vector_V01"
OUTPUT_VERSION = 1

BQ_PROJECT_ID = "aqueduct30"
BQ_DATASET_NAME = "aqueduct30v01"
BQ_INPUT_TABLE_NAME =  "y2018m12d11_rh_master_weights_gpd_v02_v08"

s3_output_path = "s3://wri-projects/Aqueduct30/Aq30vs21/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\ns3_output_path: " + s3_output_path)


# In[20]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version

get_ipython().magic('matplotlib inline')


# In[6]:

import os
import numpy as np
import pandas as pd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[45]:

# Load AQ3 Overall water risk data (Default weighting)

sql = """
SELECT
  *
FROM
 `{}.{}.{}`
WHERE
  indicator = 'awr'
  AND industry_short = 'def'
  AND group_short = "tot"
""".format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_INPUT_TABLE_NAME)
       


# In[46]:

df = pd.read_gbq(query=sql,
                 project_id =BQ_PROJECT_ID,
                 dialect="standard")


# In[47]:

df.shape


# In[48]:

df.head()


# In[49]:

df.hist(column="score",bins=5)


# In[ ]:



