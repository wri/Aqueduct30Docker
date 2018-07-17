
# coding: utf-8

# In[1]:

""" Upload RDS table to BQ
-------------------------------------------------------------------------------

WARNING!!! DELETES DATASET!!!

Author: Rutger Hofste
Date: 20180712
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04
"""

# imports
import re
import os
import numpy as np
import pandas as pd
from retrying import retry
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)
from google.cloud import bigquery
import multiprocessing

SCRIPT_NAME = 'Y2018M07D16_RH_RDS_To_BQ_V01'
OUTPUT_VERSION = 5

TESTING = 0

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = "y2018m07d12_rh_ws_categorization_label_postgis_v01_v04"

OUTPUT_DATASET_NAME = "aqueduct30v01"
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)

print("Input Table: " , INPUT_TABLE_NAME, 
      "\nOutput Table: " , OUTPUT_DATASET_NAME,".",OUTPUT_TABLE_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))


# In[4]:

cpu_count = multiprocessing.cpu_count()
print("Power to the maxxx:", cpu_count)


# In[5]:

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"


# In[6]:

bigquery_client = bigquery.Client()


# In[7]:

dataset_id = 'my_new_dataset'
dataset_ref = bigquery_client.dataset(dataset_id)
bigquery_client.delete_dataset(dataset_ref)
dataset = bigquery.Dataset(dataset_ref)
dataset = bigquery_client.create_dataset(dataset)


# In[8]:

sql = "SELECT DISTINCT pfafid_30spfaf06 FROM {} ORDER BY pfafid_30spfaf06".format(INPUT_TABLE_NAME)


# In[9]:

df = pd.read_sql(sql,engine)


# In[10]:

df.head()


# In[11]:

df.shape


# In[12]:

if TESTING:
    df = df[0:10]


# In[13]:

df_split = np.array_split(df, cpu_count*100)


# In[14]:

len(df_split)


# In[15]:

# pfaf_ids are been split in 1600 "packages" with appr. 11 rows each". 


# In[16]:

df_split_first = df_split[0]
df_split_remainder = df_split[1:]


# In[17]:

@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
def df_to_bg(df):
    for index, row in df.iterrows():
        pfafid = row["pfafid_30spfaf06"]
        sql = "SELECT * FROM {} WHERE pfafid_30spfaf06 = {}".format(INPUT_TABLE_NAME,pfafid)
        df_basin = pd.read_sql(sql,engine)
        # add timestamp
        now = datetime.datetime.now()
        df_basin["processed_timestamp"] = pd.Timestamp(now)        
        df_basin.to_gbq(destination_table="{}.{}".format(OUTPUT_DATASET_NAME,OUTPUT_TABLE_NAME),
                        project_id = "aqueduct30",
                        if_exists= "append" )
        time.sleep(0.5)
        print(index)


# In[18]:

# Upload first shard separately to initiate table schema
df_to_bg(df_split_first)


# In[ ]:

p= multiprocessing.Pool()
results_buffered = p.map(df_to_bg,df_split_remainder)
p.close()
p.join()


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:



