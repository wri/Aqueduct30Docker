
# coding: utf-8

# In[1]:

# Test to ingest data (pandas df) into Google Bigquery


# # Create Service Account
# https://cloud.google.com/docs/authentication/getting-started
# 
# 

# In[8]:

get_ipython().system('gcloud iam service-accounts create bq-user01')


# In[10]:

get_ipython().system('gcloud projects add-iam-policy-binding aqueduct30 --member "serviceAccount:bq-user01@aqueduct30.iam.gserviceaccount.com" --role "roles/owner"')


# In[11]:

get_ipython().system('gcloud iam service-accounts keys create /.google.json --iam-account bq-user01@aqueduct30.iam.gserviceaccount.com')


# In[18]:

get_ipython().system('export GOOGLE_APPLICATION_CREDENTIALS="/.google.json"')


# In[27]:

# imports
import re
import os
import numpy as np
import pandas as pd
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)
from google.cloud import bigquery


# In[3]:

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"


# In[4]:

bigquery_client = bigquery.Client()


# In[5]:

dataset_id = 'my_new_dataset'


# In[6]:

dataset_ref = bigquery_client.dataset(dataset_id)


# In[7]:

dataset = bigquery.Dataset(dataset_ref)


# In[8]:

dataset = bigquery_client.create_dataset(dataset)


# In[9]:

datasets = list(bigquery_client.list_datasets())


# In[10]:

datasets


# In[11]:

for item in datasets:
    print(item.dataset_id)


# In[12]:

# Create a table


# In[13]:

schema = [
    bigquery.SchemaField('full_name', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('age', 'INTEGER', mode='REQUIRED'),
]


# In[14]:

table_ref = dataset_ref.table('my_table')


# In[15]:

table = bigquery.Table(table_ref, schema=schema)


# In[17]:

table = bigquery_client.create_table(table)


# In[19]:

rows = bigquery_client.list_rows(table, max_results=10)


# In[21]:

rows_to_insert = [
    (u'Phred Phlyntstone', 32),
    (u'Wylma Phlyntstone', 29),
]


# In[22]:

errors = bigquery_client.insert_rows(table, rows_to_insert)


# In[24]:

table_ref = dataset_ref.table('from_pandas')


# In[29]:

# Get pandas dataframe from postgresql
DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
INPUT_TABLE_NAME = 'y2018m06d28_rh_ws_full_range_ols_postgis_30spfaf06_v02_v03'


F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
#connection = engine.connect()


# In[38]:

sql = "CREATE TABLE test01 AS SELECT * FROM {} LIMIT 100000".format(INPUT_TABLE_NAME)


# In[40]:

result = engine.execute(sql)


# In[43]:

sql = "SELECT * FROM {}".format("test01")
i = 0
for chunk in pd.read_sql_query(sql , engine, chunksize=10000):
    print("Chunk: ",i)
    i += 1
    df.to_gbq(destination_table="my_new_dataset.test_pandas",
              project_id = "aqueduct30",
              if_exists= "append" )
    


# In[ ]:




# In[ ]:




# In[30]:

sql = "SELECT * FROM {} LIMIT 10000".format(INPUT_TABLE_NAME)


# In[31]:

df = pd.read_sql(sql,engine)


# In[32]:

df.shape


# In[33]:

df.head()


# In[34]:

df.dtypes


# In[37]:




# In[ ]:



