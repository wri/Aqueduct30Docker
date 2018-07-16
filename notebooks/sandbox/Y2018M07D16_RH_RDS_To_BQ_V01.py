
# coding: utf-8

# In[ ]:

# Warning, approach does not scale. Use only for databases <100 million rows


# In[1]:

# imports
import re
import os
import numpy as np
import pandas as pd
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)
from google.cloud import bigquery

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = "y2018m07d10_rh_update_waterstress_aridlowonce_postgis_v01_v07"

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))


# In[ ]:




# In[2]:

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"


# In[3]:

bigquery_client = bigquery.Client()


# In[4]:

dataset_id = 'my_new_dataset'


# In[5]:

dataset_ref = bigquery_client.dataset(dataset_id)


# In[6]:

dataset = bigquery.Dataset(dataset_ref)


# In[7]:

dataset = bigquery_client.create_dataset(dataset)


# In[ ]:

datasets = list(bigquery_client.list_datasets())


# In[19]:

sql = "SELECT DISTINCT pfafid_30spfaf06 FROM {}".format(INPUT_TABLE_NAME)


# In[ ]:

df = pd.read_sql(sql,engine)


# In[ ]:

df.shape


# In[ ]:



