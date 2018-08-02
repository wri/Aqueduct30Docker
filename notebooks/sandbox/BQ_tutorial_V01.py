
# coding: utf-8

# In[1]:

# tutorial https://cloud.google.com/bigquery/docs/bigqueryml-scientist-start


# In[2]:

import os
import pandas as pd
from google.cloud import bigquery
get_ipython().magic('load_ext google.cloud.bigquery')


# In[3]:

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"


# In[4]:

client = bigquery.Client()


# ### Step two create your dataset

# In[6]:

dataset = bigquery.Dataset(client.dataset('bqml_tutorial'))
dataset.location = 'US'
client.create_dataset(dataset)


# ### Step three: Create your model

# In[8]:

get_ipython().run_cell_magic('bigquery', '', 'CREATE OR REPLACE MODEL `bqml_tutorial.sample_model`\nOPTIONS(model_type=\'logistic_reg\') AS\nSELECT\n  IF(totals.transactions IS NULL, 0, 1) AS label,\n  IFNULL(device.operatingSystem, "") AS os,\n  device.isMobile AS is_mobile,\n  IFNULL(geoNetwork.country, "") AS country,\n  IFNULL(totals.pageviews, 0) AS pageviews\nFROM\n  `bigquery-public-data.google_analytics_sample.ga_sessions_*`\nWHERE\n  _TABLE_SUFFIX BETWEEN \'20160801\' AND \'20170630\'')


# In[ ]:




# In[ ]:




# In[ ]:



