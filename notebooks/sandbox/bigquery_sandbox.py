
# coding: utf-8

# In[1]:

# Test to ingest data into Google Bigquery




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


# In[29]:

import os
from google.cloud import bigquery, bigquery_datatransfer


# In[21]:

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"


# In[23]:

bigquery_client = bigquery.Client()


# In[24]:

dataset_id = 'my_new_dataset'


# In[25]:

dataset_ref = bigquery_client.dataset(dataset_id)


# In[26]:

dataset = bigquery.Dataset(dataset_ref)


# In[28]:

dataset = bigquery_client.create_dataset(dataset)


# In[31]:

datasets = list(bigquery_client.list_datasets())


# In[32]:

datasets


# In[34]:

for item in datasets:
    print(item.dataset_id)


# In[ ]:



