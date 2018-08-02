
# coding: utf-8

# In[1]:

""" Upload pandas dataframe to Bigquery for sandboxing new features.
-------------------------------------------------------------------------------



"""

TESTING = 0
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M08D01_RH_Sample_DataFrame_to_BQ_V01'
OUTPUT_VERSION = 2

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "sandbox"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

print("bq dataset name: ", BQ_OUTPUT_DATASET_NAME,
      "\nOutput bq table name: ", BQ_OUTPUT_TABLE_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import os
import numpy as np
import pandas as pd
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
from google.cloud import bigquery
client = bigquery.Client()


# In[4]:

def pre_process_table(bq_output_dataset_name,bq_output_table_name,overwrite=False):
    """ Checks if a bq table exists and deletes if necessary.
    
    Args:
        bq_output_dataset_name (string): BQ Dataset name.
        bq_output_table_name (string): BQ table name.
    Returns:
        1
    
    """
    
    dataset_ref = client.dataset(bq_output_dataset_name)
    tables_server = list(client.list_tables(dataset_ref))
    tables_client = list(map(lambda x: x.table_id,tables_server))
    table_exists = bq_output_table_name in tables_client
    if table_exists:
        print("Table {}{} exists".format(bq_output_dataset_name,bq_output_table_name))
        if overwrite:
            table_ref = dataset_ref.table(bq_output_table_name)
            client.delete_table(table_ref)
            print("Overwrite True, deleting table {}{}".format(bq_output_dataset_name,bq_output_table_name))
        else:
            print("Overwrite False, keeping table {}{}".format(bq_output_dataset_name,bq_output_table_name))
    else:
        print("Table {}.{} does not exist".format(bq_output_dataset_name,bq_output_table_name))
    return 1


# In[5]:

pre_process_table(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME,overwrite=True)


# In[6]:

d = {'integer_with_nan' : [21, 45, 45, np.NaN,1],
     'integer_without_nan' : [21, 45, 45, 0,1],
     'float_with_nan' : [20.2, 40.3, np.NaN, 1000,1.0],
     'float_without_nan' : [20.2, 40.3, 66.7, 1000,1.3],
     'string_without_nan' : ["foo","bar","fooz","bars","bar"],
     'string_with_nan' : [np.NaN,"bar","fooz","bars","fooz2"],
     'category': ["cat1","cat1","cat2",np.NaN,"cat1"],
     'year':[1960,1960,1960,1961,2014],
     'month':[1,2,6,1,1]}


# In[7]:

d = {"x": [1,2,3,4,5],
     "y": [1,2,3,np.NaN,6]}


# In[ ]:




# In[8]:

df_raw = pd.DataFrame(d)


# In[9]:

df_raw


# In[10]:

df_raw.to_gbq('{}.{}'.format(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME), BQ_PROJECT_ID)


# In[11]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:09.781982

# In[ ]:



