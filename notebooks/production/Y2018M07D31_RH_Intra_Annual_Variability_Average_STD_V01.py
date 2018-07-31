
# coding: utf-8

# In[1]:

""" Calculate intra annual variability intermediate results.
-------------------------------------------------------------------------------

There are multiple ways to calculate intra annual variability.

1. Calculate coefficient of variation for each year (jan-dec) and take the mean
of those values.
1. Calculate the mean of jan, feb etc. of all years and then calculate the 
coeficeint of variation. 



Author: Rutger Hofste
Date: 20180731
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
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

TESTING = 0
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M07D31_RH_Intra_Annual_Variability_Average_STD_V01'
OUTPUT_VERSION = 1


BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"
BQ_INPUT_TABLE_NAME = "y2018m07d30_rh_gcs_to_bq_v01_v02 "
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

print("bq dataset name: ", BQ_OUTPUT_DATASET_NAME,
      "\nBQ_INPUT_TABLE_NAME: ", BQ_INPUT_TABLE_NAME,
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
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google_admin.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
from google.cloud import bigquery
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[6]:

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


# In[7]:

pre_process_table(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME,overwrite=True)


# In[9]:

sql = (
'SELECT'
  ' pfafid_30spfaf06,'
  ' temporal_resolution,'
  ' month,'
  ' year'
' FROM'
  ' `aqueduct30.{}.{}`'.format(BQ_OUTPUT_DATASET_NAME,BQ_INPUT_TABLE_NAME)    
    
)    
    


# In[10]:

sql


# In[ ]:



