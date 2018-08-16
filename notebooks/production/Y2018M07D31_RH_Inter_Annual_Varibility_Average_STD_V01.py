
# coding: utf-8

# In[1]:

""" Calculate inter annual variability. 
-------------------------------------------------------------------------------

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
SCRIPT_NAME = 'Y2018M07D31_RH_Inter_Annual_Varibility_Average_STD_V01'
OUTPUT_VERSION = 2


BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"
BQ_INPUT_TABLE_NAME = "y2018m07d30_rh_gcs_to_bq_v01_v04"
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
        print("Table {}{} does not exist".format(bq_output_dataset_name,bq_output_table_name))
    return 1


# In[5]:

pre_process_table(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME,overwrite=True)


# In[6]:

sql = (
'SELECT'
  ' pfafid_30spfaf06,'
  ' temporal_resolution,'
  ' month,'
  ' year,'
  ' delta_id,'
  ' STDDEV(riverdischarge_m_30spfaf06) OVER(PARTITION BY pfafid_30spfaf06, temporal_resolution, month ORDER BY year) AS stddev_riverdischarge_m_30spfaf06,'
  ' AVG(riverdischarge_m_30spfaf06) OVER(PARTITION BY pfafid_30spfaf06, temporal_resolution, month ORDER BY year) AS avg_riverdischarge_m_30spfaf06,'
  ' STDDEV(riverdischarge_m_delta) OVER(PARTITION BY pfafid_30spfaf06, temporal_resolution, month ORDER BY year) AS stddev_riverdischarge_m_delta,'
  ' AVG(riverdischarge_m_delta) OVER(PARTITION BY pfafid_30spfaf06, temporal_resolution, month ORDER BY year) AS avg_riverdischarge_m_delta,'
  ' STDDEV(riverdischarge_m_coalesced) OVER(PARTITION BY pfafid_30spfaf06, temporal_resolution, month ORDER BY year) AS stddev_riverdischarge_m_coalesced,'
  ' AVG(riverdischarge_m_coalesced) OVER(PARTITION BY pfafid_30spfaf06, temporal_resolution, month ORDER BY year) AS avg_riverdischarge_m_coalesced'
' FROM'
  ' `aqueduct30.{}.{}`'.format(BQ_OUTPUT_DATASET_NAME,BQ_INPUT_TABLE_NAME)
)


# In[7]:

sql


# In[8]:

job_config = bigquery.QueryJobConfig()
table_ref = client.dataset(BQ_OUTPUT_DATASET_NAME).table(BQ_OUTPUT_TABLE_NAME)
job_config.destination = table_ref
#job_config.dry_run = True
#job_config.use_query_cache = False


# In[9]:

query_job = client.query(query=sql,
                         location="US",
                         job_config=job_config)


# In[10]:

query_job.state


# In[11]:

query_job.total_bytes_processed


# In[12]:

query_job.result(timeout=120)


# In[13]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:28.658684
# 
