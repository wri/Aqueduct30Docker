
# coding: utf-8

# In[1]:

""" Calculate intra annual variability intermediate results.
-------------------------------------------------------------------------------
intra annual variability abbreviation is sv or seasonal veriability.

Using total blue water vs using available blue water. 

Aqueduct is a project that manages risk so including consumption will lead to 
a better estimation of the water that is available to you. 

There are multiple ways to calculate intra annual variability.

1. Calculate coefficient of variation for each year (jan-dec) and take the mean
of those values. 
1. Calculate the mean of jan, feb etc. of all years and then calculate the 
coeficeint of variation, similar to Aqueduct 21 and used in Aqueduct 30 for
consistency reasons.

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

sql =  "WITH cte AS ("
sql +=" SELECT"
sql +=  " pfafid_30spfaf06,"
sql +=  " AVG(delta_id) as delta_id,"
sql +=  " month,"
sql +=  " AVG(riverdischarge_m_30spfaf06) AS avg_riverdischarge_m_30spfaf06,"
sql +=  " AVG(riverdischarge_m_delta) AS avg_riverdischarge_m_delta,"
sql +=  " AVG(riverdischarge_m_coalesced) AS avg_riverdischarge_m_coalesced"
sql +=" FROM"
sql +=  " `aqueduct30.{}.{}`".format(BQ_OUTPUT_DATASET_NAME,BQ_INPUT_TABLE_NAME) 
sql +=" WHERE"
sql +=    " temporal_resolution = 'month'"
sql +=" GROUP BY "
sql +=  " pfafid_30spfaf06,"
sql +=  " month"   
sql +=" )"
sql +=" SELECT pfafid_30spfaf06,"
sql +=  " AVG(delta_id) as delta_id,"
sql +=  " AVG(avg_riverdischarge_m_30spfaf06) AS avg_riverdischarge_m_30spfaf06,"
sql +=  " STDDEV(avg_riverdischarge_m_30spfaf06) AS stddev_riverdischarge_m_30spfaf06,"

sql +=  " AVG(avg_riverdischarge_m_delta) AS avg_riverdischarge_m_delta,"
sql +=  " STDDEV(avg_riverdischarge_m_delta) AS stddev_riverdischarge_m_delta,"

sql +=  " AVG(avg_riverdischarge_m_coalesced) AS avg_riverdischarge_m_coalesced,"
sql +=  " STDDEV(avg_riverdischarge_m_coalesced) AS stddev_riverdischarge_m_coalesced"

sql +=" FROM cte"
sql +=" GROUP BY "
sql +=  " pfafid_30spfaf06"
    


# In[7]:


#sql +=  " stddev_riverdischarge_m_30spfaf06 / nullif(stddev_riverdischarge_m_30spfaf06,0) AS cv_riverdischarge_m_30spfaf06"


# In[8]:

sql


# In[9]:

job_config = bigquery.QueryJobConfig()
table_ref = client.dataset(BQ_OUTPUT_DATASET_NAME).table(BQ_OUTPUT_TABLE_NAME)
job_config.destination = table_ref

if TESTING:
    job_config.dry_run = True
    job_config.use_query_cache = False


# In[10]:

query_job = client.query(query=sql,
                         location="US",
                         job_config=job_config)


# In[11]:

query_job.result(timeout=120)


# In[12]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:09.085433
# 
# 

# In[ ]:



