
# coding: utf-8

# In[1]:

""" Categorize and label inter annual variability.
-------------------------------------------------------------------------------

IAV value to score calculation

Author: Rutger Hofste
Date: 20180801
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
SCRIPT_NAME = 'Y2018M07D31_RH_Inter_AV_Cat_label_V01'
OUTPUT_VERSION = 1

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"
BQ_INPUT_TABLE_NAME = "y2018m07d31_rh_inter_annual_variability_coef_var_v01_v01"
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
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
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

pre_process_table(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME,OVERWRITE_OUTPUT)


# In[6]:

sql  = "WITH cte AS ("
sql +=" SELECT"
sql +=  " pfafid_30spfaf06,"
sql +=  " temporal_resolution,"
sql +=  " month,"
sql +=  " year,"
sql +=  " iav_riverdischarge_m_30spfaf06,"
sql +=  " iav_riverdischarge_score_30spfaf06,"
sql +=  " CASE"
sql +=  " WHEN iav_riverdischarge_score_30spfaf06 = -1"
sql +=    " THEN -1 "
sql +=  " WHEN iav_riverdischarge_score_30spfaf06 < 5 AND iav_riverdischarge_score_30spfaf06 >= 0"
sql +=    " THEN FLOOR(iav_riverdischarge_score_30spfaf06)"
sql +=  " WHEN iav_riverdischarge_score_30spfaf06 = 5"
sql +=    " THEN 4"
sql +=  " ELSE -9999"
sql +=  " END"
sql +=  " AS iav_riverdischarge_category_30spfaf06"
sql +=" FROM"
sql +=  " `{}.{}`".format(BQ_OUTPUT_DATASET_NAME,BQ_INPUT_TABLE_NAME)
sql += " )"
sql +=" SELECT"
sql +=  " *,"
sql +=     " CASE"
sql +=     " WHEN iav_riverdischarge_category_30spfaf06 = -9999"
sql +=         " THEN 'NoData' "
sql +=     " WHEN iav_riverdischarge_category_30spfaf06 = -1"
sql +=         " THEN 'Arid and Low Water Use' "
sql +=     " WHEN iav_riverdischarge_category_30spfaf06 = 0"
sql +=         " THEN 'Low' "
sql +=     " WHEN iav_riverdischarge_category_30spfaf06 = 1"
sql +=         " THEN 'Low - Medium' "
sql +=     " WHEN iav_riverdischarge_category_30spfaf06 = 2"
sql +=         " THEN 'Medium - High' "
sql +=     " WHEN iav_riverdischarge_category_30spfaf06 = 3"
sql +=         " THEN 'High' "
sql +=     " WHEN iav_riverdischarge_category_30spfaf06 = 4"
sql +=         " THEN 'Extremely High' "
sql +=     " ELSE 'error, check score'"
sql +=     " END AS iav_riverdischarge_label_30spfaf06"
sql +=" FROM"
sql +=" cte"



# In[7]:

sql


# In[8]:

job_config = bigquery.QueryJobConfig()
table_ref = client.dataset(BQ_OUTPUT_DATASET_NAME).table(BQ_OUTPUT_TABLE_NAME)
job_config.destination = table_ref

if TESTING:
    job_config.dry_run = True
    job_config.use_query_cache = False


# In[9]:

query_job = client.query(query=sql,
                         location="US",
                         job_config=job_config)


# In[10]:

query_job.result(timeout=120)


# In[11]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:39.601059  
# 0:00:44.055266
# 

# In[ ]:



