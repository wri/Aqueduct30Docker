
# coding: utf-8

# In[1]:

# ready to run
""" Store Cloudstorage CSV files into bigquery table.
-------------------------------------------------------------------------------

Update 2020 02 13 output 6-7, input 10-12, input coalesce 8-9

Author: Rutger Hofste
Date: 20180712
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04
"""

SCRIPT_NAME = 'Y2018M07D30_RH_GCS_To_BQ_V01'
OUTPUT_VERSION = 8
OVERWRITE_OUTPUT = 1 


GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2018M07D17_RH_RDS_To_S3_V02/output_V14/"

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
INPUT_TABLE_NAME = "y2018m07d30_rh_coalesce_columns_v01_v09" #For header


OUTPUT_DATASET_NAME = "aqueduct30v01"
OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

print("Input gcs: ", GCS_INPUT_PATH,
      "\nOutput bq dataset name: ", OUTPUT_DATASET_NAME,
      "\nOutput bq table name: ", OUTPUT_TABLE_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import os
import pandas as pd
from sqlalchemy import *
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
client = bigquery.Client()


# In[4]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))


# In[5]:

def bq_check_if_exists(dataset_name,table_name):
    dataset_ref = client.dataset(dataset_name)
    tables_server = list(client.list_tables(dataset_ref))
    tables_client = list(map(lambda x: x.table_id,tables_server))
    return table_name in tables_client


# In[6]:

def bq_delete_table(dataset_name,table_name):
    table_ref = client.dataset(dataset_name).table(table_name)
    client.delete_table(table_ref)
    print('Table {}:{} deleted.'.format(dataset_name, table_name))
    return 1


# In[7]:

exists = bq_check_if_exists(OUTPUT_DATASET_NAME,OUTPUT_TABLE_NAME)


# In[8]:

if exists and OVERWRITE_OUTPUT:
    print("table exists, overwriting table")
    bq_delete_table(OUTPUT_DATASET_NAME,OUTPUT_TABLE_NAME)    


# In[9]:

# obtain schema from PostgreSQL
sql =  "SELECT column_name,data_type"
sql += " FROM information_schema.columns"
sql += " where table_name = '{}';".format(INPUT_TABLE_NAME)
print(sql)
df = pd.read_sql(sql,engine)


# In[10]:

df


# In[11]:

def SQL_to_BQ_dict(sql_type):
    """
    SQL to Bigquery type (string)
    """
    
    if sql_type == "bigint" or sql_type == "integer":
        bq_type = "INTEGER"
    elif sql_type == "text":
        bq_type = "STRING"
    elif sql_type == "double precision":
        bq_type = "FLOAT"
    else:
        bq_type = "error!!"
    return bq_type


# In[12]:

schema = []
for index, row in df.iterrows():
    sql_type = row["data_type"]
    bq_type = SQL_to_BQ_dict(sql_type)
    if bq_type == "error!!":
        print(sql_type)
    schema.append(bigquery.SchemaField(row["column_name"], bq_type))


# In[13]:

dataset_ref = client.dataset(OUTPUT_DATASET_NAME)
job_config = bigquery.LoadJobConfig()
#job_config.schema = schema
job_config.write_disposition = "WRITE_APPEND"
job_config.autodetect = True
job_config.skip_leading_rows = 1
# The source format defaults to CSV, so the line below is optional.
job_config.source_format = bigquery.SourceFormat.CSV
#uri = 'gs://aqueduct30_v01/Y2018M07D17_RH_RDS_To_S3_V02/output_V02/*'
uri = '{}*'.format(GCS_INPUT_PATH)


# In[14]:

load_job = client.load_table_from_uri(source_uris = uri,
                                      destination = dataset_ref.table(OUTPUT_TABLE_NAME),
                                      job_config=job_config) 
print('Starting job {}'.format(load_job.job_id))
load_job.result()
print('Job finished.')


# In[15]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:03:08.852822  
# 0:02:24.556886  
# 0:02:31.501342  
# 0:02:21.296729  
# 0:02:37.833459
# 

# In[ ]:



