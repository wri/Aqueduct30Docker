
# coding: utf-8

# In[2]:

""" Water Stress in delta basins vs individual basins.
-------------------------------------------------------------------------------



"""

TESTING = 0
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M08D02_RH_QA_Inspect_Delta_Water_Stress_V01'
OUTPUT_VERSION = 1

BQ_PROJECT_ID = "aqueduct30"
BQ_INPUT_DATASET_NAME = "aqueduct30v01"
BQ_OUTPUT_DATASET_NAME = "Y2018M08D03_RH_QA_Data_Retrospective_V01"
BQ_INPUT_TABLE_NAME = "y2018m08d01_rh_intra_annual_variability_coef_var_v01_v01"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"
RDS_INPUT_TABLE_NAME = "hybas06_v04"


print("\nBQ_INPUT_DATASET_NAME: ", BQ_INPUT_DATASET_NAME,
      "\nBQ_OUTPUT_DATASET_NAME: ", BQ_OUTPUT_DATASET_NAME,
      "\nBQ_INPUT_TABLE_NAME: ", BQ_INPUT_TABLE_NAME,
      "\nBQ_OUTPUT_TABLE_NAME: ", BQ_OUTPUT_TABLE_NAME)


# In[3]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[24]:

import os
import sqlalchemy
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery
get_ipython().magic('load_ext google.cloud.bigquery')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[25]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))


# In[5]:

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


# In[7]:

dataset_ref = client.dataset(BQ_OUTPUT_DATASET_NAME)
dataset = bigquery.Dataset(dataset_ref)
dataset.location = 'US'
dataset = client.create_dataset(dataset) 


# In[8]:

pre_process_table(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME,OVERWRITE_OUTPUT)


# In[20]:

sql = """
SELECT
  delta_id,
  pfafid_30spfaf06,
  temporal_resolution,
  year,
  month,
  waterstress_label_dimensionless_coalesced,
  waterstress_category_dimensionless_coalesced,
  waterstress_score_dimensionless_coalesced,
  waterstress_raw_dimensionless_coalesced,
  waterstress_label_dimensionless_delta,
  waterstress_category_dimensionless_delta,
  waterstress_score_dimensionless_delta,
  waterstress_raw_dimensionless_delta,
  waterstress_label_dimensionless_30spfaf06,
  waterstress_category_dimensionless_30spfaf06,
  waterstress_score_dimensionless_30spfaf06,
  waterstress_raw_dimensionless_30spfaf06,
  avg1y_ols_ols10_weighted_waterstress_dimensionless_30spfaf06,
  avg1y_ols_ols10_waterstress_dimensionless_30spfaf06
FROM
  `aqueduct30.aqueduct30v01.y2018m07d30_rh_gcs_to_bq_v01_v02`
WHERE
  delta_id >=0 AND year = 2014
ORDER BY 
  pfafid_30spfaf06,
  temporal_resolution,
  year,
  month
"""


# In[21]:

df = pd.read_gbq(query=sql,dialect="standard")


# In[22]:

df.shape


# In[23]:

df.head()


# In[ ]:

sql = """
SELECT
  pfaf_id,
  geom
FROM
  {}
""".format(RDS_INPUT_TABLE_NAME)


# In[ ]:

gdf = gpd.read_postgis(sql=sql,
                       con=engine)

