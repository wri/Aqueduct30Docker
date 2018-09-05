
# coding: utf-8

# In[1]:

""" Ingest tabular results to BigQuery.
-------------------------------------------------------------------------------

Groundwater stress, Groundwater table declining trend tabular data will be 
ingested to Google BigQuery.

Author: Rutger Hofste
Date: 20180903
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name
    INPUT_VERSION (integer) : input version, see readme and output number
                              of previous script.
    OUTPUT_VERSION (integer) : output version for ec2 and s3.
    
    
Returns:

Result:
    Table on Google Bigquery.

"""

SCRIPT_NAME = "Y2018M09D03_RH_GWS_Tables_To_BQ_V01"
OUTPUT_VERSION = 1

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

s3_input_path = "s3://wri-projects/Aqueduct30/rawData/Deltares/groundwater/Final_Oct_2017/data/tables"
ec2_input_path = "/volumes/data/{}/input_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("s3_input_path",s3_input_path,
      "\nec2_input_path",ec2_input_path,
      "\nBQ_OUTPUT_DATASET_NAME: ", BQ_OUTPUT_DATASET_NAME,
      "\nBQ_OUTPUT_TABLE_NAME: ",BQ_OUTPUT_TABLE_NAME
      )



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
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[4]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_input_path}')


# In[5]:

get_ipython().system('aws s3 cp {s3_input_path} {ec2_input_path} --recursive')


# In[6]:

input_file_names = ["aquifer_table_sorted.txt",
               "hybas_table_sorted.txt",
               "state_table_sorted.txt"]


# In[7]:

def process_df(df):
    """ Process dataframe to work with bq limitations. 
    
    BadRequest: 400 POST https://www.googleapis.com/bigquery/v2/projects/aqueduct30/datasets/aqueduct30v01/tables: 
    Invalid field name "slope_of_decline_cm.year-1". 
    Fields must contain only letters, numbers, and underscores, 
    start with a letter or underscore, 
    and be at most 128 characters long.
    
    Args:
        df (DataFrame): Dataframe.
        
    Returns:
        df_out (DataFrame) : Dataframe complient with bq.
    
    
    """
    
    df_out = df.rename(columns={"slope_of_decline_cm.year-1":"groundwatertabledecliningtrend_cmperyear",
                                "groundwater_stress":"groundwaterstress_dimensionless"})
    
    return df_out


# In[8]:

for input_file_name in input_file_names:
    input_file_path = os.path.join(ec2_input_path,input_file_name)
    input_file_base_name,input_file_ext  = input_file_name.split(".")
    df = pd.read_csv(input_file_path,delimiter=";")
    
    df_processed = process_df(df)
    
    destination_table = "{}.{}_{}".format(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME,input_file_base_name)
    print(destination_table)
    df_processed.to_gbq(destination_table=destination_table,
          project_id=BQ_PROJECT_ID,
          chunksize=10000,
          if_exists="replace")


# In[9]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:55.283014
