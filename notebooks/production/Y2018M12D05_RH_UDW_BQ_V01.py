
# coding: utf-8

# In[1]:

""" Process unimproved/no drinking water and store on BigQuery.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181205
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = "Y2018M12D05_RH_UDW_BQ_V01"
OUTPUT_VERSION = 3

NODATA_VALUE = -9999

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/finalData/AccessToDW"
INPUT_FILE_NAME = "dw_results_basin_v2.csv"

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 

print("S3_INPUT_PATH: ",S3_INPUT_PATH,
      "\nec2_input_path: ",ec2_input_path,
      "\nec2_output_path: ",ec2_output_path,
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

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive ')


# In[5]:

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[6]:

files = os.listdir(ec2_input_path)


# In[7]:

input_path = "{}/{}".format(ec2_input_path,INPUT_FILE_NAME)


# In[8]:

df = pd.read_csv(input_path)


# In[9]:

df.dtypes


# In[10]:

df.head()


# In[11]:

df["DW_cat"].unique()


# In[12]:

# DW -> UDW


# raw -> raw.
# s -> score.
# None -> cat.
# cat -> label. 


# In[13]:

df_out = df.rename(columns={"PFAF_ID":"pfaf_id",
                            "DW_raw":"udw_raw",
                            "DW_s":"udw_score",
                            "DW_cat":"udw_label"})


# In[14]:

"""
df_out.drop(columns=["DW_nat_raw",
                     "DW_rur_raw",
                     "DW_urb_raw",
                     "rur_pop",
                     "urb_pop",
                     "total_pop"],inplace=True)
"""


# In[15]:

df_out["udw_raw"] = df_out["udw_raw"].fillna(NODATA_VALUE)
df_out["udw_score"] = df_out["udw_score"].fillna(NODATA_VALUE)
df_out["udw_label"] = df_out["udw_label"].fillna("No Data")


# In[16]:

def update_labels(label):
    # update labels to be consistent with rest of framework
    if label == "Low (>2.5%)":
        new_label = "Low (<2.5%)"
    elif label == "Low to medium (2.5-5%)":
        new_label = "Low - Medium (2.5-5%)"
    elif label == "Medium to high (5-10%)":
        new_label = "Medium - High (5-10%)"
    elif label == "High (10-20%)":
        new_label = "High (10-20%)"
    elif label == "Extremely High (>20%)":
        new_label = "Extremely High (>20%)"
    elif label == "No Data":
        new_label = "No Data"
    else:
        print(label)
    return new_label
    
def label_to_category(row):
    if row == "Low (>2.5%)":
        cat = 0
    elif row == "Low to medium (2.5-5%)":
        cat = 1
    elif row == "Medium to high (5-10%)":
        cat = 2
    elif row == "High (10-20%)":
        cat = 3
    elif row == "Extremely High (>20%)":
        cat = 4
    else:
        cat = -9999
    return cat
    


# In[17]:

df_out["udw_cat"] = df_out["udw_label"].apply(label_to_category)


# In[18]:

df_out["udw_label"] = df_out["udw_label"].apply(update_labels)


# In[19]:

df_out = df_out.reindex(sorted(df_out.columns), axis=1)


# In[20]:

df_out


# In[21]:

df_out["udw_label"].unique()


# In[22]:

df_out["udw_cat"].unique()


# In[23]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME)


# In[24]:

df_out.to_gbq(destination_table=destination_table,
          project_id=BQ_PROJECT_ID,
          chunksize=10000,
          if_exists="replace")


# In[25]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:18.766466
