
# coding: utf-8

# In[1]:

""" Process unimproved/no sanitation and store on BigQuery.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181205
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = "Y2018M12D05_RH_USA_BQ_V01"
OUTPUT_VERSION = 4

NODATA_VALUE = -9999

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/finalData/AccessToSanitation"
INPUT_FILE_NAME = "sn_results_basin_v2.csv"

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

# SN -> USA


# raw -> raw.
# s -> score.
# None -> cat.
# cat -> label. 


# In[12]:

df_out = df.rename(columns={"PFAF_ID":"pfaf_id",
                            "SN_raw":"usa_raw",
                            "SN_s":"usa_score",
                            "SN_cat":"usa_label"})


# In[13]:

"""
df_out.drop(columns=["SN_nat_raw",
                     "SN_rur_raw",
                     "SN_urb_raw",
                     "rur_pop",
                     "urb_pop",
                     "total_pop"],inplace=True)
"""


# In[14]:

df_out["usa_raw"] = df_out["usa_raw"].fillna(NODATA_VALUE)
df_out["usa_score"] = df_out["usa_score"].fillna(NODATA_VALUE)
df_out["usa_label"] = df_out["usa_label"].fillna("No Data")


# In[15]:

df_out["usa_label"].unique()


# In[16]:

def update_labels_usa(label):
    # update labels to be consistent with rest of framework
    if label == "Low (<2.5%)":
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
        new_label = "error, check script"
    return new_label

def category_from_labels_usa(label):
    if label == "Low (<2.5%)":
        cat = 0
    elif label == "Low to medium (2.5-5%)":
        cat = 1
    elif label == "Medium to high (5-10%)":
        cat = 2
    elif label == "High (10-20%)":
        cat =3
    elif label == "Extremely High (>20%)":
        cat = 4
    else:
        cat = -9999
    return cat


# In[17]:

df_out["usa_cat"] = df_out["usa_label"].apply(category_from_labels_usa)
df_out["usa_label"] = df_out["usa_label"].apply(update_labels_usa)


# In[18]:

df_out["usa_cat"].unique() 


# In[19]:

df_out["usa_label"].unique()


# In[20]:

df_out = df_out.reindex(sorted(df_out.columns), axis=1)


# In[21]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME)


# In[22]:

df_out.to_gbq(destination_table=destination_table,
          project_id=BQ_PROJECT_ID,
          chunksize=10000,
          if_exists="replace")


# In[23]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:22.983392
# 
# 

# In[ ]:



