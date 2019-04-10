
# coding: utf-8

# In[1]:

""" Process flood risk data and store on BigQuery. 
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181204
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = "Y2018M12D04_RH_UCW_BQ_V01"
OUTPUT_VERSION = 4

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/finalData/Wastewater"
INPUT_FILE_NAME = "wastewater_results.csv"

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

# WW -> UCW


# raw -> raw.
# s -> score.
# None -> cat.
# cat -> label. 


# In[12]:

df_out = df.rename(columns={"ADM0_A3":"adm0_a3",
                            "WW_raw":"ucw_raw",
                            "WW_s":"ucw_score",
                            "WW_cat":"ucw_label"})


# In[13]:

df_out.drop(columns=["Exclude",
                     "Percent_Connected",
                     "Untreated",
                     "Primary",
                     "Secondary",
                     "Tertiary"],inplace=True)


# In[14]:

def update_labels(label):
    # Update label to correct format.
    if label == "Low to no wastewater collected":
        new_label = "No to Low Wastewater Collected"
    elif label == "Low (< 30%)":
        new_label = "Low (<30%)"
    elif label == "Low to medium (0.3 to 60%)":
        new_label = "Low - Medium (30-60%)"
    elif label == "Medium to high (0.6 to 90%)":
        new_label = "Medium - High (60-90%)"
    elif  label == "High (0.9 to 100%)":
        new_label = "High (90-100%)"
    elif label == "Extremely High (100%)":
        new_label =  "Extremely High (100%)"
    else:
        new_label = "error, check script"
    return new_label

def category_from_label(label):
    # get cat from label
    if label == "Low to no wastewater collected":
        cat = -1
    elif label == "Low (< 30%)":
        cat = 0
    elif label == "Low to medium (0.3 to 60%)":
        cat = 1
    elif label == "Medium to high (0.6 to 90%)":
        cat = 2
    elif  label == "High (0.9 to 100%)":
        cat = 3
    elif label == "Extremely High (100%)":
        cat = 4
    else:
        cat = -9999
    return cat
    



# In[15]:

df_out["ucw_cat"] = df_out["ucw_label"].apply(category_from_label)


# In[16]:

df_out["ucw_label"] = df_out["ucw_label"].apply(update_labels)


# In[17]:

df_out = df_out.reindex(sorted(df_out.columns), axis=1)


# In[18]:

df_out.head()


# In[19]:

df_out.loc[(df_out['ucw_raw'] == -1.0) ,'ucw_cat'] = -1


# In[20]:

df_out.head()


# In[21]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME)


# In[22]:

destination_table


# In[23]:

df_out.to_gbq(destination_table=destination_table,
          project_id=BQ_PROJECT_ID,
          chunksize=10000,
          if_exists="replace")


# In[24]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:18.766466
