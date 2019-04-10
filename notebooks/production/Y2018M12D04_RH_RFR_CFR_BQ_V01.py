
# coding: utf-8

# In[1]:

""" Process flood risk data and store on BigQuery. 
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181204
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = "Y2018M12D04_RH_RFR_CFR_BQ_V01"
OUTPUT_VERSION = 3

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/finalData/Floods"
INPUT_FILE_NAME = "flood_results.csv"

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

get_ipython().system("aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive --exclude 'inundationMaps/*'")


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

# RVR -> RFR
# CST -> CFR. 

# raw -> raw.
# s -> score.
# None -> cat.
# cat -> label. 


# In[12]:

df_out = df.rename(columns={"PFAF_ID":"pfaf_id",
                            "RVR_raw":"rfr_raw",
                            "CST_raw":"cfr_raw",
                            "RVR_s":"rfr_score",
                            "CST_s":"cfr_score",
                            "RVR_cat":"rfr_label",
                            "CST_cat":"cfr_label"})


# In[13]:

df_out.drop(columns=["River_pop_impacted","Coast_pop_impacted","pop_total"],inplace=True)


# In[14]:

df_out["cfr_label"].unique()


# In[15]:

def update_labels_rfr(label):
    # update labels to be consistent with rest of framework
    if label == "Low (0 to 1 in 1,000)":
        new_label = "Low (0 to 1 in 1,000)"
    elif label == "Low to medium (1 in 1,000 to 2 in 1,000)":
        new_label = "Low - Medium (1 in 1,000 to 2 in 1,000)"
    elif label == "Medium to high (2 in 1,000 to 6 in 1,000)":
        new_label = "Medium - High (2 in 1,000 to 6 in 1,000)"
    elif label == "High (6 in 1,000 to 1 in 100)":
        new_label = "High (6 in 1,000 to 1 in 100)"
    elif label == "Extremely High (more than 1 in 100)":
        new_label = "Extremely High (more than 1 in 100)"
    else:
        new_label = "error, check script"
    return new_label

def category_from_labels_rfr(label):
    if label == "Low (0 to 1 in 1,000)":
        cat = 0
    elif label == "Low to medium (1 in 1,000 to 2 in 1,000)":
        cat = 1
    elif label == "Medium to high (2 in 1,000 to 6 in 1,000)":
        cat = 2
    elif label == "High (6 in 1,000 to 1 in 100)":
        cat =3
    elif label == "Extremely High (more than 1 in 100)":
        cat = 4
    else:
        cat = -9999
    return cat


def update_labels_cfr(label):
    # update labels to be consistent with rest of framework
    if label == "Low (0 to 9 in 1,000,000)":
        new_label = "Low (0 to 9 in 1,000,000)"
    elif label == "Low to medium (9 in 1,000,000 to 7 in 100,000)":
        new_label = "Low - Medium (9 in 1,000,000 to 7 in 100,000)"
    elif label == "Medium to high (7 in 100,000 to 3 in 10,000)":
        new_label = "Medium - High (7 in 100,000 to 3 in 10,000)"
    elif label == "High (3 in 10,000 to 2 in 1,000)":
        new_label = "High (3 in 10,000 to 2 in 1,000)"
    elif label == "Extremely High (more than 2 in 1,000)":
        new_label = "Extremely High (more than 2 in 1,000)"
    else:
        print(label)
        new_label = "error"
    return new_label

def category_from_labels_cfr(label):
    # update labels to be consistent with rest of framework
    if label == "Low (0 to 9 in 1,000,000)":
        cat = 0
    elif label == "Low to medium (9 in 1,000,000 to 7 in 100,000)":
        cat  = 1
    elif label == "Medium to high (7 in 100,000 to 3 in 10,000)":
        cat = 2
    elif label == "High (3 in 10,000 to 2 in 1,000)":
        cat  = 3
    elif label == "Extremely High (more than 2 in 1,000)":
        cat = 4
    else:
        cat = -9999
    return cat
    


# In[16]:

df_out["rfr_cat"] = df_out["rfr_label"].apply(category_from_labels_rfr)
df_out["rfr_label"] = df_out["rfr_label"].apply(update_labels_rfr)


# In[17]:

df_out["cfr_cat"] = df_out["cfr_label"].apply(category_from_labels_cfr)
df_out["cfr_label"] = df_out["cfr_label"].apply(update_labels_cfr)


# In[18]:

df_out = df_out.reindex(sorted(df_out.columns), axis=1)


# In[19]:

df_out["rfr_label"].unique()


# In[20]:

df_out["cfr_label"].unique()


# In[21]:

df_out["cfr_cat"].unique()


# In[22]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME)


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
# 
