
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
OUTPUT_VERSION = 1

NODATA_VALUE = -9999

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/finalData/AccessToSanitation"
INPUT_FILE_NAME = "sn_results.csv"

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

df_out.drop(columns=["SN_nat_raw",
                     "SN_rur_raw",
                     "SN_urb_raw",
                     "rur_pop",
                     "urb_pop",
                     "total_pop"],inplace=True)


# In[14]:

df_out["usa_raw"] = df_out["usa_raw"].fillna(NODATA_VALUE)
df_out["usa_score"] = df_out["usa_score"].fillna(NODATA_VALUE)
df_out["usa_label"] = df_out["usa_label"].fillna("No Data")


# In[15]:

def score_to_category(score):
    if score != 5:
        cat = int(np.floor(score))
    else:
        cat = 4
    return cat


# In[16]:

df_out["usa_cat"] = df_out["usa_score"].apply(score_to_category)


# In[17]:

df_out = df_out.reindex(sorted(df_out.columns), axis=1)


# In[18]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME)


# In[19]:

df_out.to_gbq(destination_table=destination_table,
          project_id=BQ_PROJECT_ID,
          chunksize=10000,
          if_exists="replace")


# In[20]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:22.983392
# 
# 

# In[ ]:



