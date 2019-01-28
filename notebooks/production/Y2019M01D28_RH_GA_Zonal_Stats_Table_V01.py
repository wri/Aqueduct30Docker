
# coding: utf-8

# In[1]:

"""Post process aggregations from EE and combine with other datasets.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20190128
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0
SCRIPT_NAME = "Y2019M01D28_RH_GA_Zonal_Stats_Table_V01"
OUTPUT_VERSION = 1

GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2019M01D17_RH_GA_Zonal_Stats_Weighted_Indicators_EE_V01/output_V03"

BQ_PROJECT_ID = "aqueduct30"
BQ_DATASET_NAME = "aqueduct30v01"
BQ_INPUT_TABLE_NAME = "y2018m12d04_rh_master_merge_rawdata_gpd_v02_v05"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 

s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("GCS_INPUT_PATH: " + GCS_INPUT_PATH +
      "\nec2_input_path: " +  ec2_input_path + 
      "\nec2_output_path: " + ec2_output_path + 
      "\ns3_output_path: " + s3_output_path  )



# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

get_ipython().system('gsutil cp {GCS_INPUT_PATH}/* {ec2_input_path}')


# In[5]:

GCS_INPUT_PATH


# In[6]:

import os
import numpy as np
import pandas as pd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[7]:

sql = """
SELECT
  indicator,
  AVG(cat) AS cat,
  label
FROM
  `{}.{}.{}`
GROUP BY
  label, indicator
ORDER BY
  indicator, cat
""".format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_INPUT_TABLE_NAME)


# In[8]:

df_labels = pd.read_gbq(query=sql,
                        project_id =BQ_PROJECT_ID,
                        dialect="standard")


# In[9]:

df_labels.head()


# In[10]:

def score_to_category(score):
    if np.isnan(score):
        cat = np.nan
    else:
        if score != 5:
            cat = int(np.floor(score))
        else:
            cat = 4
    return cat


# In[11]:

input_file_name = "weights_sumee_export.csv"
input_file_path = "{}/{}".format(ec2_input_path,input_file_name)
df_weights = pd.read_csv(input_file_path)
df_weights.drop(columns=["system:index",".geo"],
                inplace=True)
df_weights.rename(columns={"sum":"sum_weights"},
                  inplace=True)


# In[12]:

indicators = ["bws","bwd","iav","sev","gtd","drr","rfr","cfr","ucw","cep","udw","usa","rri"]


# In[13]:

df_vertical = pd.DataFrame()


# In[17]:

for indicator in indicators:
    print("indicator: ", indicator)
    input_file_name = "weighted_{}_sumee_export.csv".format(indicator)
    input_file_path = "{}/{}".format(ec2_input_path,input_file_name)
    df = pd.read_csv(input_file_path)
    
    df.drop(columns=["system:index",".geo"],
            inplace=True)
    df.rename(columns={"sum":"sum_weighted_indicator"},inplace=True)
    df["indicator_name"] = indicator
    
    # Join weights and weighted_indicators
    
    df_merged = pd.merge(left=df_weights,
                         right=df,
                         how="inner",
                         left_on="gid_1",
                         right_on="gid_1")
    df_merged["score"]  = df_merged["sum_weighted_indicator"] / df_merged["sum_weights"]
    
    # The cat -> label is different for each indicator. Using a link table instead.
    df_merged["cat"] = df_merged["score"].apply(score_to_category)
    df_vertical = df_vertical.append(df_merged)
    
    


# In[18]:

df_vertical.shape


# In[19]:

df_out = pd.merge(left=df_vertical,
                   right=df_labels,
                   how="left",
                   left_on=["indicator_name","cat"],
                   right_on=["indicator","cat"])


# In[21]:

output_file_path_ec2 = "{}/{}_V{:02.0f}.csv".format(ec2_output_path,SCRIPT_NAME,OUTPUT_VERSION)


# In[22]:

df_out.to_csv(path_or_buf=output_file_path_ec2)


# In[23]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:



