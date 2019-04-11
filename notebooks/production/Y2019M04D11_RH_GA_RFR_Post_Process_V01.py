
# coding: utf-8

# In[1]:

"""Post process aggregations from riverine flood risk.
-------------------------------------------------------------------------------

Riverine flood risk calculated per province by research partner.

Author: Rutger Hofste
Date: 20190411
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = "Y2019M04D11_RH_GA_RFR_Post_Process_V01"
OUTPUT_VERSION = 1

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/finalData/Floods"
INPUT_FILE_NAME = "flood_State_results.csv"

BQ_PROJECT_ID = "aqueduct30"
BQ_DATASET_NAME = "aqueduct30v01"
BQ_INPUT_TABLE_NAME_LABEL = "y2018m12d04_rh_master_merge_rawdata_gpd_v02_v09"
BQ_INPUT_TABLE_NAME_GADM  = "y2018m11d12_rh_gadm36_level1_rds_to_bq_v01_v01"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()


ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 

s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("S3_INPUT_PATH: ",S3_INPUT_PATH,
      "\nec2_input_path: ",ec2_input_path,
      "\nec2_output_path: ",ec2_output_path,
      "\ns3_output_path: " + s3_output_path  )


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
import numpy as np
import pandas as pd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# ## Labels

# In[6]:

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
""".format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_INPUT_TABLE_NAME_LABEL)


# In[7]:

df_gadm_1 = pd.read_gbq(query=sql,
                        project_id =BQ_PROJECT_ID,
                        dialect="standard")


# ## GADM Level 1 names

# In[8]:

sql = """
SELECT
  gid_1,
  gid_0,
  name_1,
  name_0
FROM
  `{}.{}.{}`
ORDER BY
  gid_1
""".format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_INPUT_TABLE_NAME_GADM)


# In[9]:

df_gadm_1 = pd.read_gbq(query=sql,
                        project_id =BQ_PROJECT_ID,
                        dialect="standard")


# ## GADM Level 0 names

# In[10]:

sql = """
SELECT
  name_0,
  ANY_VALUE(gid_0) as gid_0
FROM
  `{}.{}.{}`
GROUP BY
  name_0
ORDER BY
  name_0
""".format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_INPUT_TABLE_NAME_GADM)


# In[11]:

df_gadm_0 = pd.read_gbq(query=sql,
                       project_id =BQ_PROJECT_ID,
                       dialect="standard")


# Process rfr data, similar to Y2018M12D04_RH_RFR_CFR_BQ_V01

# In[12]:

files = os.listdir(ec2_input_path)


# In[13]:

input_path = "{}/{}".format(ec2_input_path,INPUT_FILE_NAME)


# In[42]:

df = pd.read_csv(input_path)


# In[43]:

df_out = df.rename(columns={"PFAF_ID":"pfaf_id",
                            "RVR_raw":"rfr_raw",
                            "CST_raw":"cfr_raw",
                            "RVR_s":"rfr_score",
                            "CST_s":"cfr_score",
                            "RVR_cat":"rfr_label",
                            "CST_cat":"cfr_label"})


# In[44]:

df_out


# In[45]:

df_out.drop(columns=["Coast_pop_impacted","cfr_raw","cfr_score","cfr_label","rfr_raw"],inplace=True)


# In[46]:

df_out["rfr_label"].unique()


# In[47]:

def update_labels_rfr(label):
    # update labels to be consistent with rest of framework
    if label == "Low (0 to 1 in 1,000)":
        new_label = "Low (0 to 1 in 1,000)"
    elif label == "Low to medium (1 in 1,000 to 3 in 1,000)":
        new_label = "Low - Medium (1 in 1,000 to 3 in 1,000)"
    elif label == "Medium to high (3 in 1,000 to 7 in 1,000)":
        new_label = "Medium - High (3 in 1,000 to 7 in 1,000)"
    elif label == "High (7 in 1,000 to 1 in 100)":
        new_label = "High (7 in 1,000 to 1 in 100)"
    elif label == "Extremely High (more than 1 in 100)":
        new_label = "Extremely High (more than 1 in 100)"
    else:
        new_label = "error, check script"
    return new_label

def category_from_labels_rfr(label):
    if label == "Low (0 to 1 in 1,000)":
        cat = 0
    elif label == "Low to medium (1 in 1,000 to 3 in 1,000)":
        cat = 1
    elif label == "Medium to high (3 in 1,000 to 7 in 1,000)":
        cat = 2
    elif label == "High (7 in 1,000 to 1 in 100)":
        cat =3
    elif label == "Extremely High (more than 1 in 100)":
        cat = 4
    else:
        cat = -9999
    return cat


# In[48]:

df_out["rfr_cat"] = df_out["rfr_label"].apply(category_from_labels_rfr)
df_out["rfr_label"] = df_out["rfr_label"].apply(update_labels_rfr)


# In[49]:

df_out["rfr_label"].unique()


# In[50]:

df_out["rfr_cat"].unique()


# In[51]:

df_out["indicator_name"] = "rfr"
df_out["weight"] = "pop"


# In[52]:

df_out.head()


# In[56]:

df_out2 = df_out.rename(columns={"River_pop_impacted":"weight",
                                 "pop_total":"sum_weights"})


# In[58]:

df_out2.head()


# In[59]:

df_gid_1 = pd.merge(left=df_out2,
                    right=df_gadm_1,
                    how="left",
                    left_on="gid_1",
                    right_on="gid_1")


# In[60]:

df_gid_1.head()


# In[61]:

def province_to_country(df):
    """ Convert province level dataframe to country level
    DataFrame
    
    
    """   
    df["gid_0"] = df["gid_1"].apply(lambda x:  x.split(".")[0])
    
    grouped = df.groupby('gid_0')
    df_country = df.groupby(by="gid_0",as_index=False).sum()
    return df_country


# In[62]:

df_gid_0 = province_to_country(df_gid_1)


# In[63]:

df_gid_0


# Use population to weight states to get to country`

# In[ ]:



