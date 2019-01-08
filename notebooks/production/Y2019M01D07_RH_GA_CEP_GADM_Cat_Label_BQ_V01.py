
# coding: utf-8

# In[1]:

""" Cleanup, add category and label for icep at gadm level 1.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20190107
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_
    NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.
    DATABASE_ENDPOINT (string) : RDS or postGreSQL endpoint.
    DATABASE_NAME (string) : Database name.
    TABLE_NAME_AREA_30SPFAF06 (string) : Table name used for areas. Must exist
        on same database as used in rest of script.
    S3_INPUT_PATH_RIVERDISCHARGE (string) : AWS S3 input path for 
        riverdischarge.    
    S3_INPUT_PATH_DEMAND (string) : AWS S3 input path for 
        demand.     

"""

TESTING = 0
SCRIPT_NAME = "Y2019M01D07_RH_GA_CEP_GADM_Cat_Label_BQ_V01"
OUTPUT_VERSION = 1

COUNT_THRESHOLD = 1000 #(icepbasin cellsize 60km )

NODATA_VALUE = -9999

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2019M01D07_RH_GA_CEP_Zonal_Stats_GADM_EE_V01/output_V01/"

BQ_PROJECT_ID = "aqueduct30"
BQ_INPUT_LINK_TABLE_NAME = "y2018m11d12_rh_gadm36_level1_to_rds_v01_v04"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 

s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("S3_INPUT_PATH: ",S3_INPUT_PATH,
      "\nec2_input_path: ",ec2_input_path,
      "\nBQ_OUTPUT_DATASET_NAME: ", BQ_OUTPUT_DATASET_NAME,
      "\nBQ_OUTPUT_TABLE_NAME: ",BQ_OUTPUT_TABLE_NAME,
      "\ns3_output_path:",s3_output_path
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

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive')


# In[5]:

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[6]:

def normalize_score(row):
    if row <= -5:
        minV, maxV, addV = icep_min, -5, 0
    elif row <= 0:
        minV, maxV, addV = -5, -1, 0
    elif row <= 1:
        minV, maxV, addV = 0, 1, 2
    elif row <= 5:
        minV, maxV, addV = 1, 5, 3
    else:
        minV, maxV, addV = 5, icep_max, 4

    # Normalize score base on class bounds
    score = (row - minV) / (maxV - minV) + addV
    # Fix scores less than 0 or great than 5
    final_score = np.where(score < 0, 0, np.where(score > 5, 5, score))
    return final_score

def category_to_label(row):
    if row < -9998:
        cat = "No Data"
    elif row < 1:
        cat = "Low (< -5)"
    elif row < 2:
        cat = "Low to medium (-5 to 0)"
    elif row < 3:
        cat = "Medium to high (0 to +1)"
    elif row < 4:
        cat = "High (+1 to +5)"
    elif row <= 5:
        cat = "Extremely High (> +5)"
    else:
        cat = "Error"
    return cat

def label_to_category(row):
    if row == "Low (< -5)":
        cat = 0
    elif row == "Low to medium (-5 to 0)":
        cat = 1
    elif row == "Medium to high (0 to +1)":
        cat = 2
    elif row == "High (+1 to +5)":
        cat = 3
    elif row == "Extremely High (> +5)":
        cat = 4
    else:
        cat = -9999
    return cat


# In[7]:

files = os.listdir(ec2_input_path)


# In[8]:

files


# In[9]:

input_file_path = "{}/df_gadm36_l1_30s.pkl".format(ec2_input_path)


# In[10]:

df = pd.read_pickle(input_file_path)


# In[11]:

df.head()


# In[12]:

df.zones = df.zones.astype(np.int64)


# In[13]:

df = df.rename(columns={"mean":"cep_raw",
                        "zones":"gid_1_id"})


# In[14]:

sql = """
SELECT
  gid_1_id,
  gid_1
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_INPUT_LINK_TABLE_NAME)


# In[15]:

df_link = pd.read_gbq(query=sql,
                      dialect="standard")


# In[16]:

df_link.head()


# In[17]:

df.head()


# In[18]:

df_link.shape


# In[19]:

df.shape


# In[20]:

df_merged = pd.merge(left=df,
                     right=df_link,
                     how="left",
                     left_on="gid_1_id",
                     right_on="gid_1_id")


# In[21]:

df_merged.set_index(keys=["gid_1"],
                    drop = False,
                    inplace = True)


# In[22]:

df_merged.head()


# In[23]:

icep_min = df["cep_raw"].min()
icep_max = df["cep_raw"].max()


# In[24]:

icep_min


# In[25]:

icep_max


# In[26]:

df_merged["cep_raw"] = df_merged["cep_raw"].fillna(-9999.0)


# In[27]:

df_merged["cep_score"] = df_merged["cep_raw"].apply(lambda x: normalize_score(x))


# In[28]:

# Replace nodata scores with NoData value
df_merged["cep_score"][df_merged["cep_raw"] <-9998 ] = NODATA_VALUE


# In[29]:

df_merged["cep_label"] = df_merged["cep_score"].apply(lambda x: category_to_label(x))


# In[30]:

df_merged["cep_cat"] = df_merged["cep_label"].apply(lambda x: label_to_category(x))


# In[31]:

df_merged = df_merged.drop(columns=["output_version","reducer","script_used","spatial_aggregation","spatial_resolution","unit","parameter"])


# In[32]:

df_merged.columns = df_merged.columns.str.lower()


# In[33]:

df_merged.head()


# In[34]:

df_merged.sort_index(axis=1,
                     inplace=True)


# In[35]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME)


# In[36]:

df_merged.to_gbq(destination_table=destination_table,
                 project_id=BQ_PROJECT_ID,
                 chunksize=10000,
                 if_exists="replace")


# In[37]:

output_file_path_pkl = "{}/cep_cat_label.pkl".format(ec2_output_path)
output_file_path_csv = "{}/cep_cat_label.csv".format(ec2_output_path)
df_merged.to_pickle(output_file_path_pkl)
df_merged.to_csv(output_file_path_csv,encoding='utf-8')


# In[38]:

get_ipython().system('aws s3 cp  {ec2_output_path} {s3_output_path} --recursive')


# In[39]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:19.839925
# 
