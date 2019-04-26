
# coding: utf-8

# In[1]:

"""Post process aggregations from riverine flood risk.
-------------------------------------------------------------------------------

Riverine flood risk calculated per province and country by research partner.

impacted_pop -> sum_weighted_indicator
total_pop -> sum_weights

Please note there is an inconsistency in GADM. Countries with one province
disappear in level 1. 

Author: Rutger Hofste
Date: 20190411
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = "Y2019M04D11_RH_GA_RFR_Post_Process_V01"
OUTPUT_VERSION = 5

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/finalData/Floods"
INPUT_FILE_NAME_PROVINCE = "flood_State_results.csv"
INPUT_FILE_NAME_COUNTRY = "flood_Country_results.csv"

# Updating labels to new format
labels_dict_country = {
"Low (0 to 2 in 1,000)" : "Low (0 to 2 in 1,000)",
"Low to medium (2 in 1,000 to 4 in 1,000)" : "Low - Medium (2 in 1,000 to 4 in 1,000)",
"Medium to high (4 in 1,000 to 8 in 1,000)" : "Medium - High (4 in 1,000 to 8 in 1,000)",
"High (8 in 1,000 to 1 in 100)" : "High (8 in 1,000 to 1 in 100)",
"Extremely High (more than 1 in 100)":"Extremely High (more than 1 in 100)"
}

labels_dict_province = {
"Low (0 to 1 in 1,000)" : "Low (0 to 1 in 1,000)",
"Low to medium (1 in 1,000 to 3 in 1,000)" : "Low - Medium (1 in 1,000 to 3 in 1,000)" ,
"Medium to high (3 in 1,000 to 7 in 1,000)" : "Medium - High (3 in 1,000 to 7 in 1,000)",
"High (7 in 1,000 to 1 in 100)" : "High (7 in 1,000 to 1 in 100)",
"Extremely High (more than 1 in 100)" : "Extremely High (more than 1 in 100)"
}

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


# In[6]:

sql_labels = """
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

df_labels = pd.read_gbq(query=sql_labels,
                        project_id =BQ_PROJECT_ID,
                        dialect="standard")


# In[8]:

sql_gadm0 = """
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


# In[9]:

sql_gadm1 = """
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


# In[10]:

df_gadm_0 = pd.read_gbq(query=sql_gadm0,
                       project_id =BQ_PROJECT_ID,
                       dialect="standard")


# In[11]:

df_gadm_1 = pd.read_gbq(query=sql_gadm1,
                       project_id =BQ_PROJECT_ID,
                       dialect="standard")


# In[12]:

df_rfr_country = pd.read_csv("{}/{}".format(ec2_input_path,INPUT_FILE_NAME_COUNTRY))


# In[13]:

df_rfr_province = pd.read_csv("{}/{}".format(ec2_input_path,INPUT_FILE_NAME_PROVINCE))


# In[14]:

def score_to_category(score):
    if score != 5:
        cat = int(np.floor(score))
    else:
        cat = 4
    return cat

def process_dataframe(df,geographic_scale):
    """
    Clean dataframes by dropping unnescary columns
    """
    df.drop(columns=["Coast_pop_impacted",
                     "CST_raw",
                     "CST_s",
                     "CST_cat"],inplace=True)
    df_out = df.rename(columns={"PFAF_ID":"pfaf_id",
                        "RVR_raw":"raw",
                        "RVR_s":"score",
                        "RVR_cat":"label",
                        "River_pop_impacted":"sum_weighted_indicator",
                        "pop_total":"sum_weights"})

    df_out["indicator_name"] = "rfr"
    df_out["weight"] = "Pop"
    df_out["cat"] = df_out["score"].apply(score_to_category)
    df_out["score_ranked"] = df_out["score"].rank(ascending=False,method="min")
    
    if geographic_scale == "country":
        df_out["label"] = df_out["label"].apply(lambda x: labels_dict_country[x])
        df_out = pd.merge(left=df_out,
                          right=df_gadm_0,
                          how="left",
                          left_on="gid_0",
                          right_on="gid_0")
        df_out = df_out.reindex(sorted(df_out.columns), axis=1)
        df_out = df_out.set_index("gid_0",drop=False)
        
    elif geographic_scale == "province":
        df_out["label"] = df_out["label"].apply(lambda x: labels_dict_province[x])
        df_out = pd.merge(left=df_out,
                          right=df_gadm_1,
                          how="left",
                          left_on="gid_1",
                          right_on="gid_1")
        df_out = df_out.reindex(sorted(df_out.columns), axis=1)
        df_out = df_out.set_index("gid_1",drop=False)
    
    # Export
    df_out["count_valid"] = np.NaN
    df_out["fraction_valid"] = np.NaN
    
    output_file_path_ec2 = "{}/{}_{}_V{:02.0f}.csv".format(ec2_output_path,SCRIPT_NAME,geographic_scale,OUTPUT_VERSION)
    df_out.to_csv(path_or_buf=output_file_path_ec2,index=True)
    destination_table = "{}.{}_{}_V{:02.0f}".format(BQ_DATASET_NAME,SCRIPT_NAME,geographic_scale,OUTPUT_VERSION).lower()
    df_out.to_gbq(destination_table=destination_table,
                  project_id=BQ_PROJECT_ID,
                  if_exists="replace")
    return df_out
    



# In[15]:

df_out_country = process_dataframe(df_rfr_country,"country")


# In[16]:

df_out_province = process_dataframe(df_rfr_province,"province")


# In[17]:

df_out_country.head()


# In[18]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:   
# 0:00:39.233810  
# 0:00:41.319815
