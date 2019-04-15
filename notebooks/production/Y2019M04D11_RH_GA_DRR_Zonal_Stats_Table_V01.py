
# coding: utf-8

# In[1]:

"""Post process aggregations from drought risk EE.
-------------------------------------------------------------------------------

Store drought risk result in tabular format, compatible with other baseline 
water stress, baseline water depletion, interannual variability, seasonal 
variobility. 

indicator weights 
drr withdrawal per sector

Author: Rutger Hofste
Date: 20190411
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0
SCRIPT_NAME = "Y2019M04D11_RH_GA_DRR_Zonal_Stats_Table_V01"
OUTPUT_VERSION = 3

GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2019M01D29_RH_GA_DR_Zonal_Stats_GADM_EE_V01/output_V03"

BQ_PROJECT_ID = "aqueduct30"
BQ_DATASET_NAME = "aqueduct30v01"
BQ_INPUT_TABLE_NAME_LABEL = "y2018m12d04_rh_master_merge_rawdata_gpd_v02_v09"
BQ_INPUT_TABLE_NAME_GADM  = "y2018m11d12_rh_gadm36_level1_rds_to_bq_v01_v01"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()


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

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

get_ipython().system('gsutil -m cp {GCS_INPUT_PATH}/* {ec2_input_path}')


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

df_labels = pd.read_gbq(query=sql,
                        project_id =BQ_PROJECT_ID,
                        dialect="standard")


# In[8]:

df_labels.head()


# ## GADM Level 1 names

# In[9]:

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


# In[10]:

df_gadm_1 = pd.read_gbq(query=sql,
                        project_id =BQ_PROJECT_ID,
                        dialect="standard")


# ## GADM Level 0 names

# In[11]:

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


# In[12]:

df_gadm_0 = pd.read_gbq(query=sql,
                       project_id =BQ_PROJECT_ID,
                       dialect="standard")


# ## DRR

# In[13]:

def score_to_category(score):
    if np.isnan(score):
        cat = np.nan
    else:
        if score < 5:
            cat = int(np.floor(score))
        else:
            cat = 4
    return cat


# In[14]:

def get_weights_df(sector):
    """ Get Dataframe Per sector
    
    """
    input_file_name = "{}_weights_sumee_export.csv".format(sector)
    input_file_path = "{}/{}".format(ec2_input_path,input_file_name)
    df_weights = pd.read_csv(input_file_path)
    df_weights.drop(columns=["system:index",".geo"],
                    inplace=True)
    df_weights.rename(columns={"sum":"sum_weights"},
                      inplace=True)
    return df_weights

def get_weighted_indicator_df(indicator):
    """ Get DataFrame per indicator

    """
    input_file_name = "{}_weighted_{}_sumee_export.csv".format(sector,indicator)
    input_file_path = "{}/{}".format(ec2_input_path,input_file_name)
    df = pd.read_csv(input_file_path)
    df.drop(columns=["system:index",".geo"],
            inplace=True)
    df.rename(columns={"sum":"sum_weighted_indicator"},inplace=True)
    df["indicator_name"] = indicator
    df["weight"] = sector
    return df
    

def province_to_country(df,sector,indicator):
    """ Convert province level dataframe to country level
    DataFrame
    
    
    """   
    df["gid_0"] = df["gid_1"].apply(lambda x:  x.split(".")[0])
    
    grouped = df.groupby('gid_0')
    df_country = df.groupby(by="gid_0",as_index=False).sum()
    df_country["indicator_name"] = indicator
    df_country["weight"] = sector
    return df_country

def process_df(df):
    """ Calculate Score, add cat and label. 
    
    Due to the zonal statistics in Google Earth Engine, 
    some semi masked cells produce score higher than 5. 
    Clipping all scores above 5 to 5. 
    
    Sorts columns alphabetically.
    
    Ranks based on score. Uses minimum rank:
    http://www.datasciencemadesimple.com/rank-dataframe-python-pandas-min-max-dense-rank-group/
    
    
    """
    df["score"]  = df["sum_weighted_indicator"] / df["sum_weights"]
    df["score"].clip(lower=None,upper=5,inplace=True)
    df["cat"] = df["score"].apply(score_to_category)
    df = df.reindex(sorted(df.columns), axis=1)
    df = pd.merge(left=df,
                   right=df_labels,
                   how="left",
                   left_on=["indicator_name","cat"],
                   right_on=["indicator","cat"])
    df.drop(columns=["indicator"],
            inplace=True)
    df["score_ranked"] = df["score"].rank(ascending=False,method="min")
    return df

def  export_df(df,geographic_scale):
    """ Export Dataframe as csv on e2
    and table on BigQuery
    
    Args:
        df(pd.DataFrame)  :  DataFrame to export.
        geographic_scale : "country" or "province"
    
    
    """
    
    if geographic_scale == "country":
        df_merged = pd.merge(left=df,
                             right=df_gadm_0,
                             how="left",
                             left_on="gid_0",
                             right_on="gid_0")
        df_merged = df_merged.reindex(sorted(df_merged.columns), axis=1)
        df_merged_csv = df_merged.set_index("gid_0")
    elif geographic_scale == "province":
        df.drop(columns=["gid_0"],inplace=True)
        df_merged = pd.merge(left=df,
                             right=df_gadm_1,
                             how="left",
                             left_on="gid_1",
                             right_on="gid_1")
        df_merged = df_merged.reindex(sorted(df_merged.columns), axis=1)
        df_merged_csv = df_merged.set_index("gid_1")
    
    
    output_file_path_ec2 = "{}/{}_{}_V{:02.0f}.csv".format(ec2_output_path,SCRIPT_NAME,geographic_scale,OUTPUT_VERSION)
    df_merged_csv.to_csv(path_or_buf=output_file_path_ec2,index=True)
    
    destination_table = "{}.{}_{}_V{:02.0f}".format(BQ_DATASET_NAME,SCRIPT_NAME,geographic_scale,OUTPUT_VERSION).lower()

    df_merged.to_gbq(destination_table=destination_table,
                     project_id=BQ_PROJECT_ID,
                     if_exists="replace")

    return df_merged


# In[15]:

sectors = ["One","Tot","Dom","Ind","Irr","Liv"]
#indicators = ["bws","bwd","iav","sev","gtd","drr","rfr","cfr","ucw","cep","udw","usa","rri"]
indicators = ["drr"]


# In[16]:

df_appended_gid_1 = pd.DataFrame()
df_appended_gid_0 = pd.DataFrame()

for sector in sectors:
    for indicator in indicators:
        print("sector:" , sector , "indicator: ", indicator)
        df_weights_gid_1 = get_weights_df(sector)
        df_indicator_gid_1 = get_weighted_indicator_df(indicator)
        df_gid_1 = pd.merge(left=df_weights_gid_1,
                                   right=df_indicator_gid_1,
                                   how="inner",
                                   left_on="gid_1",
                                   right_on="gid_1")
        df_gid_0 = province_to_country(df_gid_1,sector,indicator)
        df_gid_0 = process_df(df_gid_0)
        df_gid_1 = process_df(df_gid_1)
        
        df_appended_gid_0 = df_appended_gid_0.append(df_gid_0)
        df_appended_gid_1 = df_appended_gid_1.append(df_gid_1)


# In[17]:

df_gid_0 = export_df(df_appended_gid_0,"country")
df_gid_1 = export_df(df_appended_gid_1,"province")


# In[18]:

df_gid_0.shape


# In[19]:

df_gid_1.shape


# In[20]:

df_gid_1.head()


# In[21]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[22]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:   
# 0:00:41.101792

# In[ ]:



