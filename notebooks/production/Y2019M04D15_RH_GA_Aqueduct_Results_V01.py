
# coding: utf-8

# In[1]:

"""Share country aggregation results with external party in multiple formats. 
-------------------------------------------------------------------------------

Update 2019 07 17:
Based on discussions with WRI, we add an extra column with un-membership status.
We remove all geometries that are non un member.

Merge country and province datasets into consistent database. 

Please note there is an inconsistency in GADM. Countries with one province
disappear in level 1. 

We merge three data sources:

Indicators based on PCR-GLOBWB 2: Baseline water stress, baseline water 
depletion, interannual variability and seasonal variability.

drought risk

riverine flood risk


Added on 2019 06 12:
not all countries are well represented by the hydroBasin level 6 sub-basins
and we decided to mask out based on inspection. 


Author: Rutger Hofste
Date: 20190415
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = "Y2019M04D15_RH_GA_Aqueduct_Results_V01"
OUTPUT_VERSION = 7

BQ_PROJECT_ID = "aqueduct30"
BQ_DATASET_NAME = "aqueduct30v01"

BQ_INPUT_TABLE_NAME = {}
BQ_INPUT_TABLE_NAME["country"] = {}
BQ_INPUT_TABLE_NAME["country"]["PCRGLOBWB"] = "y2019m01d28_rh_ga_zonal_stats_table_v01_country_v13"
BQ_INPUT_TABLE_NAME["country"]["drought"] = "y2019m04d11_rh_ga_drr_zonal_stats_table_v01_country_v06"
BQ_INPUT_TABLE_NAME["country"]["flood"] = "y2019m04d11_rh_ga_rfr_post_process_v01_country_v05"


BQ_INPUT_TABLE_NAME["province"] = {}
BQ_INPUT_TABLE_NAME["province"]["PCRGLOBWB"]  = "y2019m01d28_rh_ga_zonal_stats_table_v01_province_v13"
BQ_INPUT_TABLE_NAME["province"]["drought"] = "y2019m04d11_rh_ga_drr_zonal_stats_table_v01_province_v06"
BQ_INPUT_TABLE_NAME["province"]["flood"] = "y2019m04d11_rh_ga_rfr_post_process_v01_province_v05"

BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

S3_INPUT_PATH_VALIDHYBAS = "s3://wri-projects/Aqueduct30/processData/Y2019M06D12_RH_GA_Check_HydroBasin6_V01/output_V01/"
S3_INPUT_PATH_WRIALL = "s3://wri-projects/Aqueduct30/processData/Y2019M07D16_Rh_GA_Extra_Data_V01/input_V01"
S3_INPUT_PARH_POPULATION = "s3://wri-projects/Aqueduct30/processData/Y2019M07D16_Rh_GA_Extra_Data_V01/input_V01"


ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 

s3_output_path = "s3://wri-projects/Aqueduct30/finalData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nec2_input_path: " +  ec2_input_path + 
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

get_ipython().system('aws s3 cp {S3_INPUT_PATH_VALIDHYBAS} {ec2_input_path} --recursive')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_WRIALL} {ec2_input_path} --recursive')


# In[6]:

import os
import numpy as np
import pandas as pd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[7]:

def get_df_valid_hybas():
    input_filename= "gadm0_valid_hybas6_V01.csv"
    input_path = "{}/{}".format(ec2_input_path,input_filename)
    df = pd.read_csv(input_path)
    df = df[["GID_0","valid_hybas6"]]
    return df


# In[8]:

df_validhybas = get_df_valid_hybas()


# In[9]:

def get_df_wriprimary():
    input_filename = "wri_all_countries.csv"
    input_path = "{}/{}".format(ec2_input_path,input_filename)
    df = pd.read_csv(input_path)
    df = df[["iso_a3","iso_n3","primary","un_region","wb_region"]]
    return df
    


# In[10]:

df_wriprimary  = get_df_wriprimary()


# In[11]:

def get_df_population():
    input_filename = "pop_2019.csv"
    input_path = "{}/{}".format(ec2_input_path,input_filename)
    df = pd.read_csv(input_path)
    df["population_2019_million"] = df["PopTotal"] / 1000
    df = df[["LocID","population_2019_million"]]
    return df


# In[12]:

df_population = get_df_population()


# In[13]:

def get_df(geographic_scale,indicator_group,table_name):
    if geographic_scale == "country":
        sql = """
        SELECT
          gid_0,
          name_0,
          indicator_name,
          weight,
          score,
          score_ranked,
          cat,
          label,
          sum_weights,
          sum_weighted_indicator,
          count_valid,
          fraction_valid
        FROM
          `{}.{}.{}`
        """.format(BQ_PROJECT_ID,BQ_DATASET_NAME,table_name)
    elif geographic_scale == "province":
        sql = """
        SELECT
          gid_1,
          name_1,
          gid_0,
          name_0,
          indicator_name,
          weight,
          score,
          score_ranked,
          cat,
          label,
          sum_weights,
          sum_weighted_indicator,
          count_valid,
          fraction_valid
        FROM
          `{}.{}.{}`
        """.format(BQ_PROJECT_ID,BQ_DATASET_NAME,table_name)
    df = pd.read_gbq(query=sql,
                     project_id =BQ_PROJECT_ID,
                     dialect="standard")
    return df


# In[14]:

def augment_df(df):
    """
    Augment the dataframe with a few extra columns.
    
    """
    
    df_out2 = pd.merge(left=df,
                       right=df_validhybas,
                       how="left",
                       left_on="gid_0",
                       right_on="GID_0")
    df_out2.drop(columns=["GID_0"],
                 inplace=True)
    
    df_out2 = pd.merge(left=df_out2,
                       right=df_wriprimary,
                       how="left",
                       left_on="gid_0",
                       right_on="iso_a3")
    df_out2 = pd.merge(left=df_out2,
                       right=df_population,
                       how="left",
                       left_on="iso_n3",
                       right_on="LocID")
    
    
    return df_out2

    
    
    


# In[15]:

def mask_invalid(df):
    """
    mask if fraction valid < 0.5 OR NOT hydrology_valid 
    For Riverine Flood Risk, we don't have fraction_valid. 
    
    """
    cond = (((df["fraction_valid"]>0.5) | (df["indicator_name"] == "rfr" ))& (df["valid_hybas6"] ==1))
    df['score'] = np.where(cond, df["score"], -9999)
    df['cat'] = np.where(cond, df["cat"], -9999)
    df['label'] = np.where(cond, df["label"], "NoData")
    
    return df
    


# In[16]:

def clean_vertical(df,geographic_scale):
    """
    Clean up vertical dataframe
    
    """
    if geographic_scale == "country":
        columns = ["iso_a3",
                   "iso_n3",
                   "primary",
                   "name_0",
                   "indicator_name",
                   "weight",
                   "score",
                   "score_ranked",
                   "cat",
                   "label",
                   "sum_weights",
                   "sum_weighted_indicator",
                   "count_valid",
                   "fraction_valid",
                   "valid_hybas6",                   
                   "un_region",
                   "wb_region",
                   "population_2019_million"
                   ]
    elif geographic_scale == "province":
        columns = ["gid_1",
                   "name_1",
                   "iso_a3",
                   "iso_n3",
                   "primary",
                   "name_0",
                   "indicator_name",
                   "weight",
                   "score",
                   "score_ranked",
                   "cat",
                   "label",
                   "sum_weights",
                   "sum_weighted_indicator",
                   "count_valid",
                   "fraction_valid",
                   "valid_hybas6",                   
                   "un_region",
                   "wb_region",
                   "population_2019_million"
                   ]
    df_out = df[columns]
    return df_out
    
    


# In[17]:

dict_out = {}
for geographic_scale, dictje  in BQ_INPUT_TABLE_NAME.items():
    df_out = pd.DataFrame()
    for indicator_group, table_name in dictje.items():
        print(geographic_scale,indicator_group,table_name)
        
        df = get_df(geographic_scale,indicator_group,table_name)
        df_out = df_out.append(df)
    
    
    df_out2 = augment_df(df_out)
    df_out3 = mask_invalid(df_out2)


    #df_out3["score_ranked_all"] = df_out3.groupby(by=["indicator_name","weight"])["score"].rank(ascending=False,method="min")
    # Only primary (UN member) countries: 
    
    # Removed for version 7
    #df_out4 = df_out3.loc[df_out3["primary"] == 1]
    df_out4  = df_out3.copy()
    df_out4["score_ranked"] =  df_out4.groupby(by=["indicator_name","weight"])["score"].rank(ascending=False,method="min")

    # added on 2019 07 24
    df_out4 = df_out4.loc[df_out4["indicator_name"].isin(["bws","drr","rfr"])]
    
    dict_out[geographic_scale] = clean_vertical(df_out4,geographic_scale)
    
    output_file_path_ec2 = "{}/{}_{}_V{:02.0f}.csv".format(ec2_output_path,SCRIPT_NAME,geographic_scale,OUTPUT_VERSION)
    dict_out[geographic_scale].to_csv(path_or_buf=output_file_path_ec2,index=True)
    destination_table = "{}.{}_{}_V{:02.0f}".format(BQ_DATASET_NAME,SCRIPT_NAME,geographic_scale,OUTPUT_VERSION).lower()

    dict_out[geographic_scale].to_gbq(destination_table=destination_table,
                  project_id=BQ_PROJECT_ID,
                  if_exists="replace")


       


# In[18]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[19]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:01:55.346004
# 
