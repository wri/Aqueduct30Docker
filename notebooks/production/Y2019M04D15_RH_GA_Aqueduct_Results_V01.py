
# coding: utf-8

# In[1]:

"""Share country aggregation results with external party in multiple formats. 
-------------------------------------------------------------------------------

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
OUTPUT_VERSION = 4


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

import os
import numpy as np
import pandas as pd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[6]:

input_filename_validhybas = "gadm0_valid_hybas6_V01.csv"


# In[7]:

input_path_validhybase = "{}/{}".format(ec2_input_path,input_filename_validhybas)


# In[8]:

df_validhybas = pd.read_csv(input_path_validhybase)


# In[9]:

df_validhybas = df_validhybas[["GID_0","valid_hybas6"]]


# In[10]:

for geographic_scale, dictje  in BQ_INPUT_TABLE_NAME.items():
    df_out = pd.DataFrame()
    for indicator_group, table_name in dictje.items():
        print(geographic_scale,indicator_group,table_name)
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
        
        df_out = df_out.append(df)
    
        
    
    df_out2 = pd.merge(left=df_out,
                       right=df_validhybas,
                       how="left",
                       left_on="gid_0",
                       right_on="GID_0")
    df_out2.drop(columns=["GID_0"],
                 inplace=True)
    
    
    
    
    output_file_path_ec2 = "{}/{}_{}_V{:02.0f}.csv".format(ec2_output_path,SCRIPT_NAME,geographic_scale,OUTPUT_VERSION)
    df_out2.to_csv(path_or_buf=output_file_path_ec2,index=True)
    destination_table = "{}.{}_{}_V{:02.0f}".format(BQ_DATASET_NAME,SCRIPT_NAME,geographic_scale,OUTPUT_VERSION).lower()

    df_out2.to_gbq(destination_table=destination_table,
                  project_id=BQ_PROJECT_ID,
                  if_exists="replace")
    


# In[11]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[12]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:01:55.346004
# 
