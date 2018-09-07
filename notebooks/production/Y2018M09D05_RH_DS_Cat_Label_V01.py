
# coding: utf-8

# In[1]:

""" Add category and Label for drought severity.
-------------------------------------------------------------------------------

Drought Severity Soil Moisture 


0- 0.1 Low
0.1 - 0.25 Low - Medium
0.25 - 0.5 Medium - High
0.5 - 0.75 High
0.75 - 0 Extremely High


Using Linear Interpolation

if x < 0.1
    y = max(0,10x)
elif 0.1<x<0.25
    y = (20/3)*x + (1/3)
else
    y = min(5,4*x+1)

Author: Rutger Hofste
Date: 20180905
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name
    INPUT_VERSION (integer) : input version, see readme and output number
                              of previous script.
    OUTPUT_VERSION (integer) : output version for ec2 and s3.
    
    
Returns:

Result:
    Table on Google Bigquery.

"""

SCRIPT_NAME = "Y2018M09D05_RH_DS_Cat_Label_V01"
OUTPUT_VERSION = 1

GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2018M09D05_RH_DS_Zonal_Stats_V01/output_V04/"


BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

ec2_input_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 


print("GCS_INPUT_PATH: ",GCS_INPUT_PATH,
      "\nec2_input_path: ",ec2_input_path,
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
get_ipython().system('mkdir -p {ec2_input_path}')


# In[4]:

get_ipython().system('gsutil -m cp {GCS_INPUT_PATH}* {ec2_input_path}')


# In[5]:

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[6]:

def raw_value_to_score_droughtseverity(x):
    """ Linear interpolation
    
    thresholds set by Yoshi
    
    Using Linear Interpolation

    if x < 0.1
        y = max(0,10x)
    elif 0.1<x<0.25
        y = (20/3)*x + (1/3)
    else
        y = min(5,4*x+1)
    
    """
    
    
    if x == -9999:
        y = -9999
    elif x<0.1:
        y = max(10*x,0)
    elif (x >= 0.1) and ( x < 0.25):
        y = (20/3)*x + 1/3
    elif (x >= 0.25):
        y = min(4*x + 1,5)
    return y





def score_to_category(score):
    if score != 5:
        cat = int(np.floor(score))
    else:
        cat = 4
    return cat


def category_to_label(cat):
    if cat == -9999:
        label = "NoData"
    elif cat == 0:
        label = "Low"
    elif cat == 1:
        label = "Low - Medium"
    elif cat == 2:
        label = "Medium - High"
    elif cat == 3:
        label = "High"
    elif cat == 4: 
        label = "Extremely High"
    else:
        label = "Error"
    return label

def process_droughtseveritysoilmoisture(df):
    df["droughtseveritysoilmoisture_score"] = df["droughtseveritysoilmoisture_dimensionless"].apply(raw_value_to_score_droughtseverity)
    df["droughtseveritysoilmoisture_cat"] = df["droughtseveritysoilmoisture_score"].apply(score_to_category)
    df["droughtseveritysoilmoisture_label"] = df["droughtseveritysoilmoisture_cat"].apply(category_to_label)
    return df
    
def process_droughtseveritystreamflow(df):
    df["droughtseveritystreamflow_score"] = df["droughtseveritystreamflow_dimensionless"].apply(raw_value_to_score_droughtseverity)
    df["droughtseveritystreamflow_cat"] = df["droughtseveritystreamflow_score"].apply(score_to_category)
    df["droughtseveritystreamflow_label"] = df["droughtseveritystreamflow_cat"].apply(category_to_label)
    return df


# In[7]:

files = os.listdir(ec2_input_path)


# In[8]:

files


# In[9]:

def  group_basins(pfaf_id):
    """ Returns pfaf_id unless part of the complex -180 degree meridian crossing
    polygons.    
    
    """
    group_subbasins = [353011,353012,353013]    
    if pfaf_id in group_subbasins:
        pfaf_id_grouping = 353011
    else:
        pfaf_id_grouping = pfaf_id
    return pfaf_id_grouping

def process_duplicate_pfafid(df,value_column_name,weight_column_name,group_by_attribute):
    """ Handles statistics for the basins crossing the -180 meridian. 
    
    handles the first case: duplicate pfaf_ids.
    
    pfaf_id's of features:
    
    
    1 -----------------
    Western Hemisphere:
        PFAF_ID = 353020,
        SUB_AREA = 5236.9 
    
    Eastern Hemisphere:    
        PFAF_ID = 353020 
        SUB_AREA = 2498.7
    
    solution: weighted aggregation and remove duplicate pfaf_id's
    
    
    2---------------

    Eastern Hemisphere
        
        PFAF_ID 353012
        SUB_AREA = 111764.6
    
    Western Hemisphere
        PFAF_ID = 353011     
        SUB_AREA = 28931.1

        PFAF_ID = 353013 
        SUB_AREA = 7363.9
      
    
    solution: weighted aggregation and store result in each row. 
    
    Args:
        df (DataFrame) : Dataframe with weigths and values.
        value_column_name (string): Name of column with values.
        weight_column_name (string): Name of column with weights.
        group_by_attribute (string): Group by attribute. 
    Returns
        df_out (DataFrame) : dataframe with weighted values.
    
    
    """
    
    df_temp = df.copy()
    df_temp["weighted_values"] = df[weight_column_name] * df[value_column_name]
    df_temp_sums = df_temp.groupby(by=group_by_attribute,as_index=False).sum()
    df_temp_sums[value_column_name] = df_temp_sums["weighted_values"] / df_temp_sums[weight_column_name]
    df_temp_sums = df_temp_sums[[group_by_attribute,value_column_name]]
    return df_temp_sums


# In[10]:

d_out = {}
for one_file in files:
    print(one_file)
    input_file_path = "{}/{}".format(ec2_input_path,one_file)
    df = pd.read_csv(input_file_path)
    df = df.fillna(-9999)
    df["PFAF_ID_GROUPING"] = df["PFAF_ID"].apply(group_basins)
    
    if one_file =="droughtseveritysoilmoistureee_export.csv":       
        df_weighted = process_duplicate_pfafid(df,"droughtseveritysoilmoisture_dimensionless","SUB_AREA","PFAF_ID_GROUPING")
        df = df.drop("droughtseveritysoilmoisture_dimensionless",axis=1)
        df_merge = df.merge(right=df_weighted,how="left",on="PFAF_ID_GROUPING")
        df_merge = df_merge.drop("PFAF_ID_GROUPING",axis=1)
        df_merge = df_merge.drop("SUB_AREA",axis=1)
        df_merge = df_merge.groupby(by="PFAF_ID",as_index=False).first()
        d_out[one_file] = process_droughtseveritysoilmoisture(df_merge)
        
    elif one_file =="droughtseveritystreamflowee_export.csv":
        df_weighted = process_duplicate_pfafid(df,"droughtseveritystreamflow_dimensionless","SUB_AREA","PFAF_ID_GROUPING")
        df = df.drop("droughtseveritystreamflow_dimensionless",axis=1)
        df_merge = df.merge(right=df_weighted,how="left",on="PFAF_ID_GROUPING")
        df_merge = df_merge.drop("PFAF_ID_GROUPING",axis=1)
        df_merge = df_merge.drop("SUB_AREA",axis=1)
        df_merge = df_merge.groupby(by="PFAF_ID",as_index=False).first()
        d_out[one_file] = process_droughtseveritystreamflow(df_merge)
        pass


# In[11]:

df_merged = d_out["droughtseveritysoilmoistureee_export.csv"].merge(right=d_out["droughtseveritystreamflowee_export.csv"],
                                                                    how="left",
                                                                    on="PFAF_ID")


# In[12]:

df_merged.head()


# In[13]:

df_merged.shape


# In[14]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME)


# In[15]:

df_merged.to_gbq(destination_table=destination_table,
                 project_id=BQ_PROJECT_ID,
                 chunksize=10000,
                 if_exists="replace")


# In[16]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:17.856315  
# 0:00:26.999023
