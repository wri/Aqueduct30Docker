
# coding: utf-8

# In[1]:

""" Merge, cleanup, add category and label for drought risk.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 201809028
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

Result:
    Table on Google Bigquery.


"""

SCRIPT_NAME = "Y2018M09D28_RH_DR_Cat_Label_V01"
OUTPUT_VERSION = 4

GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2018M09D28_RH_DR_Zonal_Stats_EE_V01/output_V01/"

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

files = os.listdir(ec2_input_path)


# In[7]:

files


# In[8]:

def raw_value_to_score(x):
    """ input is already [0-1]
    mapping to [0-5]
    
    """
    if x == -9999:
        y = -9999
    else:
        y = 5 * x
    return y


def raw_value_to_score_vulnerability(x):
    """ Applying quantile approach as suggested by email from Gustavo Naumann
    
    """
    if x == -9999:
        y = -9999
    elif x<0.45:
        y = max(x/0.16-(0.29/0.16),0)
    elif (x >= 0.45) and ( x < 0.72):
        y = (1/0.27)*x - (2/3)
    elif (x >= 0.72) and ( x < 0.75):
        y = (1/0.03)*x -22 
    elif (x >= 0.75) and ( x < 0.84):
        y = (1/0.09)*x -(16/3)
    elif (x >= 0.84):
        y = min(5, (1/0.16)*x-(10/8)) 
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
        label = "Low (0.0-0.2)"
    elif cat == 1:
        label = "Low - Medium (0.2-0.4)"
    elif cat == 2:
        label = "Medium (0.4-0.6)"
    elif cat == 3:
        label = "Medium - High (0.6-0.8)"
    elif cat == 4: 
        label = "High (0.8-1.0)"
    else:
        label = "Error"
    return label


# In[9]:

d_out = {}
df_merge = pd.DataFrame(columns=['PFAF_ID']) 
for one_file in files:
    print(one_file)
    file_name, extension = one_file.split(".")
    parameter = file_name[:-9] # remove ee_export
    
    
    input_file_path = "{}/{}".format(ec2_input_path,one_file)
    df = pd.read_csv(input_file_path)
    df.drop_duplicates(subset="PFAF_ID",
                       keep="first",
                       inplace=True)
    df = df.fillna(-9999)
    df_out = df[["PFAF_ID","mean","count"]]
    df_out = df_out.rename(columns={"mean":"drought{}_dimensionless".format(parameter),
                           "count":"drought{}_count".format(parameter)})
    if one_file == "vulnerabilityee_export.csv":
        df_out_valid = df_out.loc[df_out["drought{}_dimensionless".format(parameter)]>=0]
        q = df_out_valid["drought{}_dimensionless".format(parameter)].quantile(q=[0,0.2,0.4,0.6,0.8,1])
        df_out["drought{}_score".format(parameter)] = df_out["drought{}_dimensionless".format(parameter)].apply(raw_value_to_score_vulnerability)
        print(q)
    else:
        df_out["drought{}_score".format(parameter)] = df_out["drought{}_dimensionless".format(parameter)].apply(raw_value_to_score)
    
    df_out["drought{}_cat".format(parameter)] = df_out["drought{}_score".format(parameter)].apply(score_to_category)
    df_out["drought{}_label".format(parameter)] = df_out["drought{}_cat".format(parameter)].apply(category_to_label)
    
    df_merge = df_merge.merge(right=df_out,how="outer",on="PFAF_ID")


# In[10]:

q


# In[11]:

df_merge.head()


# In[12]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME)


# In[13]:

destination_table


# In[14]:

df_merge.to_gbq(destination_table=destination_table,
                 project_id=BQ_PROJECT_ID,
                 chunksize=10000,
                 if_exists="replace")


# In[15]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:23.124523
# 

# In[ ]:



