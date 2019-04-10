
# coding: utf-8

# In[1]:

""" Add category and Label for groundwater stress and trend.
-------------------------------------------------------------------------------

Add category and label to groundwater stress data. Only aquifer data for now.

Groundwater Stress Categories:

<1 Low
1-5 Low - Medium
5-10 Medium - High
10-20 High
> 20 Extremely High

Linear interpolation groundwater stress

if x<1
    y = max(x,0)
elif 1 < x < 5
    y = (1/4)x + 3/4
elif 5 < x < 10
    y = 1/5 x + 1
elif 10 < x < 20 
    y = 1/10x + 2
elif x > 20
    y = min(x,5)


Groundwater Table Declining Trends Categories:
unit = cm/year

- 9999 NoData
- 9998 Insignificant trend
< 0 No Depletion
0 - 2 Low Depletion
2 - 8 Moderate Depletion
>8 High Depletion

however we need a 5 score category so. Names of categories TBD.

-1 -0 Low Depletion Risk -> No Depletion
0 - 2 Low-Medium Depletion Risk -> Moderate Depletion
2 - 4 Medium-High Depletion Risk - > Moderate Depletion
4 - 8 High Depletion Risk -> Moderate Depletion
>8 Extremely High Depletion Risk -> Extremely High Depletion

if x<0
    y = max(0,x+1)
elif 0 < x < 2
    y = (1/2)x + 1
elif 2 < x < 4
    y = (1/2) x + 1
elif 4 < x < 8 
    y = (1/4)x + 2
elif x > 8
    y = min((1/4)x + 2,5)


Author: Rutger Hofste
Date: 20180903
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

SCRIPT_NAME = "Y2018M09D03_RH_GWS_Cat_Label_V01"
OUTPUT_VERSION = 2

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"
BQ_INPUT_TABLE_NAME = "y2018m09d03_rh_gws_tables_to_bq_v01_v01_aquifer_table_sorted"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

print("BQ_OUTPUT_DATASET_NAME: ", BQ_OUTPUT_DATASET_NAME,
      "\nBQ_INPUT_TABLE_NAME: ",BQ_INPUT_TABLE_NAME,
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

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[ ]:




# In[4]:

sql = "SELECT * FROM {}.{}".format(BQ_OUTPUT_DATASET_NAME,BQ_INPUT_TABLE_NAME)


# In[5]:

sql


# In[6]:

df = pd.read_gbq(query=sql,dialect="standard")


# In[7]:

df.head()


# In[8]:

def raw_value_to_score_groundwater_stress(x):
    """ Linear interpolation
    
    thresholds set by Deltares
    
    
    """
    
    
    if x == -9999:
        y = -9999
    elif x<1:
        y = max(x,0)
    elif (x >= 1) and ( x < 5):
        y = (1/4)*x + 3/4
    elif (x >= 5) and (x < 10):
        y = (1/5)*x + 1
    elif (x >= 10) and (x < 20): 
        y = (1/10)*x + 2
    elif x >= 20:
        y = 4
    return y


def raw_value_to_score_gtdt(x):
    """
    
    thresholds set by Deltares

    """
    if x == -9999:
        y = -9999
    elif x<0:
        y = max(x+1,0)
    elif (x >= 0) and ( x < 2):
        y = x + 1
    elif (x >= 2) and (x < 4):
        y = (1/2)*x + 1
    elif (x >= 4) and (x < 8): 
        y = (1/2)*x + 1
    elif x >= 8:
        y = min(5,(1/2)*x + 1)
    return y

    


def score_to_category(score):
    if score != 5:
        cat = int(np.floor(score))
    else:
        cat = 4
    return cat


def category_to_label_groundwater_stress(cat):
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


def category_to_label_gtdt(cat):
    if cat == -9999:
        label = "NoData"
    elif cat == -9998:
        label = "Insignificant Trend"
    elif cat == 0:
        label = "Low (<0 cm/y)"
    elif cat == 1:
        label = "Low - Medium (0-2 cm/y)"
    elif cat == 2:
        label = "Medium - High (2-4 cm/y)"
    elif cat == 3:
        label = "High (4-8 cm/y)"
    elif cat == 4: 
        label = "Extremely High (>8 cm/y)"
    else:
        label = "Error"
    return label







# In[9]:

df["groundwaterstress_score"] = df["groundwaterstress_dimensionless"].apply(raw_value_to_score_groundwater_stress)


# In[10]:

df["groundwaterstress_cat"] = df["groundwaterstress_score"].apply(score_to_category)


# In[11]:

df["groundwaterstress_label"] = df["groundwaterstress_cat"].apply(category_to_label_groundwater_stress)


# In[12]:

df["groundwatertabledecliningtrend_score"] = df["groundwatertabledecliningtrend_cmperyear"].apply(raw_value_to_score_gtdt)


# In[13]:

#df["groundwatertabledecliningtrend_score"] = np.where(df["r_squared"]>=0.9,df["groundwatertabledecliningtrend_score"],-9998)
# changed to 0.8 on 2019 04 05
df["groundwatertabledecliningtrend_score"] = np.where(df["r_squared"]>=0.8,df["groundwatertabledecliningtrend_score"],-9998)


# In[14]:

df["groundwatertabledecliningtrend_cat"] = df["groundwatertabledecliningtrend_score"].apply(score_to_category)


# In[15]:

df["groundwatertabledecliningtrend_label"] = df["groundwatertabledecliningtrend_cat"].apply(category_to_label_gtdt)


# In[16]:

df.head()


# In[17]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME)


# In[18]:

df.to_gbq(destination_table=destination_table,
    project_id=BQ_PROJECT_ID,
    chunksize=10000,
    if_exists="replace")


# In[19]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:12.746715

# In[ ]:



