
# coding: utf-8

# In[1]:

""" Compare weighted and unweighted annual results for one basin.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20180718
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M07D18_RH_QA_Annual_Weighted_Unweighted_OneBasin_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

PROJECT_ID = "aqueduct30"
INPUT_TABLE_NAME = "Y2018M07D17_RH_RDS_To_S3_V01"
INPUT_DATASET_NAME = "aqueduct30v01"


SUBBASIN_OF_INTEREST =  431700 #172111 233036 261492 431700

COLUMNS_OF_INTEREST = ["pfafid_30spfaf06",
                       "temporal_resolution",
                       "year",
                       "month",
                       "waterstress_label_dimensionless_30spfaf06",
                       "waterstress_category_dimensionless_30spfaf06",
                       "waterstress_score_dimensionless_30spfaf06",
                       "waterstress_raw_dimensionless_30spfaf06",
                       "avg1y_ols_ols10_weighted_waterstress_dimensionless_30spfaf06",
                       "avg1y_ols_ols10_waterstress_dimensionless_30spfaf06",
                       "ols_ols10_waterstress_dimensionless_30spfaf06",
                       "ols_ols10_ptotww_m_30spfaf06"]


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

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"


# In[4]:

sql = "SELECT"
for column_of_interest in COLUMNS_OF_INTEREST:
    sql += " {},".format(column_of_interest)
sql = sql[:-1]
sql += " FROM {}.{}".format(INPUT_DATASET_NAME,INPUT_TABLE_NAME)
sql += " WHERE year = 2014"
sql += " AND pfafid_30spfaf06 = {}".format(SUBBASIN_OF_INTEREST)
sql += " ORDER BY month"


# In[5]:

print(sql)


# In[6]:

df = pd.read_gbq(query=sql,
                 project_id=PROJECT_ID,
                 dialect="standard")


# In[7]:

df.shape


# In[8]:

df.head()


# Weighted water stress calculated by multiplying the monthly water stress score with the total withdrawal. 

# In[9]:

df["ws_times_ptotww"] = df["waterstress_raw_dimensionless_30spfaf06"] * df["ols_ols10_ptotww_m_30spfaf06"]


# In[10]:

total_value = df["ws_times_ptotww"].sum()


# In[11]:

total_weight = df["ols_ols10_ptotww_m_30spfaf06"].sum()


# In[12]:

weighted_value_pandas = total_value/total_weight


# In[13]:

weighted_value_postgis = df.loc[df["temporal_resolution"] == "year"]["avg1y_ols_ols10_weighted_waterstress_dimensionless_30spfaf06"]


# In[14]:

print(weighted_value_pandas, weighted_value_postgis)


# In[15]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:



