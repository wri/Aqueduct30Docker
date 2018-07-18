
# coding: utf-8

# In[1]:

""" Compare weighted and unweighted annual results.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20180718
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M06D22_RH_QA_Annual_Weighted_Unweighted_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

PROJECT_ID = "aqueduct30"
INPUT_TABLE_NAME = "Y2018M07D17_RH_RDS_To_S3_V01"
INPUT_DATASET_NAME = "aqueduct30v01"


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

sql = "SELECT * FROM {}.{}".format(INPUT_DATASET_NAME,INPUT_TABLE_NAME)
sql += "WHERE temporal_resolution = 'year'"
sql += " AND year = 2014"


# In[5]:

print(sql)


# In[ ]:

df = pd.read_gbq(query=sql,
                 project_id=PROJECT_ID,
                 dialect="standard")


# In[ ]:

df.shape


# In[ ]:



