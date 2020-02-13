
# coding: utf-8

# In[1]:

""" Convert RDS table to csv files on S3 and GCS
-------------------------------------------------------------------------------

update 2020/02/10 update output 11-12 input 8-9

Author: Rutger Hofste
Date: 20180712
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04
"""

# imports
import re
import os
import numpy as np
import pandas as pd
from retrying import retry
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)
import multiprocessing

SCRIPT_NAME = 'Y2018M07D17_RH_RDS_To_S3_V02'
OUTPUT_VERSION = 12

TESTING = 0

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME = "y2018m07d30_rh_coalesce_columns_v01_v09"

ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
gcs_output_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input Table: " , INPUT_TABLE_NAME,
      "\nOutput ec2: ", ec2_output_path,
      "\nOutput s3: " , s3_output_path,
      "\nOutput gcs: ", gcs_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))


# In[5]:

cpu_count = multiprocessing.cpu_count()
print("Power to the maxxx:", cpu_count)


# In[6]:

sql = "SELECT DISTINCT pfafid_30spfaf06 FROM {} ORDER BY pfafid_30spfaf06".format(INPUT_TABLE_NAME)


# In[7]:

df = pd.read_sql(sql,engine)


# In[8]:

df.shape


# In[9]:

df.head()


# In[10]:

if TESTING:
    df = df[0:10]


# In[11]:

df_split = np.array_split(df, cpu_count*100)


# In[12]:

def basin_to_csv(df):
    for index, row in df.iterrows():
        pfafid = row["pfafid_30spfaf06"]
        sql = "SELECT * FROM {} WHERE pfafid_30spfaf06 = {}".format(INPUT_TABLE_NAME,pfafid)
        df_basin = pd.read_sql(sql,engine)
        now = datetime.datetime.now()
        df_basin["processed_timestamp"] = pd.Timestamp(now)  
        output_file_name = "{}_V{:02.0f}.csv".format(pfafid,OUTPUT_VERSION)
        output_file_path = "{}/{}".format(ec2_output_path,output_file_name)
        # Added on Y2018M07D30, convert to numeric type if possible
        df_basin2 = df_basin.apply(pd.to_numeric,errors='ignore')
        df_basin2.to_csv(output_file_path)
        print(output_file_path)


# In[13]:

# cleared output to save space
p= multiprocessing.Pool()
results_buffered = p.map(basin_to_csv,df_split)
p.close()
p.join()


# In[14]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive --quiet')


# In[15]:

# cleared output to save space
get_ipython().system('gsutil -m cp {ec2_output_path}/*.csv {gcs_output_path}')


# In[16]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:42:01.416597  
# 0:42:14.565295  
# 0:31:40.728932  
# 0:23:35.132388
# 
# 

# In[ ]:



