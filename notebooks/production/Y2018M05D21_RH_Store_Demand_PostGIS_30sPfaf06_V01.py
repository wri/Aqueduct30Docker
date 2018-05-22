
# coding: utf-8

# In[1]:

""" Store demand data in postGIS database.
-------------------------------------------------------------------------------

Store the demand results in the postGIS table. Use a 
consistent schema to allow other indicators to be stored as well.


Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.

"""

TESTING = 0
OVERWRITE = 1 
SCRIPT_NAME = "Y2018M05D21_RH_Store_Demand_PostGIS_30sPfaf06_V01"
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
TABLE_NAME = "demand01"

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M04D22_RH_Zonal_Stats_Demand_EE_V01/output_V01"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nInput ec2: " + ec2_input_path,
      "\nInput s3: " + S3_INPUT_PATH)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()


# In[4]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_input_path}')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive --exclude="*" --include="*.pkl"')


# In[6]:

import os
import numpy as np
import pandas as pd
import aqueduct3
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)


# In[7]:

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
connection = engine.connect()

if OVERWRITE:
    sql = text("DROP TABLE IF EXISTS {};".format(TABLE_NAME))
    result = engine.execute(sql)
    


# In[8]:

months = range(1,12+1)
years = range(1960,2014+1)
temporal_resolutions = ["year","month"]

input_file_names = os.listdir(ec2_input_path)
print(len(input_file_names))


# In[9]:

def pre_process_df(df):
    df["zones"] = df["zones"].astype(np.int64)
    # Other columns are not converted because they contain nan's. 
    # in a next version I might fill the nans with a no data value
    # and convert to integer. 
    return df


# In[10]:

if TESTING:
    input_file_names = input_file_names[0:10]


# In[11]:

i = 0 
start_time = time.time()
for input_file_name in input_file_names:
    i = i + 1
    elapsed_time = time.time() - start_time 
    print("Index: {:03.0f} Elapsed: {}".format(i, timedelta(seconds=elapsed_time)))
    input_file_path = "{}/{}".format(ec2_input_path,input_file_name)
    df = pd.read_pickle(input_file_path)
    df_cleaned = pre_process_df(df)
    df_cleaned.to_sql(TABLE_NAME,engine,if_exists='append', index=False,chunksize=100)


# In[12]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 13:45:20.759114
