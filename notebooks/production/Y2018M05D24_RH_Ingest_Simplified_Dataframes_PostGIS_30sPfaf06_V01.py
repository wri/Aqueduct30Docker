
# coding: utf-8

# In[1]:

""" Store merged and simplified pandas dataframes in postGIS database. 
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20180524
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.
    DATABASE_ENDPOINT (string) : RDS or postGreSQL endpoint.
    DATABASE_NAME (string) : Database name.
    TABLE_NAME_AREA_30SPFAF06 (string) : Table name used for areas. Must exist
        on same database as used in rest of script.
    S3_INPUT_PATH_RIVERDISCHARGE (string) : AWS S3 input path for 
        riverdischarge.    
    S3_INPUT_PATH_DEMAND (string) : AWS S3 input path for 
        demand.     

"""

TESTING = 1
SCRIPT_NAME = "Y2018M05D24_RH_Ingest_Simplified_Dataframes_PostGIS_30sPfaf06_V01"
OVERWRITE_INPUT = 1
OVERWRITE_OUTPUT = 1
OUTPUT_VERSION = 6

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M05D23_RH_Simplify_DataFrames_Pandas_30sPfaf06_V03/output_V08"

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

# All Lowercase
OUTPUT_TABLE_NAME = "global_historical_all_multiple_m_30spfaf06_v{:02.0f}".format(OUTPUT_VERSION)

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)


print("\nInput ec2: " + ec2_input_path,
      "\nInput s3 : " + S3_INPUT_PATH,
      "\nOutput postGIS table : " + OUTPUT_TABLE_NAME)



# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

if OVERWRITE_INPUT:
    get_ipython().system('rm -r {ec2_input_path}')
    get_ipython().system('mkdir -p {ec2_input_path}')
    get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive --exclude="*" --include="*.pkl"')

if OVERWRITE_OUTPUT:
    get_ipython().system('rm -r {ec2_output_path}')
    get_ipython().system('mkdir -p {ec2_output_path}')
    


# In[4]:

# imports
import re
import os
import numpy as np
import pandas as pd
import aqueduct3
from datetime import timedelta
from sqlalchemy import *
pd.set_option('display.max_columns', 500)


# In[5]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
connection = engine.connect()

if OVERWRITE_OUTPUT:
    sql = text("DROP TABLE IF EXISTS {};".format(OUTPUT_TABLE_NAME))
    result = engine.execute(sql)

file_names = os.listdir(ec2_input_path)

if TESTING:
    file_names = file_names[0:3]


# In[6]:

i = 0 
start_time = time.time()
for file_name in file_names:
    i = i + 1 
    elapsed_time = time.time() - start_time 
    print("Processed dataframe {} / {} Elapsed: {}".format(i,len(file_names),timedelta(seconds=elapsed_time)))
    
    file_path = "{}/{}".format(ec2_input_path,file_name)
    df = pd.read_pickle(file_path)
    df["input_file_name"] = file_name
    df.to_sql(name=OUTPUT_TABLE_NAME,
              con=connection,
              if_exists = "append" )
    


# In[7]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 3:25:08.538574
