
# coding: utf-8

# In[1]:

""" Store gdbd and hybas deltas in postgis in lookup table.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20180725
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

TESTING = 0
SCRIPT_NAME = "Y2018M07D25_RH_Delta_Lookup_Table_PostGIS_V01"
OVERWRITE_INPUT = 1
OUTPUT_VERSION = 1

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M07D25_RH_Basin_Manual_Step_V01/hybas_deltas/"
INPUT_FILE_NAME = "hybas_deltas.csv"

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

# All Lowercase
OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nInput ec2: " + ec2_input_path,
      "\nOutput Table Name: "+ OUTPUT_TABLE_NAME)


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


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH}{INPUT_FILE_NAME} {ec2_input_path}')


# In[5]:

import os
import sqlalchemy
import pandas as pd


# In[6]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))


# In[7]:

file_path = "{}/{}".format(ec2_input_path,INPUT_FILE_NAME)


# In[8]:

df = pd.read_csv(file_path)


# In[9]:

def process_df(df):
    # returns deltas only in simplified form
    df_basin = df.loc[df["delta_id"]>0]
    df_simple = df_basin[["PFAF_ID","delta_id"]]
    df_simple = df_simple.rename(columns={"PFAF_ID":"pfaf_id"})
    df_simple = df_simple.sort_values(by=["delta_id"])
    return df_simple


# In[10]:

df.head()


# In[11]:

df_simple = process_df(df)


# In[12]:

df_simple.shape


# In[13]:

df_simple.to_sql(name=OUTPUT_TABLE_NAME,
                 con=engine,
                 if_exists = "replace" )


# In[14]:

engine.dispose()


# In[15]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

