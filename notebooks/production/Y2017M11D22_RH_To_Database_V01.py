
# coding: utf-8

# # Store Hydrobasin related files on PostgreSQL RDS database
# 
# * Purpose of script: This script will process the hydrobasin related data into multiple tables according to the database ERD
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171122 
# 
# The script requires a file called .password to be stored in the current working directory with the password to the database.

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[14]:

SCRIPT_NAME = "Y2017M11D22_RH_To_Database_V01"

INPUT_VERSION = 1
INPUT_FILE_NAME = "hydrobasins_fao_fiona_merged_v%0.2d" %(INPUT_VERSION)

EC2_INPUT_PATH = "/volumes/data/%s/input/" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output/" %(SCRIPT_NAME)

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Merge_FAONames_V01/output/"

# Database settings
DATABASE_IDENTIFIER = "aqueduct30v01"
DATABASE_NAME = "database01"
TABLE_NAME = str.lower(SCRIPT_NAME)


# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[12]:

import os
import pandas as pd
import geopandas as gpd
from ast import literal_eval
import boto3
import botocore
from sqlalchemy import *


# In[15]:

# RDS Connection
def rdsConnect(database_identifier,database_name):
    rds = boto3.client('rds')
    F = open(".password","r")
    password = F.read().splitlines()[0]
    F.close()
    response = rds.describe_db_instances(DBInstanceIdentifier="%s"%(database_identifier))
    status = response["DBInstances"][0]["DBInstanceStatus"]
    print("Status:",status)
    endpoint = response["DBInstances"][0]["Endpoint"]["Address"]
    print("Endpoint:",endpoint)
    engine = create_engine('postgresql://rutgerhofste:%s@%s:5432/%s' %(password,endpoint,database_name))
    connection = engine.connect()
    return engine, connection


# In[16]:

engine, connection = rdsConnect(DATABASE_IDENTIFIER,DATABASE_NAME)


# In[ ]:




# In[10]:

gdf = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME+".shp"))


# In[11]:

gdf.head()


# The idea is to store the data in two tables: major basin and minor basin together with the geometry. There is no unique identifier for the minor basins so we will use a composite key    
#     

# In[22]:

def compositeKey(MAJ_BAS,SUB_BAS):
    key = 'MAJ_BAS_%0.4d_SUB_BAS_%0.7d' %(MAJ_BAS,SUB_BAS)
    return key


# In[23]:




# In[20]:

gdf.head()


# In[26]:

gdf["test"]= gdf.apply(lambda x: compositeKey(42,43),1)


# In[27]:

gdf.head()


# In[ ]:



