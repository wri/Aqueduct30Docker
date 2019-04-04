
# coding: utf-8

# ### Add upstream, downstream and basin PFAF_ID to database
# 
# * Purpose of script: create a table with pfaf_id and upstream_pfaf_id, downstream_pfaf_id and basin_pfaf_id
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171123
# 
# 
# The script requires a file called .password to be stored in the current working directory with the password to the database. Basic functionality
# 

# In[1]:

get_ipython().magic('matplotlib inline')
import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

SCRIPT_NAME = "Y2017M11D23_RH_Upstream_Downstream_Basin_To_Database_V01"

EC2_INPUT_PATH = "/volumes/data/%s/input/" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output/" %(SCRIPT_NAME)

INPUT_VERSION = 1
OUTPUT_VERSION = 2

# Database settings
DATABASE_IDENTIFIER = "aqueduct30v02"
DATABASE_NAME = "database01"

INPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_V%0.2d" %(INPUT_VERSION)

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Downstream_V01/output/"


# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[5]:

import os
import numpy as np 
import pandas as pd
from ast import literal_eval
import boto3
import botocore
from sqlalchemy import *


# In[6]:

scopes = ["upstream_pfaf_ids","downstream_pfaf_ids","basin_pfaf_ids"]


# In[7]:

df = pd.read_csv(os.path.join(EC2_INPUT_PATH,INPUT_FILENAME+".csv"))


# In[ ]:

df.columns = map(str.lower, df.columns)


# In[ ]:

df = df.set_index("pfaf_id",drop=False)


# In[ ]:

df = df.drop_duplicates(subset="pfaf_id") #one basin 353020 has two HybasIDs 


# In[ ]:

def rowToDataFrame(index,row,columnName):    
    listje = literal_eval(row[columnName])
    dfRow = pd.DataFrame()
    for i, item in enumerate(listje):
        dfRow.at[i, "pfaf_id"] = np.int64(index)
        dfRow.at[i, columnName] = np.int64(item)    
    return dfRow

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
    


# In[ ]:

resultDict = {}
for scope in scopes:
    columnName = scope
    df2 = pd.DataFrame(data=df[scope],index=df.index)
    dfOut = pd.DataFrame()
    for index, row in df2.iterrows():
        dfRow = rowToDataFrame(index,row,columnName)
        dfOut = dfOut.append(dfRow)

    dfOut['pfaf_id'] = dfOut['pfaf_id'].astype(np.int64)    
    dfOut[columnName] = dfOut[columnName].astype(np.int64)    
    dfOut = dfOut.reset_index(drop=True)
    dfOut.index.names = ['id']
    
    resultDict[scope] = dfOut
    
    


# In[ ]:

resultDict["upstream_pfaf_ids"].head()


# Store in database

# In[ ]:

engine, connection = rdsConnect(DATABASE_IDENTIFIER,DATABASE_NAME)


# In[ ]:

for key, dfScope in resultDict.items():
    if key == "basin_pfaf_ids":
        tableName = "basin_pfaf6_v%0.2d" %(OUTPUT_VERSION)
    elif key == "upstream_pfaf_ids":
        tableName = "upstream_pfaf6_v%0.2d" %(OUTPUT_VERSION)
    elif key == "downstream_pfaf_ids":
        tableName = "downstream_pfaf6_v%0.2d" %(OUTPUT_VERSION)
    else:
        tableName = "error"
        print("error")
    dfScope.to_sql(
        name = tableName,
        con = connection,
        if_exists="replace",
        index= True)
    


# In[ ]:

connection.close()


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

