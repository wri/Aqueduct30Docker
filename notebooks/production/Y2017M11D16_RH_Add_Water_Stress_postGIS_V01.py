
# coding: utf-8

# ### Add Water Stress Data to PostGIS RDS 
# 
# * Purpose of script: add supply, demand and water stress data from PCRGlobWB to a postGIS Relational Database
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171116
# 
# This script requires you to set a password for your database. The script will search for the file .password in the current working directory. You can use your terminal window to create the password.
# 
# 1. dfLeft = Geometries and basin info from PostGreSQL  
# 1. dfRight = Water Stress data from S3  
# 

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

SCRIPT_NAME = "Y2017M11D15_RH_Add_HydroBasins_postGIS_V01"

# Database settings
DATABASE_IDENTIFIER = "aqueduct30v01"
DATABASE_NAME = "database01"
TABLE_NAME = "hybasvalid01"

# Water Stress Data
INPUT_VERSION = 6
S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M10D04_RH_Threshold_WaterStress_V02/output/" 
INPUT_FILE_NAME = "Y2017M10D04_RH_Threshold_WaterStress_V%0.2d" %(INPUT_VERSION)

EC2_INPUT_PATH = "/volumes/data/%s/input" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output" %(SCRIPT_NAME)


# In[23]:

tableNameWaterStress = TABLE_NAME + "waterstress"


# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH} ')
get_ipython().system('rm -r {EC2_OUTPUT_PATH} ')

get_ipython().system('mkdir -p {EC2_INPUT_PATH} ')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH} ')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[5]:

import os
import boto3
import botocore
from sqlalchemy import *
import pandas as pd


# In[6]:

rds = boto3.client('rds')


# In[7]:

F = open(".password","r")
password = F.read().splitlines()[0]
F.close()


# In[8]:

response = rds.describe_db_instances(DBInstanceIdentifier="%s"%(DATABASE_IDENTIFIER))
endpoint = response["DBInstances"][0]["Endpoint"]["Address"]


# In[9]:

print(endpoint)


# In[10]:

engine = create_engine('postgresql://rutgerhofste:%s@%s:5432/%s' %(password,endpoint,DATABASE_NAME))


# In[11]:

connection = engine.connect()


# In[12]:

sql = """SELECT column_name 
FROM information_schema.columns
WHERE table_schema = 'public' 
AND table_name   = 'hybasvalid01'"""
resultAll = connection.execute(sql).fetchall()


# In[13]:

columnNamesGeometries = [r[0] for r in resultAll]


# In[14]:

culumnNamesAlreadyExist = columnNamesGeometries + ["hybas_id2","unnamed: 0"]


# In[15]:

culumnNamesAlreadyExist.remove('geom')


# In[16]:

dfRight = pd.read_pickle(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME+".pkl"))


# In[17]:

dfRight.shape


# In[18]:

dfRight.columns = map(str.lower, dfRight.columns)


# In[19]:

dfRight = dfRight.set_index("pfaf_id",drop=False)


# In[20]:

df2 = dfRight.copy()


# In[ ]:

print(columnNamesGeometries)


# In[21]:

dfRight.drop(culumnNamesAlreadyExist,1,inplace=True)


# In[22]:

dfRight.head()


# Drop Duplicate Columns from tabel

# In[28]:

dfRight.dtypes


# In[ ]:

#columnsToDrop = ["HYBAS_ID2","HYBAS_ID","Unnamed: 0","NEXT_DOWN","NEXT_SINK","MAIN_BAS","DIST_SINK","DIST_MAIN","SUB_AREA","UP_AREA","ENDO","COAST","ORDER","SORT"]


# In[25]:

dfRight["pfaf_id2"] = dfRight.index


# In[29]:

dfRight.shape


# In[36]:

dfRight01 = dfRight[0:1]


# In[37]:

dfRight01.shape


# In[40]:

dfRight01.to_sql(tableNameWaterStress,engine,if_exists='replace', index=False,chunksize=100)


# In[ ]:




# In[ ]:



