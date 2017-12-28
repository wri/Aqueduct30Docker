
# coding: utf-8

# # Y2917M12D28_RH_EE_Results_To_PostGIS_V01
# 
# * Purpose of script: Store earth engine raster results in postgis 
# * Kernel used: python27
# * Date created: 20171228

# This script requires you to set a password for your database. The script will search for the file .password in the current working directory. You can use your terminal window to create the password.

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

DATABASE_IDENTIFIER = "aqueduct30v03"
DATABASE_NAME = "database01"
TABLE_NAME = "hybasvalid01"


# In[ ]:




# In[3]:

import os
import boto3
import botocore
from sqlalchemy import *
import pandas as pd
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry.multipolygon import MultiPolygon


# In[4]:

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


# In[5]:

engine, connection = rdsConnect(DATABASE_IDENTIFIER,DATABASE_NAME)


# In[ ]:



