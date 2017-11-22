
# coding: utf-8

# In[1]:

# Database settings
DATABASE_IDENTIFIER = "aqueduct30v01"
DATABASE_NAME = "database01"


# In[2]:

import os
import pandas as pd
import geopandas as gpd
from ast import literal_eval
import boto3
import botocore
from sqlalchemy import *
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry.multipolygon import MultiPolygon


# In[3]:

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


# In[5]:

engine, connection = rdsConnect(DATABASE_IDENTIFIER,DATABASE_NAME)


# In[6]:

sql = "SELECT * FROM fao_minor_v04"


# In[7]:

gdfMinor =gpd.GeoDataFrame.from_postgis(sql,connection,geom_col='geom' )


# In[8]:

gdfMinor.head()


# In[ ]:

gdfMinor

