
# coding: utf-8

# # Store FAO related files on PostgreSQL RDS database
# 
# * Purpose of script: This script will process the hydrobasin related data into multiple tables according to the database ERD
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171122 
# 
# The script requires a file called .password to be stored in the current working directory with the password to the database.

# In[1]:

get_ipython().magic('matplotlib inline')
import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[49]:

SCRIPT_NAME = "Y2017M11D22_RH_FAO_To_Database_V01"

INPUT_VERSION = 1
OUTPUT_VERSION = 2

INPUT_FILE_NAME = "hydrobasins_fao_fiona_merged_v%0.2d" %(INPUT_VERSION)

EC2_INPUT_PATH = "/volumes/data/%s/input/" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output/" %(SCRIPT_NAME)

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Merge_FAONames_V01/output/"

# Database settings
DATABASE_IDENTIFIER = "aqueduct30v01"
DATABASE_NAME = "database01"
TABLE_NAME = str.lower(SCRIPT_NAME)

TABLE_NAME_FAO_MAJOR = "fao_major_v%0.2d" %(OUTPUT_VERSION)
TABLE_NAME_FAO_MINOR = "fao_minor_v%0.2d" %(OUTPUT_VERSION)


# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[5]:

import os
import pandas as pd
import geopandas as gpd
from ast import literal_eval
import boto3
import botocore
from sqlalchemy import *
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry.multipolygon import MultiPolygon


# In[6]:

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

def uploadGDFtoPostGIS(gdf,tableName):
    # this function uploads a shapefile to table in AWS RDS. 
    # It handles combined polygon/multipolygon geometry and stores it in multipolygon
    gdf2 = gdf.copy()
    gdf2["type"] = gdf2.geometry.geom_type
    gdfPolygon = gdf2.loc[gdf2["type"]=="Polygon"]
    gdfMultiPolygon = gdf2.loc[gdf2["type"]=="MultiPolygon"]
    gdfPolygon2 = gdfPolygon.copy()
    gdfMultiPolygon2 = gdfMultiPolygon.copy()
    gdfPolygon2['geom'] = gdfPolygon['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
    gdfMultiPolygon2['geom'] = gdfMultiPolygon['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
    gdfPolygon2.drop("geometry",1, inplace=True)
    gdfMultiPolygon2.drop("geometry",1, inplace=True)
    gdfPolygon2.drop("type",1, inplace=True)
    gdfMultiPolygon2.drop("type",1, inplace=True)
    
    gdfPolygon2.to_sql("temppolygon", engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4326)})
    
    gdfMultiPolygon2.to_sql("tempmultipolygon", engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('MULTIPOLYGON', srid= 4326)})
    
    sql = "ALTER TABLE temppolygon ALTER COLUMN geom type geometry(MultiPolygon, 4326) using ST_Multi(geom);" 
    result = connection.execute(sql)
    sql = "CREATE TABLE %s AS (SELECT * FROM temppolygon UNION SELECT * FROM tempmultipolygon);" %(tableName)
    result = connection.execute(sql)
    sql = "DROP TABLE temppolygon,tempmultipolygon"
    result = connection.execute(sql)
    sql = "select * from %s" %(tableName)
    gdfFromSQL =gpd.GeoDataFrame.from_postgis(sql,connection,geom_col='geom' )
    return gdfFromSQL


# In[7]:

engine, connection = rdsConnect(DATABASE_IDENTIFIER,DATABASE_NAME)


# In[ ]:




# In[8]:

gdf = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME+".shp"))


# In[9]:

gdf.head()


# The idea is to store the data in two tables: major basin and minor basin together with the geometry. There is no unique identifier for the minor basins so we will use a composite key    
#     

# In[13]:

def compositeKey(MAJ_BAS,SUB_BAS):
    key = 'MAJ_BAS_%0.4d_SUB_BAS_%0.7d' %(MAJ_BAS,SUB_BAS)
    return key


# In[14]:

gdf["FAOid"]= gdf.apply(lambda x: compositeKey(x["MAJ_BAS"],x["SUB_BAS"]),1)


# In[15]:

gdf.head()


# In[16]:

gdf = gdf.set_index("FAOid",drop=False)


# In[35]:

dfMajorFull = gdf[["MAJ_BAS","MAJ_NAME","MAJ_AREA","LEGEND"]]
gdfMinor = gdf[["SUB_BAS","TO_BAS","MAJ_BAS","SUB_NAME","SUB_AREA","geometry","FAOid"]]


# In[18]:

dfMajorFull.head()


# In[19]:

dfMajor = dfMajorFull.groupby("MAJ_BAS").first()


# In[20]:

dfMajor.head()


# In[21]:

dfMajor.to_sql(
    name = TABLE_NAME_FAO_MAJOR,
    con = connection,
    if_exists="replace",
    index= True)


# ## Minor river basin

# In[36]:

gdfMinor.head()


# Geometry consists of polygon and multipolygon type. Upload both to postGIS and set polygon to multipolygon and join. 

# In[48]:

gdfMinor.head()


# In[44]:

gdfFromSQL = uploadGDFtoPostGIS(gdfMinor,TABLE_NAME_FAO_MINOR)


# In[ ]:

gdfFromSQL.head()

