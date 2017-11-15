
# coding: utf-8

# ### Make geometry valid
# 
# * Purpose of script: Ingest Data and perform geometric operations on the SQL database to make geometry valid
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171110

# This script requires some additional steps that are not automated yet. The objective is to set up a PosGIS enabled PostgreSQL AWS RDS instance. 
# 
# https://gis.stackexchange.com/questions/239198/geopandas-dataframe-to-postgis-table-help
# 
# http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ConnectToPostgreSQLInstance.html
# 
# http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.html#Appendix.PostgreSQL.CommonDBATasks.PostGIS
# 
# database is not protected by default. Basic workflow:
# 
# 1. Create database
# 1. Load data into geopandas
# 1. split by geometry type
# 1. upload to postGIS database
# 1. Make valid
# 1. combine results in geopandas

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

SCRIPT_NAME = "Y2017M11D10_RH_Make_Geometry_Valid_V01"

INPUT_VERSION = 1
OUTPUT_VERSION = 5

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/%s/output/" %(SCRIPT_NAME)

INPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V%0.2d" %(INPUT_VERSION)

EC2_INPUT_PATH = "/volumes/data/%s/input" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output" %(SCRIPT_NAME)

OUTPUT_FILE_NAME = "Y2017M11D10_RH_Make_Geometry_Valid_V%0.2d" %(OUTPUT_VERSION)

S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/%s/output/" %(SCRIPT_NAME)

# Database settings
DATABASE_IDENTIFIER = "aqueduct30v01"
DATABASE_NAME = "database01"
TABLE_NAME = "hybasvalid01"


# In[3]:

from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import pandas as pd
import geopandas as gpd
import os
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
import boto3
import botocore
import time


# In[4]:

get_ipython().magic('matplotlib inline')


# In[5]:

F = open(".password","r")
password = F.read().splitlines()[0]
F.close()


# In[6]:

rds = boto3.client('rds')


# In[7]:

response = rds.describe_db_instances(DBInstanceIdentifier="%s"%(DATABASE_IDENTIFIER)) 


# In[8]:

status = response["DBInstances"][0]["DBInstanceStatus"]


# In[9]:

endpoint = response["DBInstances"][0]["Endpoint"]["Address"]


# In[10]:

print(endpoint)


# Ingest Data in PostGIS

# In[11]:

engine = create_engine('postgresql://rutgerhofste:nopassword@%s:5432/%s' %(endpoint,DATABASE_NAME))


# In[12]:

connection = engine.connect()


# In[ ]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[ ]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive --quiet')


# In[13]:

gdf = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILENAME+".shp"))


# In[14]:

gdf = gdf.set_index("PFAF_ID", drop=False)


# In[15]:

gdf.head()


# In[16]:

gdf.shape


# In[17]:

gdf2 = gdf.copy()


# In[18]:

gdf2["type"] = gdf2.geometry.geom_type


# In[19]:

gdfPolygon = gdf2.loc[gdf2["type"]=="Polygon"]
gdfMultiPolygon = gdf2.loc[gdf2["type"]=="MultiPolygon"]


# In[20]:

gdfPolygon2 = gdfPolygon.copy()
gdfMultiPolygon2 = gdfMultiPolygon.copy()


# In[21]:

def explode(indf):
    outdf = gpd.GeoDataFrame(columns=indf.columns)
    for idx, row in indf.iterrows():
        if type(row.geometry) == Polygon:
            outdf = outdf.append(row,ignore_index=True)
        if type(row.geometry) == MultiPolygon:
            multdf = gpd.GeoDataFrame(columns=indf.columns)
            recs = len(row.geometry)
            multdf = multdf.append([row]*recs,ignore_index=True)
            for geom in range(recs):
                multdf.loc[geom,'geometry'] = row.geometry[geom]
            outdf = outdf.append(multdf,ignore_index=True)
    return outdf


# In[22]:

gdfMultiPolygonExploded = explode(gdfMultiPolygon2)


# In[23]:

gdfMultiPolygonExploded['geometry'] = gdfMultiPolygonExploded['geometry'].apply(lambda x: Polygon(x))


# In[24]:

gdfPolygon2['geom'] = gdfPolygon['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))


# In[25]:

gdfMultiPolygon2['geom'] = gdfMultiPolygon['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))


# In[26]:

gdfMultiPolygonExploded['geom'] = gdfMultiPolygonExploded['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))


# In[27]:

gdfPolygon2.drop("geometry",1, inplace=True)
gdfMultiPolygon2.drop("geometry",1, inplace=True)
gdfMultiPolygonExploded.drop("geometry",1, inplace=True)


# The following command will connect to a temporary free tier AWS RDS instance

# In[28]:

tableNamePolygon = TABLE_NAME+"polygon"
tableNameMultiPolygon = TABLE_NAME+"multipolygon"
tableNameMultiPolygonExploded = TABLE_NAME+"multipolygonexploded"


# In[29]:

gdfPolygon2.to_sql(tableNamePolygon, engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4326)})


# In[30]:

gdfMultiPolygon2.to_sql(tableNameMultiPolygon, engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('MULTIPOLYGON', srid= 4326)})


# In[31]:

gdfMultiPolygonExploded.to_sql(tableNameMultiPolygonExploded, engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4326)})


# In[32]:

sql = "create table %s as select * from %s" %(tableNamePolygon+"_pristine",tableNamePolygon)
result = connection.execute(sql)


# In[33]:

sql = "create table %s as select * from %s" %(tableNameMultiPolygon+"_pristine",tableNameMultiPolygon)
result = connection.execute(sql)


# In[34]:

sql = "create table %s as select * from %s" %(tableNameMultiPolygonExploded+"_pristine",tableNameMultiPolygonExploded)
result = connection.execute(sql)


# In[ ]:

sql = "update %s set geom = st_makevalid(geom)" %(tableNamePolygon)
result = connection.execute(sql)


# In[ ]:

sql = "update %s set geom = st_makevalid(geom)" %(tableNameMultiPolygon)
result = connection.execute(sql)


# In[ ]:




# In[36]:

sql = "ALTER TABLE %s ALTER COLUMN geom TYPE geometry(multipolygon,4326) USING ST_Multi(geom)" %(tableNameMultiPolygonExploded)
result = connection.execute(sql)


# In[37]:

sql = "update %s set geom = st_makevalid(geom)" %(tableNameMultiPolygonExploded)
result = connection.execute(sql)


# In[ ]:

sql = "update %s set geom = st_removerepeatedpoints(geom)" %(tableNamePolygon)
result = connection.execute(sql)


# In[ ]:

sql = "update %s set geom = st_removerepeatedpoints(geom)" %(tableNameMultiPolygon)
result = connection.execute(sql)


# Check if operation succesful 

# In[ ]:

sql = "select * from %s" %(tableNamePolygon)


# In[ ]:

gdfAWSPolygon=gpd.GeoDataFrame.from_postgis(sql,connection,geom_col='geom' ).set_index("PFAF_ID", drop=False)


# In[ ]:

sql = "select * from %s" %(tableNameMultiPolygon)


# In[ ]:

gdfAWSMultiPolygon=gpd.GeoDataFrame.from_postgis(sql,connection,geom_col='geom' ).set_index("PFAF_ID", drop=False)


# In[ ]:

test = gdfAWSMultiPolygon.copy()


# In[ ]:

gdfAWSPolygon.crs = {'init' :'epsg:4326'}
gdfAWSMultiPolygon.crs = {'init' :'epsg:4326'}


# In[ ]:

gdfAWS = gdfAWSPolygon.append(gdfAWSMultiPolygon)


# In[ ]:

gdfAWS.to_file(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME+".shp"))


# In[ ]:

gdfAWSPolygon.to_file(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME+"_polygonOnly.shp"))


# In[ ]:

gdfAWSMultiPolygon.to_file(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME+"_multiPolygonOnly.shp"))


# In[ ]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[ ]:

connection.close()


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

