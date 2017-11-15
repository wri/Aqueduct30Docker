
# coding: utf-8

# ### Add HydroBasin data to Postgis Database server
# 
# * Purpose of script: Ingest Data from HydroBasins last step into a postGIS database
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171110
# 
# The script requires a file called .password to be stored in the current working directory with the password to the database.
# 
# Please note that columns with uppercase should be referred to by using double quotes whereas strings need single quotes. 

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

SCRIPT_NAME = "Y2017M11D15_RH_Add_HydroBasins_postGIS_V01"

INPUT_VERSION = 1

EC2_INPUT_PATH = "/volumes/data/%s/input" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output" %(SCRIPT_NAME)

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"

INPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V%0.2d" %(INPUT_VERSION)

# Database settings
DATABASE_IDENTIFIER = "aqueduct30v01"
DATABASE_NAME = "database01"
TABLE_NAME = "hybasvalid02"


# In[3]:

import os
import boto3
import botocore
from sqlalchemy import *
import geopandas as gpd
import pandas as pd
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2 import Geometry, WKTElement


# In[4]:

rds = boto3.client('rds')


# In[5]:

F = open(".password","r")
password = F.read().splitlines()[0]
F.close()


# In[6]:

response = rds.describe_db_instances(DBInstanceIdentifier="%s"%(DATABASE_IDENTIFIER))


# In[7]:

status = response["DBInstances"][0]["DBInstanceStatus"]


# In[8]:

print(status)


# In[9]:

endpoint = response["DBInstances"][0]["Endpoint"]["Address"]


# In[10]:

print(endpoint)


# In[11]:

engine = create_engine('postgresql://rutgerhofste:%s@%s:5432/%s' %(password,endpoint,DATABASE_NAME))


# In[12]:

connection = engine.connect()


# In[13]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[14]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive --quiet')


# In[15]:

gdf = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILENAME+".shp"))


# In[16]:

gdf = gdf.set_index("PFAF_ID", drop=False)


# In[17]:

gdf.columns = map(str.lower, gdf.columns)


# In[18]:

gdf.head()


# In[19]:

gdf2 = gdf.copy()
gdf2["type"] = gdf2.geometry.geom_type
gdfPolygon = gdf2.loc[gdf2["type"]=="Polygon"]
gdfMultiPolygon = gdf2.loc[gdf2["type"]=="MultiPolygon"]
gdfPolygon2 = gdfPolygon.copy()
gdfMultiPolygon2 = gdfMultiPolygon.copy()


# In[20]:

gdfPolygon2['geom'] = gdfPolygon['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
gdfMultiPolygon2['geom'] = gdfMultiPolygon['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))


# In[21]:

gdfPolygon2.drop("geometry",1, inplace=True)
gdfMultiPolygon2.drop("geometry",1, inplace=True)


# In[22]:

gdfPolygon2.drop("type",1, inplace=True)
gdfMultiPolygon2.drop("type",1, inplace=True)


# In[23]:

tableNamePolygon = TABLE_NAME+"polygon"
tableNameMultiPolygon = TABLE_NAME+"multipolygon"
tableNameAttributes = TABLE_NAME+"attributes"


# In[24]:

gdfPolygon2.to_sql(tableNamePolygon, engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4326)})


# In[25]:

sql = "create table %s as select * from %s" %(tableNamePolygon+"_pristine",tableNamePolygon)
result = connection.execute(sql)


# In[26]:

gdfMultiPolygon2.to_sql(tableNameMultiPolygon, engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('MULTIPOLYGON', srid= 4326)})


# In[27]:

sql = "create table %s as select * from %s" %(tableNameMultiPolygon+"_pristine",tableNameMultiPolygon)
result = connection.execute(sql)


# In[28]:

df = pd.read_csv(os.path.join(EC2_INPUT_PATH,INPUT_FILENAME+".csv"))


# In[29]:

df = df.set_index("PFAF_ID", drop=False)


# In[30]:

df.columns = map(str.lower, df.columns)


# In[31]:

df.to_sql(tableNameAttributes,engine,if_exists='replace', index=False)


# In[32]:

sql = "create table %s as select * from %s" %(tableNameAttributes+"_pristine",tableNameAttributes)
result = connection.execute(sql)


# We now have three tables: Polygons, Multipolygons and Attributes. We will perform some operations and perform a join. 

# In[33]:

sql = "ALTER TABLE %s ALTER COLUMN geom type geometry(MultiPolygon, 4326) using ST_Multi(geom);" %(tableNamePolygon)
result = connection.execute(sql)


# In[ ]:

sql = "CREATE TABLE test01 AS SELECT * FROM hybasvalid01polygon INNER JOIN hybasvalid01attributes ON (hybasvalid01polygon.PFAF_ID = hybasvalid01attributes.PFAF_ID);" 
result = connection.execute(sql)


# In[ ]:

SELECT geom
    FROM polygonselection, hybasvalid02attributes
    WHERE hybasvalid02attributes.pfaf_id = polygonselection.pfaf_id;


# In[ ]:

connection.close()

