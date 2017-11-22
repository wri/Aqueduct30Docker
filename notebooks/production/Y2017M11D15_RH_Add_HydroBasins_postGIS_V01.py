
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
# Please note that columns with uppercase should be referred to by using double quotes whereas strings need single quotes. Please note that the script will consolidate two polygons in Russia that spans two hemispheres into one. 

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

SCRIPT_NAME = "Y2017M11D15_RH_Add_HydroBasins_postGIS_V01"

INPUT_VERSION = 3

EC2_INPUT_PATH = "/volumes/data/%s/input" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output" %(SCRIPT_NAME)

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"

INPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V%0.2d" %(INPUT_VERSION)

# Database settings
DATABASE_IDENTIFIER = "aqueduct30v01"
DATABASE_NAME = "database01"
TABLE_NAME = str.lower(SCRIPT_NAME)


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

get_ipython().magic('matplotlib inline')


# In[5]:

rds = boto3.client('rds')


# In[6]:

F = open(".password","r")
password = F.read().splitlines()[0]
F.close()


# In[7]:

response = rds.describe_db_instances(DBInstanceIdentifier="%s"%(DATABASE_IDENTIFIER))


# In[8]:

status = response["DBInstances"][0]["DBInstanceStatus"]


# In[9]:

print(status)


# In[10]:

endpoint = response["DBInstances"][0]["Endpoint"]["Address"]


# In[11]:

print(endpoint)


# In[12]:

engine = create_engine('postgresql://rutgerhofste:%s@%s:5432/%s' %(password,endpoint,DATABASE_NAME))


# In[13]:

connection = engine.connect()


# In[14]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[15]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive --quiet')


# In[16]:

gdf = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILENAME+".shp"))


# In[17]:

gdf.columns = map(str.lower, gdf.columns)


# In[18]:

gdf = gdf.set_index("pfaf_id", drop=False)


# for PostgreSQL its better to have non-duplicate tables whereas for Pandas having duplicate column names is better. Renaming.  

# In[19]:

gdf.columns = ['pfaf_id2', 'geometry']


# In[20]:

gdf.head()


# Dissolve polygon in Siberia with pfaf_id 353020

# In[21]:

gdf = gdf.dissolve(by=gdf.index)


# In[22]:

gdf2 = gdf.copy()
gdf2["type"] = gdf2.geometry.geom_type
gdfPolygon = gdf2.loc[gdf2["type"]=="Polygon"]
gdfMultiPolygon = gdf2.loc[gdf2["type"]=="MultiPolygon"]
gdfPolygon2 = gdfPolygon.copy()
gdfMultiPolygon2 = gdfMultiPolygon.copy()


# In[23]:

gdfPolygon2['geom'] = gdfPolygon['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
gdfMultiPolygon2['geom'] = gdfMultiPolygon['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))


# In[24]:

gdfPolygon2.drop("geometry",1, inplace=True)
gdfMultiPolygon2.drop("geometry",1, inplace=True)


# In[25]:

gdfPolygon2.drop("type",1, inplace=True)
gdfMultiPolygon2.drop("type",1, inplace=True)


# In[26]:

gdfPolygon2.head()


# In[27]:

tableNamePolygon = TABLE_NAME+"polygon"
tableNameMultiPolygon = TABLE_NAME+"multipolygon"
tableNameGeometries = TABLE_NAME+"geometries"
tableNameAttributes = TABLE_NAME+"attributes"
tableNameOut = TABLE_NAME


# In[28]:

gdfPolygon2.to_sql(tableNamePolygon, engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4326)})


# In[29]:

gdfMultiPolygon2.to_sql(tableNameMultiPolygon, engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('MULTIPOLYGON', srid= 4326)})


# In[30]:

df = pd.read_csv(os.path.join(EC2_INPUT_PATH,INPUT_FILENAME+".csv"))


# In[31]:

df.columns = map(str.lower, df.columns)


# In[32]:

df = df.set_index("pfaf_id", drop=False)


# In[33]:

df.head()


# In[34]:

df.shape


# Dissolve polygon in Siberia with pfaf_id 353020

# In[35]:

df.drop_duplicates(subset="pfaf_id",keep='first', inplace=True)


# In[36]:

df.shape


# In[37]:

df.to_sql(tableNameAttributes,engine,if_exists='replace', index=False)


# ### Outer Join
# 
# We now have three tables: Polygons, Multipolygons and Attributes. We will perform some operations and perform an outer join.   
# Convert polygons to multipolygon and make valid

# In[38]:

sql = "ALTER TABLE %s ALTER COLUMN geom type geometry(MultiPolygon, 4326) using ST_Multi(geom);" %(tableNamePolygon)
result = connection.execute(sql)


# In[39]:

sql = "CREATE TABLE %s AS (SELECT * FROM %s UNION SELECT * FROM %s);" %(tableNameGeometries, tableNamePolygon,tableNameMultiPolygon)
result = connection.execute(sql)


# In[40]:

sql = "update %s set geom = st_makevalid(geom);" %(tableNameGeometries)
result = connection.execute(sql)


# In[41]:

sql = "CREATE TABLE %s AS SELECT * FROM %s l LEFT JOIN %s r ON l.pfaf_id2 = r.pfaf_id;" %(tableNameOut,tableNameGeometries,tableNameAttributes)
result = connection.execute(sql)


# In[42]:

sql = 'ALTER TABLE %s DROP COLUMN IF EXISTS pfaf_id2, DROP COLUMN IF EXISTS "pfaf_id.1", DROP COLUMN IF EXISTS hybas_id2' %(tableNameOut)
result = connection.execute(sql)


# In[43]:

sql = "DROP TABLE %s,%s,%s,%s" %(tableNamePolygon,tableNameMultiPolygon,tableNameAttributes,tableNameGeometries)
result = connection.execute(sql)


# ### Testing

# In[44]:

sql = "select * from %s" %(tableNameOut)


# In[45]:

gdfFromSQL =gpd.GeoDataFrame.from_postgis(sql,connection,geom_col='geom' ).set_index("pfaf_id", drop=False)


# In[46]:

gdfFromSQL.head()


# In[47]:

gdfFromSQL.shape


# In[48]:

connection.close()


# In[49]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

