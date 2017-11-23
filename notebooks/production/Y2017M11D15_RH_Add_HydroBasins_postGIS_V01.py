
# coding: utf-8

# ### Add HydroBasin data to Postgis Database server
# 
# * Purpose of script: Ingest Data from HydroBasins to postgis. Data includes geometries and attribute data
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171110
# 
# The script requires a file called .password to be stored in the current working directory with the password to the database.
# 
# Please note that columns with uppercase should be referred to by using double quotes whereas strings need single quotes. Please note that the script will consolidate two polygons in Russia that spans two hemispheres into one. 

# In[1]:

get_ipython().magic('matplotlib inline')
import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

SCRIPT_NAME = "Y2017M11D15_RH_Add_HydroBasins_postGIS_V01"

INPUT_VERSION = 3
OUTPUT_VERSION= 1

EC2_INPUT_PATH = "/volumes/data/%s/input" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output" %(SCRIPT_NAME)

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"

INPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V%0.2d" %(INPUT_VERSION)

# Database settings
DATABASE_IDENTIFIER = "aqueduct30v02"
DATABASE_NAME = "database01"
TABLE_NAME = "hydrobasin6_v%0.2d" %(OUTPUT_VERSION)


# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive --quiet')


# In[5]:

import os
import boto3
import botocore
from sqlalchemy import *
import geopandas as gpd
import pandas as pd
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2 import Geometry, WKTElement


# In[6]:

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

def uploadGDFtoPostGIS(gdf,tableName,saveIndex):
    # this function uploads a polygon shapefile to table in AWS RDS. 
    # It handles combined polygon/multipolygon geometry and stores it in valid multipolygon in epsg 4326.
    
    # gdf = input geoDataframe
    # tableName = postGIS table name (string)
    # saveIndex = save index column in separate column in postgresql, otherwise discarded. (Boolean)
    
    
    gdf["type"] = gdf.geometry.geom_type    
    geomTypes = ["Polygon","MultiPolygon"]
    
    for geomType in geomTypes:
        gdfType = gdf.loc[gdf["type"]== geomType]
        geomTypeLower = str.lower(geomType)
        gdfType['geom'] = gdfType['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
        gdfType.drop(["geometry","type"],1, inplace=True)      
        print("Create table temp%s" %(geomTypeLower)) 
        gdfType.to_sql(
            name = "temp%s" %(geomTypeLower),
            con = engine,
            if_exists='replace',
            index= saveIndex, 
            dtype={'geom': Geometry(str.upper(geomType), srid= 4326)}
        )
        
    # Merge both tables and make valid
    sql = []
    sql.append("DROP TABLE IF EXISTS %s"  %(tableName))
    sql.append("ALTER TABLE temppolygon ALTER COLUMN geom type geometry(MultiPolygon, 4326) using ST_Multi(geom);")
    sql.append("CREATE TABLE %s AS (SELECT * FROM temppolygon UNION SELECT * FROM tempmultipolygon);" %(tableName))
    sql.append("UPDATE %s SET geom = st_makevalid(geom);" %(tableName))
    sql.append("DROP TABLE temppolygon,tempmultipolygon")

    for statement in sql:
        print(statement)
        result = connection.execute(statement)    
    gdfFromSQL =gpd.GeoDataFrame.from_postgis("select * from %s" %(tableName),connection,geom_col='geom' )
    return gdfFromSQL


# In[7]:

engine, connection = rdsConnect(DATABASE_IDENTIFIER,DATABASE_NAME)


# In[8]:

gdf = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILENAME+".shp"))


# In[9]:

gdf.shape


# In[10]:

gdf.columns = map(str.lower, gdf.columns)


# In[11]:

gdf = gdf.set_index("pfaf_id", drop=False)


# In[ ]:

gdf.head()


# Dissolve polygon in Siberia with pfaf_id 353020

# In[ ]:

gdf = gdf.dissolve(by="pfaf_id")


# In[ ]:

gdf["pfaf_id"] = gdf.index


# In[ ]:

gdf.shape


# In[ ]:

#gdf = gdf.drop_duplicates(subset="pfaf_id",keep='first')


# In[ ]:

df = pd.read_csv(os.path.join(EC2_INPUT_PATH,INPUT_FILENAME+".csv"))


# In[ ]:

df.columns = map(str.lower, df.columns)


# In[ ]:

df = df.drop_duplicates(subset="pfaf_id",keep='first')


# In[ ]:

df.dtypes


# Select attributes that are NF 1-3 compliant

# In[ ]:

df2 = df[["pfaf_id","hybas_id","next_down","next_sink","main_bas","dist_sink","dist_main","sub_area","up_area","endo","coast","order","sort"]]


# In[ ]:

gdf2 = gdf.merge(df2,on="pfaf_id")


# In[ ]:

gdf2 = gdf2.set_index("pfaf_id",drop=False)


# In[ ]:

gdf2.head()


# In[ ]:

gdf2.shape


# In[ ]:

gdfFromSQL = uploadGDFtoPostGIS(gdf2,TABLE_NAME,False)


# ### Testing

# In[ ]:

gdfFromSQL.head()


# In[ ]:

gdfFromSQL.shape


# In[ ]:

connection.close()


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

