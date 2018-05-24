
# coding: utf-8

# In[10]:

""" Add hydrobasins geometry and table to postGIS database. 
-------------------------------------------------------------------------------

The script requires a file called .password to be stored in the current working
directory with the password to the database.

Please note that columns with uppercase should be referred to by using double 
quotes whereas strings need single quotes. Please note that the script will 
consolidate two polygons in Russia that spans two hemispheres into one.

Author: Rutger Hofste
Date: 20171115
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.
    DATABASE_ENDPOINT (string) : RDS or postGreSQL endpoint.
    DATABASE_NAME (string) : Database name.
    TABLE_NAME_AREA_30SPFAF06 (string) : Table name used for areas. Must exist
        on same database as used in rest of script.
    S3_INPUT_PATH_RIVERDISCHARGE (string) : AWS S3 input path for 
        riverdischarge.    
    S3_INPUT_PATH_DEMAND (string) : AWS S3 input path for 
        demand.    

"""

SCRIPT_NAME = "Y2017M11D15_RH_Add_HydroBasins_postGIS_V01"
OUTPUT_VERSION= 2

#S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output_V07/"
S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Merge_HydroBasins_V02/output_V04/"
INPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_V04" 

# Database settings
DATABASE_IDENTIFIER = "aqueduct30v05"
DATABASE_NAME = "database01"
OUTPUT_TABLE_NAME = "hybas06_v{:02.0f}".format(OUTPUT_VERSION)


ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nInput ec2: " + ec2_input_path,
      "\nInput s3 : " + S3_INPUT_PATH,
      "\nOutput postGIS table : " + OUTPUT_TABLE_NAME)


# In[7]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[8]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')

get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[9]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive --quiet')


# In[11]:

import os
import boto3
import botocore
from sqlalchemy import *
import geopandas as gpd
import pandas as pd
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2 import Geometry, WKTElement


# In[14]:

def rdsConnect(database_identifier,database_name):
    rds = boto3.client('rds')
    F = open("/.password","r")
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


# In[15]:

engine, connection = rdsConnect(DATABASE_IDENTIFIER,DATABASE_NAME)


# In[16]:

gdf = gpd.read_file(os.path.join(ec2_input_path,INPUT_FILENAME+".shp"))


# In[17]:

gdf.shape


# In[10]:

gdf.columns = map(str.lower, gdf.columns)


# In[11]:

gdf = gdf.set_index("pfaf_id", drop=False)


# In[12]:

gdf.head()


# Dissolve polygon in Siberia with pfaf_id 353020

# In[13]:

gdf = gdf.dissolve(by="pfaf_id")


# In[14]:

gdf["pfaf_id"] = gdf.index


# In[15]:

gdf.shape


# In[16]:

#gdf = gdf.drop_duplicates(subset="pfaf_id",keep='first')


# In[17]:

df = pd.read_csv(os.path.join(EC2_INPUT_PATH,INPUT_FILENAME+".csv"))


# In[18]:

df.columns = map(str.lower, df.columns)


# In[19]:

df = df.drop_duplicates(subset="pfaf_id",keep='first')


# In[20]:

df.dtypes


# Select attributes that are NF 1-3 compliant

# In[21]:

df2 = df[["pfaf_id","hybas_id","next_down","next_sink","main_bas","dist_sink","dist_main","sub_area","up_area","endo","coast","order","sort"]]


# In[22]:

gdf2 = gdf.merge(df2,on="pfaf_id")


# In[23]:

gdf2 = gdf2.set_index("pfaf_id",drop=False)


# In[24]:

gdf2.head()


# In[25]:

gdf2.shape


# In[26]:

gdfFromSQL = uploadGDFtoPostGIS(gdf2,TABLE_NAME,False)


# ### Testing

# In[27]:

gdfFromSQL.head()


# In[28]:

gdfFromSQL.shape


# In[29]:

connection.close()


# In[30]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

