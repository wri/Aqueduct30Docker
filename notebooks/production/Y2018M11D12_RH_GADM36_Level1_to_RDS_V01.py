
# coding: utf-8

# In[1]:

""" Upload GADM 3.6 level 1 to RDS.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181112
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

TESTING = 0
SCRIPT_NAME = "Y2018M11D12_RH_GADM36_Level1_to_RDS_V01"
OUTPUT_VERSION = 2

# Database settings
RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"
OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

INPUT_URL = "https://biogeo.ucdavis.edu/data/gadm3.6/gadm36_levels_gpkg.zip"
LEVEL = 1 #Province or equivalent level

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nInput ec2: " + ec2_input_path,
      "\nInput URL : " + INPUT_URL,
      "\nOutput postGIS table : " + OUTPUT_TABLE_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

# Version 3.6 Date accessed 2018 09 11
# Compressed Size = 1.2 GB 
# Uncompressed Size =  3.5 GB

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

get_ipython().system('wget {INPUT_URL} -P {ec2_input_path}')


# In[5]:

import os
import sqlalchemy
import geopandas as gpd
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2 import Geometry, WKTElement


# In[6]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))
connection = engine.connect()


# In[7]:

files = os.listdir("{}".format(ec2_input_path))


# In[8]:

files


# In[9]:

file_name = files[0]


# In[10]:

get_ipython().system("unzip '{ec2_input_path}/{file_name}' -d {ec2_output_path}")


# In[11]:

layer = "level{:01.0f}".format(LEVEL)


# In[12]:

input_file_path = "{}/{}".format(ec2_output_path,"gadm36_levels.gpkg")


# In[13]:

gdf = gpd.read_file(input_file_path,layer=layer)


# In[14]:

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

if TESTING:
    gdf = gdf.sample(1000)


# In[16]:

gdf.head()


# In[17]:

gdf.shape


# In[18]:

gdf.columns = map(str.lower, gdf.columns)


# In[19]:

gdf = gdf.set_index("gid_{}".format(LEVEL), drop=False)


# In[20]:

gdfFromSQL = uploadGDFtoPostGIS(gdf,OUTPUT_TABLE_NAME,False)


# In[21]:

engine.dispose()


# In[22]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:
# 0:16:54.891228
