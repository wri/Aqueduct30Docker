
# coding: utf-8

# In[1]:

""" Store ICEP data in PostGIS Database.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181001
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
OVERWRITE_OUTPUT = 1

SCRIPT_NAME = "Y2018M10D01_RH_ICEP_Basins_PostGIS_V01"
OUTPUT_VERSION = 2

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/finalData/ICEP"

OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

# Database settings
DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)


print("\nInput ec2: " + ec2_input_path,
      "\nInput s3 : " + S3_INPUT_PATH,
      "\nOutput postGIS table : " + OUTPUT_TABLE_NAME)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive')
    


# In[4]:

import numpy as np
import pandas as pd
import geopandas as gpd
from sqlalchemy import *
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry.multipolygon import MultiPolygon
pd.set_option('display.max_columns', 500)


# In[5]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()


# In[6]:

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
connection = engine.connect()

if OVERWRITE_OUTPUT:
    sql = text("DROP TABLE IF EXISTS {};".format(OUTPUT_TABLE_NAME))
    result = engine.execute(sql)


# In[7]:

input_file_path = "{}/icep_results.shp".format(ec2_input_path)


# In[8]:

gdf = gpd.read_file(input_file_path)


# In[9]:

def score_to_category(score):
    if score != 5:
        cat = int(np.floor(score))
    else:
        cat = 4
    return cat


# In[10]:

gdf = gdf.rename(columns={"BASINID":"icepbasinid",
                          "ICEP_raw":"icep_dimensionless",
                          "ICEP_s":"icep_score",
                          "ICEP_cat":"icep_label"})


# In[11]:

gdf["icep_cat"] = gdf["icep_score"].apply(score_to_category)


# In[12]:

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


# In[13]:

gdf.head()


# In[14]:

gdf.columns = map(str.lower, gdf.columns)


# In[15]:

gdfFromSQL = uploadGDFtoPostGIS(gdf,OUTPUT_TABLE_NAME,False)


# In[16]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:00:08.221081  
# 0:00:09.097920
# 
# 

# In[ ]:



